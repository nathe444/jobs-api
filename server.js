const express = require('express');
const axios = require('axios');
const dotenv = require('dotenv');
const cron = require('node-cron');
const { createClient } = require('@supabase/supabase-js');

// Load environment variables
dotenv.config();

const app = express();
const PORT = process.env.PORT || 3000;

// Supabase client
const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY
);

// Middleware
app.use(express.json());

// Cybersecurity keywords
const CYBER_KEYWORDS = [
  'cybersecurity', 'information security', 'infosec', 'soc', 'siem', 'grc', 'iam',
  'appsec', 'cloud security', 'pentest', 'security engineer', 'security analyst'
];

const EXCLUDE_KEYWORDS = [
  'physical security', 'security guard', 'surveillance'
];

// Helper function to check if job is cybersecurity-related
function isCybersecurityJob(job) {
  const text = `${job.title || ''} ${job.description_text || ''}`.toLowerCase();
  
  // Check exclude keywords first
  const hasExclude = EXCLUDE_KEYWORDS.some(keyword => text.includes(keyword));
  if (hasExclude) return false;
  
  // Check include keywords
  const hasInclude = CYBER_KEYWORDS.some(keyword => text.includes(keyword));
  return hasInclude;
}

// Helper function to validate apply URL
function isValidApplyUrl(url) {
  if (!url) return false;
  try {
    const parsed = new URL(url);
    return ['http:', 'https:'].includes(parsed.protocol);
  } catch {
    return false;
  }
}

// Helper function to normalize salary data
function normalizeSalary(job) {
  const raw = job.salary_raw ?? job.salary ?? job.compensation ?? null;
  const rawIsObject = raw && typeof raw === 'object' && !Array.isArray(raw);
  const monetaryValue = rawIsObject ? raw.value : null;
  const valueIsObject = monetaryValue && typeof monetaryValue === 'object' && !Array.isArray(monetaryValue);

  const min = job.salary_min ?? job.salary_min_derived ?? job.salary_range_min ?? job.salary_from ?? (valueIsObject ? monetaryValue.minValue : null);
  const max = job.salary_max ?? job.salary_max_derived ?? job.salary_range_max ?? job.salary_to ?? (valueIsObject ? monetaryValue.maxValue : null);
  const value = valueIsObject ? monetaryValue.value : null;
  const currency = job.salary_currency ?? job.currency ?? job.compensation_currency ?? (rawIsObject ? raw.currency : null);
  const period = job.salary_period ?? job.salary_unit ?? job.compensation_period ?? (valueIsObject ? monetaryValue.unitText : null);

  const toNumber = (value) => {
    if (value === null || value === undefined) return null;
    if (typeof value === 'number' && Number.isFinite(value)) return value;
    const cleaned = String(value).replace(/[^0-9.]/g, '');
    const parsed = Number(cleaned);
    return Number.isFinite(parsed) ? parsed : null;
  };

  const minNum = toNumber(min ?? value);
  const maxNum = toNumber(max);

  if (minNum || maxNum || currency || period) {
    return JSON.stringify({
      min: minNum || undefined,
      max: maxNum || undefined,
      currency: currency || undefined,
      period: period || undefined,
      raw: raw || undefined
    });
  }

  if (raw === null || raw === undefined) return null;
  if (typeof raw === 'string') return raw.substring(0, 255);

  try {
    return JSON.stringify(raw).substring(0, 255);
  } catch {
    return String(raw).substring(0, 255);
  }
}

// Fetch jobs from RapidAPI
function fetchJobs() {
  return axios.get('https://active-jobs-db.p.rapidapi.com/active-ats-7d', {
    headers: {
      'X-RapidAPI-Key': process.env.RAPIDAPI_KEY,
      'X-RapidAPI-Host': 'active-jobs-db.p.rapidapi.com'
    },
    params: {
      limit: 100,
      offset: 0,
      title_filter: 'cybersecurity',
      description_type: 'text'
    }
  });
}

// Job sync function (used by cron and manual endpoint)
async function syncJobs() {
  console.log('Starting job sync...');
  if (!process.env.RAPIDAPI_KEY) {
    throw new Error('RAPIDAPI_KEY is missing in .env');
  }
  
  // Fetch jobs from RapidAPI
  console.log('Fetching jobs from RapidAPI...');
  let response;
  try {
    response = await fetchJobs();
  } catch (err) {
    const status = err.response?.status;
    const body = err.response?.data;
    console.error('RapidAPI error:', status, body);
    const rapidError = new Error('RapidAPI error');
    rapidError.status = status;
    rapidError.body = body;
    throw rapidError;
  }
  
  const jobs = response.data;
  console.log(`Fetched ${jobs.length} jobs from API`);

  if (jobs.length > 0) {
    const sampleJob = jobs[0];
    console.log('Sample job keys:', Object.keys(sampleJob || {}));
    console.log('Sample job preview:', sampleJob);
    try {
      console.log('Sample job structure:', JSON.stringify(sampleJob, null, 2));
    } catch (error) {
      console.warn('Sample job structure: JSON stringify failed', error.message);
    }
  }
  
  // Filter jobs
  const filteredJobs = jobs.filter(job => {
    const hasTitle = job.title;
    const hasOrg = job.organization;
    const hasUrl = job.url;
    const isCyber = isCybersecurityJob(job);
    const isValidUrl = isValidApplyUrl(job.url);
    return hasTitle && hasOrg && hasUrl && isCyber && isValidUrl;
  });
  
  console.log(`Filtered to ${filteredJobs.length} cybersecurity jobs`);
  
  // Normalize and prepare for database
  const normalizedJobs = filteredJobs.map(job => ({
    title: job.title.substring(0, 255),
    company: job.organization.substring(0, 100),
    location: job.locations_alt_raw?.[0] || 'Remote',
    is_remote: job.location_type === 'TELECOMMUTE' || job.remote_derived === true,
    apply_url: job.url,
    source: job.source || 'active-jobs-db',
    external_job_id: job.id?.toString() || Math.random().toString(),
    posted_at: new Date(job.date_posted || job.date_created || new Date()).toISOString(),
    last_updated: new Date(job.date_created || new Date()).toISOString(),
    salary: normalizeSalary(job),
    job_type: job.employment_type?.[0] || 'FULL_TIME',
    description_snippet: job.description_text?.substring(0, 1000) || null,
    created_at: new Date().toISOString(),
    updated_at: new Date().toISOString()
  }));
  
  // Upsert into database (prevents duplicates based on external_job_id)
  const { data, error } = await supabase
    .from('jobs')
    .upsert(normalizedJobs, { onConflict: 'external_job_id' })
    .select();
  
  if (error) {
    throw new Error(`Database error: ${error.message}`);
  }
  
  console.log(`Upserted ${data.length} jobs into database`);
  
  return {
    fetched: jobs.length,
    filtered: filteredJobs.length,
    upserted: data.length
  };
}

// Cron job: runs every day at midnight
cron.schedule('0 0 * * *', async () => {
  console.log('Running scheduled job sync...');
  try {
    const result = await syncJobs();
    console.log('Scheduled sync completed:', result);
  } catch (error) {
    console.error('Scheduled sync failed:', error.message);
  }
});

// Route to manually trigger sync
app.get('/sync', async (req, res) => {
  try {
    const result = await syncJobs();
    res.json({ message: 'Sync completed', ...result });
  } catch (error) {
    console.error('Sync error:', error.message, error.status || '', error.body || '');
    if (error.status) {
      return res.status(502).json({ error: 'Sync failed', status: error.status, details: error.body });
    }
    res.status(500).json({ error: 'Sync failed', details: error.message });
  }
});

// Route to get jobs
app.get('/jobs', async (req, res) => {
  try {
    const { data, error } = await supabase
      .from('jobs')
      .select('*')
      .order('posted_at', { ascending: false });
    
    if (error) {
      console.error('Database error:', error);
      return res.status(500).json({ error: 'Database error', details: error.message });
    }
    
    res.json(data);
  } catch (error) {
    console.error('Error:', error.message);
    res.status(500).json({ error: 'Failed to fetch jobs', details: error.message });
  }
});

// Route to get job by ID
app.get('/jobs/:id', async (req, res) => {
  try {
    const { data, error } = await supabase
      .from('jobs')
      .select('*')
      .eq('id', req.params.id)
      .single();
    
    if (error) {
      return res.status(404).json({ error: 'Job not found' });
    }
    
    res.json(data);
  } catch (error) {
    console.error('Error:', error.message);
    res.status(500).json({ error: 'Failed to fetch job', details: error.message });
  }
});

// Health check
app.get('/health', (req, res) => {
  res.json({ status: 'ok', timestamp: new Date().toISOString() });
});

// Start server
app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
  console.log('Cron job scheduled: Daily at midnight');
  console.log('Endpoints:');
  console.log('  GET /health - Health check');
  console.log('  GET /sync - Manually trigger job sync');
  console.log('  GET /jobs - Get all jobs');
  console.log('  GET /jobs/:id - Get job by ID');
});