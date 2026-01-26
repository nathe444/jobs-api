const express = require('express');
const axios = require('axios');
const dotenv = require('dotenv');
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

// Route to fetch and store jobs
app.get('/sync', async (req, res) => {
  try {
    console.log('Starting job sync...');
    
    // Fetch jobs from RapidAPI
    console.log('Fetching jobs from RapidAPI...');
    console.log('API Key:', process.env.RAPIDAPI_KEY ? 'Set' : 'Not set');
    
    const response = await axios.get('https://active-jobs-db.p.rapidapi.com/active-ats-7d', {
      headers: {
        'X-RapidAPI-Key': process.env.RAPIDAPI_KEY,
        'X-RapidAPI-Host': 'active-jobs-db.p.rapidapi.com'
      },
      params: {
        limit: 50,
        offset: 0,
        title_filter: 'cybersecurity',
        description_type: 'text'
      }
    });
    
    console.log('API Response Status:', response.status);
    
    const jobs = response.data;
    console.log(`Fetched ${jobs.length} jobs from API`);
    
    // Log first few job titles to see what we're getting
    console.log('First 5 job titles:');
    jobs.slice(0, 5).forEach((job, index) => {
      console.log(`${index + 1}. ${job.title}`);
    });
    
    // Filter jobs
    const filteredJobs = jobs.filter(job => {
      const hasTitle = job.title;
      const hasOrg = job.organization;
      const hasUrl = job.url;
      const isCyber = isCybersecurityJob(job);
      const isValidUrl = isValidApplyUrl(job.url);
      
      console.log(`Job: ${job.title} - hasTitle: ${hasTitle}, hasOrg: ${hasOrg}, hasUrl: ${hasUrl}, isCyber: ${isCyber}, isValidUrl: ${isValidUrl}`);
      
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
      salary: job.salary_raw?.substring(0, 100) || null,
      job_type: job.employment_type?.[0] || 'FULL_TIME',
      description_snippet: job.description_text?.substring(0, 1000) || null,
      created_at: new Date().toISOString(),
      updated_at: new Date().toISOString()
    }));
    
    // Insert into database
    const { data, error } = await supabase
      .from('jobs')
      .insert(normalizedJobs)
      .select();
    
    if (error) {
      console.error('Database error:', error);
      return res.status(500).json({ error: 'Database error', details: error.message });
    }
    
    console.log(`Inserted ${data.length} jobs into database`);
    
    res.json({
      message: 'Sync completed',
      fetched: jobs.length,
      filtered: filteredJobs.length,
      inserted: data.length
    });
    
  } catch (error) {
    console.error('Sync error:', error.message);
    console.error('Error stack:', error.stack);
    if (error.response) {
      console.error('API Error Response:', error.response.status, error.response.data);
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
  console.log('Endpoints:');
  console.log('  GET /health - Health check');
  console.log('  GET /sync - Fetch and store jobs');
  console.log('  GET /jobs - Get all jobs');
  console.log('  GET /jobs/:id - Get job by ID');
});