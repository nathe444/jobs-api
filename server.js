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

const ORG_LOGO_SIZE = 256;

function extractDomain(inputUrl) {
  if (!inputUrl) return null;
  try {
    const parsed = new URL(inputUrl);
    return parsed.hostname.replace(/^www\./, '');
  } catch {
    return null;
  }
}

// Generate a URL-friendly slug from a string (only letters, gaps replaced by -)
function generateSlug(text) {
  if (!text) return null;
  return text
    .toLowerCase()
    .replace(/[^a-z]+/g, '-')  // Replace any non-letter character(s) with a single -
    .replace(/^-+|-+$/g, '');   // Remove leading/trailing dashes
}

function buildFaviconUrl(domain) {
  if (!domain) return null;
  return `https://www.google.com/s2/favicons?domain=${encodeURIComponent(domain)}&sz=${ORG_LOGO_SIZE}`;
}

const SERPAPI_KEY = (process.env.SERPAPI_KEY || '').trim();
const COMPANY_ENRICH_API_KEY = (process.env.COMPANY_ENRICH_API_KEY || '').trim();
const GROQ_API_KEY = (process.env.GROQ_API_KEY || '').trim();

// Job categories for classification
const JOB_CATEGORIES = [
  'INTERNSHIPS',
  'DEVSECOPS',
  'SECURITY-ENGINEER',
  'INFOSEC',
  'ANALYST',
  'CLOUD-SECURITY',
  'GRC',
  'PENETRATION-TESTING',
  'SALES'
];

// Rate limiting helper
const delay = (ms) => new Promise(resolve => setTimeout(resolve, ms));

// Classify job category using Groq API (Llama 3)
async function classifyJobCategory(title, descriptionSnippet) {
  if (!GROQ_API_KEY) {
    return null;
  }

  const prompt = `Classify this cybersecurity job into exactly ONE category.

Categories: ${JOB_CATEGORIES.join(', ')}

Job Title: ${title || 'N/A'}
Description: ${(descriptionSnippet || '').substring(0, 500)}

Respond with ONLY the category name, nothing else.`;

  try {
    const response = await axios.post(
      'https://api.groq.com/openai/v1/chat/completions',
      {
        model: 'llama-3.3-70b-versatile',
        messages: [{ role: 'user', content: prompt }],
        temperature: 0.1,
        max_tokens: 30
      },
      {
        headers: {
          'Authorization': `Bearer ${GROQ_API_KEY}`,
          'Content-Type': 'application/json'
        }
      }
    );

    const rawCategory = response.data?.choices?.[0]?.message?.content?.trim().toUpperCase();
    
    // Validate the response is one of our categories
    const matchedCategory = JOB_CATEGORIES.find(cat => 
      rawCategory === cat || rawCategory?.includes(cat)
    );

    return matchedCategory || null;
  } catch (error) {
    console.warn('Groq classification failed:', error.response?.data?.error?.message || error.message);
    return null;
  }
}

async function fetchOrganizationUrlFromSerpApi(organization) {
  if (!SERPAPI_KEY || !organization) return null;
  try {
    const response = await axios.get('https://serpapi.com/search.json', {
      params: {
        q: `${organization} official website`,
        engine: 'google',
        api_key: SERPAPI_KEY,
        num: 3
      }
    });

    const organic = response.data?.organic_results || [];
    const firstLink = organic.find(item => item?.link)?.link || null;
    return firstLink;
  } catch (error) {
    const status = error.response?.status;
    const message = error.response?.data?.error || error.message;
    console.warn('SerpAPI lookup failed:', status || '', message);
    return null;
  }
}

async function resolveOrganizationUrls(job) {
  const orgUrl = job.organization_url || null;
  const orgDomain = extractDomain(orgUrl);
  const logoFromOrgUrl = buildFaviconUrl(orgDomain);

  if (orgUrl || !SERPAPI_KEY) {
    return {
      organizationUrl: orgUrl,
      organizationLogoUrl: job.organization_logo || logoFromOrgUrl
    };
  }

  const serpOrgUrl = await fetchOrganizationUrlFromSerpApi(job.organization || '');
  const serpDomain = extractDomain(serpOrgUrl);
  const serpLogoUrl = buildFaviconUrl(serpDomain);

  return {
    organizationUrl: serpOrgUrl,
    organizationLogoUrl: job.organization_logo || serpLogoUrl
  };
}

async function fetchCompanyDetails(companyDomain) {
  if (!COMPANY_ENRICH_API_KEY || !companyDomain) return null;
  try {
    const response = await axios.get('https://api.companyenrich.com/companies/enrich', {
      params: { domain: companyDomain },
      headers: {
        Authorization: `Bearer ${COMPANY_ENRICH_API_KEY}`,
        'Content-Type': 'application/json'
      }
    });
    return response.data || null;
  } catch (error) {
    const status = error.response?.status;
    const message = error.response?.data || error.message;
    console.warn('CompanyEnrich lookup failed:', status || '', message);
    return null;
  }
}

async function upsertCompanyByOrganizationUrl(organizationUrl, companyName, companyDomain, companySlug) {
  if (!organizationUrl) return;

  const companyData = await fetchCompanyDetails(companyDomain);

  const locationParts = [
    companyData?.location?.address,
    companyData?.location?.city?.name,
    companyData?.location?.state?.name,
    companyData?.location?.country?.name
  ].filter(Boolean);

  const payload = {
    organization_url: organizationUrl,
    company_name: companyName ? companyName.substring(0, 100) : null,
    company_slug: companySlug,
    about: companyData?.description || null,
    founded_year: companyData?.founded_year
      ? new Date(`${companyData.founded_year}-01-01`).toISOString().split('T')[0]
      : null,
    industries: companyData?.industries || (companyData?.industry ? [companyData.industry] : null),
    socials: companyData?.socials || null,
    logo_url: companyData?.logo_url || null,
    location: locationParts.length ? locationParts.join(', ') : null,
    long_description: companyData?.seo_description || null,
    size: companyData?.employees || null,
    website: companyData?.website || null,
    updated_at: new Date().toISOString()
  };

  const { error } = await supabase
    .from('companies')
    .upsert(payload, { onConflict: 'organization_url' });

  if (error) {
    console.warn('Company upsert failed:', error.message);
  }
}

async function updateCompanyJobsCount(organizationUrl) {
  if (!organizationUrl) return;

  const { count, error: countError } = await supabase
    .from('jobs')
    .select('id', { count: 'exact', head: true })
    .eq('organization_url', organizationUrl);

  if (countError) {
    console.warn('Jobs count failed:', countError.message);
    return;
  }

  const { error: updateError } = await supabase
    .from('companies')
    .update({ jobs_count: count || 0, updated_at: new Date().toISOString() })
    .eq('organization_url', organizationUrl);

  if (updateError) {
    console.warn('Jobs count update failed:', updateError.message);
  }
}

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
  const aiCurrency = job.ai_salary_currency ?? null;
  const aiValue = job.ai_salary_value ?? null;
  const aiMin = job.ai_salary_minvalue ?? null;
  const aiMax = job.ai_salary_maxvalue ?? null;
  const aiUnit = job.ai_salary_unittext ?? null;
  const rawIsObject = raw && typeof raw === 'object' && !Array.isArray(raw);
  const monetaryValue = rawIsObject ? raw.value : null;
  const valueIsObject = monetaryValue && typeof monetaryValue === 'object' && !Array.isArray(monetaryValue);

  const min = job.salary_min ?? job.salary_min_derived ?? job.salary_range_min ?? job.salary_from ?? (valueIsObject ? monetaryValue.minValue : null) ?? aiMin;
  const max = job.salary_max ?? job.salary_max_derived ?? job.salary_range_max ?? job.salary_to ?? (valueIsObject ? monetaryValue.maxValue : null) ?? aiMax;
  const value = valueIsObject ? monetaryValue.value : null;
  const currency = job.salary_currency ?? job.currency ?? job.compensation_currency ?? (rawIsObject ? raw.currency : null) ?? aiCurrency;
  const period = job.salary_period ?? job.salary_unit ?? job.compensation_period ?? (valueIsObject ? monetaryValue.unitText : null) ?? aiUnit;

  const toNumber = (value) => {
    if (value === null || value === undefined) return null;
    if (typeof value === 'number' && Number.isFinite(value)) return value;
    const cleaned = String(value).replace(/[^0-9.]/g, '');
    const parsed = Number(cleaned);
    return Number.isFinite(parsed) ? parsed : null;
  };

  const minNum = toNumber(min ?? value ?? aiValue);
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
      limit: 150,
      offset: 0,
      title_filter: 'cybersecurity',
      description_type: 'text',
      remote: true,
      include_ai: true,
      ai_has_salary: true
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
  const filteredJobs = jobs;
  
  console.log(`Filtered to ${filteredJobs.length} cybersecurity jobs`);
  
  const processedCompanies = new Set();

  // Normalize and prepare for database (sequential to respect Groq rate limits)
  const normalizedJobs = [];
  for (let i = 0; i < filteredJobs.length; i++) {
    const job = filteredJobs[i];
    console.log(`Processing job ${i + 1}/${filteredJobs.length}: ${job.title?.substring(0, 50)}`);
    
    const { organizationUrl, organizationLogoUrl } = await resolveOrganizationUrls(job);

    const companyName = job.organization?.substring(0, 100) || null;
    const companyDomain = job.domain_derived
      || extractDomain(organizationUrl)
      || null;

    // Generate slugs
    const companySlug = generateSlug(companyName);
    const jobTitleSlug = generateSlug(job.title);
    const externalId = job.id?.toString() || Math.random().toString(36).substring(2, 10);
    const idSuffix = externalId.slice(-8); // Use last 8 chars of external_job_id for uniqueness
    // Make job_slug unique by combining company_slug, job title slug, and id suffix
    const jobSlug = companySlug && jobTitleSlug 
      ? `${companySlug}-${jobTitleSlug}-${idSuffix}` 
      : `${jobTitleSlug}-${idSuffix}`;

    if (organizationUrl && !processedCompanies.has(organizationUrl)) {
      processedCompanies.add(organizationUrl);
      if (companyDomain) {
        await upsertCompanyByOrganizationUrl(organizationUrl, companyName, companyDomain, companySlug);
      }
    }

    const fullDescription = job.description_text || null;

    // Classify job category using Groq (with rate limiting - 2 sec between calls for 30 req/min limit)
    const jobCategory = await classifyJobCategory(job.title, fullDescription);
    if (GROQ_API_KEY && i < filteredJobs.length - 1) {
      await delay(2100); // ~28 requests per minute to stay under 30 req/min limit
    }

    normalizedJobs.push({
      title: job.title.substring(0, 255),
      company: companyName,
      company_slug: companySlug,
      job_slug: jobSlug,
      category: jobCategory,
      location: job.locations_alt_raw?.[0] || 'Remote',
      is_remote: job.location_type === 'TELECOMMUTE' || job.remote_derived === true,
      apply_url: job.url,
      source: job.source || 'active-jobs-db',
      external_job_id: job.id?.toString() || Math.random().toString(),
      posted_at: new Date(job.date_posted || job.date_created || new Date()).toISOString(),
      last_updated: new Date(job.date_created || new Date()).toISOString(),
      salary: normalizeSalary(job),
      job_type: job.employment_type?.[0] || 'FULL_TIME',
      description_snippet: fullDescription,
      organization_url: organizationUrl,
      organization_logo_url: organizationLogoUrl,
      created_at: new Date().toISOString(),
      updated_at: new Date().toISOString()
    });
  }
  
  // Upsert into database (prevents duplicates based on external_job_id)
  const { data, error } = await supabase
    .from('jobs')
    .upsert(normalizedJobs, { onConflict: 'external_job_id' })
    .select();
  
  if (error) {
    throw new Error(`Database error: ${error.message}`);
  }
  
  await Promise.all([...processedCompanies].map(updateCompanyJobsCount));

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