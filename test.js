// Simple test to verify the server works
const axios = require('axios');

async function testServer() {
  const baseUrl = 'http://localhost:3000';
  
  try {
    // Test health endpoint
    console.log('Testing health endpoint...');
    const healthResponse = await axios.get(`${baseUrl}/health`);
    console.log('Health check:', healthResponse.data);
    
    // Test jobs endpoint (should return empty array if no jobs)
    console.log('\nTesting jobs endpoint...');
    const jobsResponse = await axios.get(`${baseUrl}/jobs`);
    console.log(`Found ${jobsResponse.data.length} jobs`);
    
    console.log('\nServer is working correctly!');
    console.log('To sync jobs, call: GET /sync');
    
  } catch (error) {
    console.error('Error testing server:', error.message);
    if (error.code === 'ECONNREFUSED') {
      console.log('Make sure the server is running: npm start');
    }
  }
}

testServer();