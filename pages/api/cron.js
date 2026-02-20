// pages/api/cron.js
export default async function handler(req, res) {
  // ✅ Hanya terima POST request
  if (req.method !== 'POST') {
    return res.status(405).json({ 
      error: 'Method not allowed',
      message: 'Please use POST method'
    });
  }

  // ✅ Optional: Verify authorization header
  const authHeader = req.headers.authorization;
  const expectedToken = process.env.CRON_SECRET;
  
  if (expectedToken && authHeader !== `Bearer ${expectedToken}`) {
    return res.status(401).json({ 
      error: 'Unauthorized',
      message: 'Invalid or missing authorization header'
    });
  }

  try {
    console.log('🚀 Triggering GitHub Actions...');

    // ✅ Trigger GitHub Actions Workflow
    const response = await fetch(
      `https://api.github.com/repos/${process.env.GITHUB_OWNER}/${process.env.GITHUB_REPO}/actions/workflows/${process.env.WORKFLOW_FILE}/dispatches`,
      {
        method: 'POST',
        headers: {
          'Authorization': `token ${process.env.GITHUB_TOKEN}`,
          'Accept': 'application/vnd.github.v3+json',
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          ref: 'main',
          inputs: {
            trigger_source: 'vercel-cron',
            triggered_at: new Date().toISOString()
          }
        }),
      }
    );

    if (!response.ok) {
      const errorData = await response.text();
      console.error('GitHub API error:', errorData);
      throw new Error(`GitHub API returned ${response.status}: ${errorData}`);
    }

    console.log('✅ GitHub Actions triggered successfully!');

    return res.status(200).json({
      success: true,
      message: 'GitHub Actions workflow triggered successfully',
      timestamp: new Date().toISOString(),
      data: {
        repo: process.env.GITHUB_REPO,
        workflow: process.env.WORKFLOW_FILE,
        ref: 'main'
      }
    });

  } catch (error) {
    console.error('❌ Cron trigger error:', error);
    return res.status(500).json({
      success: false,
      error: error.message,
      timestamp: new Date().toISOString()
    });
  }
}