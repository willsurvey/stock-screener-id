// pages/api/health.js
export default function handler(req, res) {
  return res.status(200).json({
    status: 'ok',
    timestamp: new Date().toISOString(),
    message: 'Vercel API is running!'
  });
}