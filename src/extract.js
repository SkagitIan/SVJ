// extract-queue consumer — reads R2 snapshot, calls Gemini, upserts job_postings

const GEMINI_URL =
  'https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent';

export async function handleExtractBatch(batch, env) {
  for (const message of batch.messages) {
    const { business_id, r2_key } = message.body;
    try {
      await extractJobs(business_id, r2_key, env);
      message.ack();
    } catch (err) {
      console.error(`Extract error for business ${business_id}:`, err.message);
      await logCrawl(env, business_id, 'error', `extract: ${err.message}`);
      message.retry();
    }
  }
}

async function extractJobs(business_id, r2_key, env) {
  const obj = await env.R2.get(r2_key);
  if (!obj) throw new Error(`R2 key not found: ${r2_key}`);
  const pageText = await obj.text();

  const response = await fetch(`${GEMINI_URL}?key=${env.GEMINI_API_KEY}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      contents: [
        {
          parts: [
            {
              text: `Analyze this text from a company careers page. Extract all active job postings.
Return ONLY a JSON array. Each object must have:
  - job_title (string)
  - department (string or null)
  - salary_info (string or null)
  - application_url (string or null)
If no jobs are found, return [].
Do not include any explanation or markdown formatting.

PAGE TEXT:
${pageText.slice(0, 30000)}`,
            },
          ],
        },
      ],
      generationConfig: { temperature: 0.1 },
    }),
  });

  if (!response.ok) {
    throw new Error(`Gemini API ${response.status}: ${await response.text()}`);
  }

  const data = await response.json();
  const raw = data.candidates?.[0]?.content?.parts?.[0]?.text ?? '[]';

  let jobs = [];
  try {
    const cleaned = raw.replace(/^```json\n?/, '').replace(/\n?```$/, '').trim();
    jobs = JSON.parse(cleaned);
    if (!Array.isArray(jobs)) jobs = [];
  } catch {
    jobs = [];
  }

  const extractStart = Math.floor(Date.now() / 1000);

  for (const job of jobs) {
    if (!job.job_title?.trim()) continue;

    const existing = await env.DB.prepare(
      'SELECT id FROM job_postings WHERE business_id = ? AND job_title = ?'
    )
      .bind(business_id, job.job_title.trim())
      .first();

    if (existing) {
      await env.DB.prepare(
        `UPDATE job_postings
         SET last_seen_at = ?, is_active = 1,
             department = ?, salary_info = ?, application_url = ?
         WHERE id = ?`
      )
        .bind(
          extractStart,
          job.department ?? null,
          job.salary_info ?? null,
          job.application_url ?? null,
          existing.id
        )
        .run();
    } else {
      await env.DB.prepare(
        `INSERT INTO job_postings
           (business_id, job_title, department, salary_info, application_url, first_seen_at, last_seen_at)
         VALUES (?, ?, ?, ?, ?, ?, ?)`
      )
        .bind(
          business_id,
          job.job_title.trim(),
          job.department ?? null,
          job.salary_info ?? null,
          job.application_url ?? null,
          extractStart,
          extractStart
        )
        .run();
    }
  }

  // Expire jobs that weren't in this extraction
  await env.DB.prepare(
    `UPDATE job_postings SET is_active = 0
     WHERE business_id = ? AND last_seen_at < ? AND is_active = 1`
  )
    .bind(business_id, extractStart)
    .run();

  await logCrawl(
    env,
    business_id,
    'extract_ok',
    `${jobs.length} job(s) extracted`
  );
}

async function logCrawl(env, business_id, status, message) {
  await env.DB.prepare(
    'INSERT INTO crawl_log (business_id, status, message) VALUES (?, ?, ?)'
  )
    .bind(business_id, status, message ?? null)
    .run();
}
