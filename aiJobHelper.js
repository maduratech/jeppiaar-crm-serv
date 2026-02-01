import { createClient } from "@supabase/supabase-js";

// Helper to run an AI job with retry/backoff and log attempts to DB table `ai_jobs`.
// Expects `supabase` client to be created in calling module, but we'll create
// a local client if not supplied via params.

export async function runAiJob({
  supabaseClient,
  jobType = "itinerary_generation",
  leadId = null,
  payload = {},
  maxRetries = 3,
  backoffBaseMs = 1000,
  runFn,
}) {
  const supabase =
    supabaseClient ||
    createClient(
      process.env.SUPABASE_URL,
      process.env.SUPABASE_SERVICE_ROLE_KEY
    );

  // Insert job record
  const jobRecord = {
    lead_id: leadId,
    job_type: jobType,
    status: "pending",
    attempts: 0,
    last_error: null,
    payload: JSON.stringify(payload || {}),
    created_at: new Date().toISOString(),
    updated_at: new Date().toISOString(),
  };

  const { data: created, error: createErr } = await supabase
    .from("ai_jobs")
    .insert(jobRecord)
    .select()
    .single();

  const jobId = created?.id || null;

  let attempt = 0;
  let lastErr = null;
  while (attempt < maxRetries) {
    attempt += 1;
    try {
      // Update attempts counter
      await supabase
        .from("ai_jobs")
        .update({
          attempts: attempt,
          status: "running",
          updated_at: new Date().toISOString(),
        })
        .eq("id", jobId);

      const result = await runFn({ attempt });

      // On success write result and mark completed
      await supabase
        .from("ai_jobs")
        .update({
          status: "completed",
          result: JSON.stringify(result || {}),
          updated_at: new Date().toISOString(),
        })
        .eq("id", jobId);

      return { success: true, result };
    } catch (err) {
      lastErr = err;
      const errMsg = (err && (err.message || String(err))) || "Unknown error";
      // Update job with error
      await supabase
        .from("ai_jobs")
        .update({
          last_error: errMsg,
          attempts: attempt,
          status: attempt >= maxRetries ? "failed" : "retrying",
          updated_at: new Date().toISOString(),
        })
        .eq("id", jobId);

      // Exponential backoff
      const delayMs = backoffBaseMs * Math.pow(2, attempt - 1);
      await new Promise((res) => setTimeout(res, delayMs));
    }
  }

  return { success: false, error: lastErr };
}
