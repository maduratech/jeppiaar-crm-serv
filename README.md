# Jeppiaar CRM – Backend Server

Backend API for Jeppiaar CRM: proxy to Supabase, auth middleware, WhatsApp webhook, and server-side jobs.

## Setup

1. **Install dependencies**
   ```bash
   npm install
   ```

2. **Configure environment**
   - Copy `.env.example` to `.env`
   - Fill in `SUPABASE_URL`, `SUPABASE_SERVICE_ROLE_KEY`, and other vars (see `.env.example`). **Never commit `.env`.**

3. **Run**
   ```bash
   npm start
   ```
   Default port: `3001` (override with `PORT` in `.env`).

## Main pieces

- **index.js** – Express app: CORS, auth middleware, API routes (leads, customers, staff, etc.), WhatsApp webhook.
- **whatsapp-crm.js** – WhatsApp CRM bot logic.
- **whatsapp-bot.js** – Bot entry / webhook handler.
- **utils/** – Cache, logger, rate limiter, PDF cleanup, progress tracker, etc.

## Env

See `.env.example` for all options. Required: `SUPABASE_URL`, `SUPABASE_SERVICE_ROLE_KEY`. Optional: WhatsApp tokens, SMTP, CORS origins.
