// Simple rate limiter middleware
// Can be extended to use Redis for distributed systems

class RateLimiter {
  constructor() {
    this.requests = new Map();
  }

  // Clean up old entries periodically
  cleanup() {
    const now = Date.now();
    for (const [key, data] of this.requests.entries()) {
      if (now > data.resetTime) {
        this.requests.delete(key);
      }
    }
  }

  // Middleware factory
  createLimiter(options = {}) {
    const {
      windowMs = 15 * 60 * 1000, // 15 minutes
      max = 10, // 10 requests per window
      message = "Too many requests, please try again later",
      keyGenerator = (req) => {
        // Default: use user ID if available, otherwise IP
        return req.user?.id || req.ip || "anonymous";
      },
    } = options;

    // Cleanup every 5 minutes
    setInterval(() => this.cleanup(), 5 * 60 * 1000);

    return (req, res, next) => {
      const key = keyGenerator(req);
      const now = Date.now();

      // Clean up old entries for this key
      const existing = this.requests.get(key);
      if (existing && now > existing.resetTime) {
        this.requests.delete(key);
      }

      // Get or create request data
      let requestData = this.requests.get(key);
      if (!requestData) {
        requestData = {
          count: 0,
          resetTime: now + windowMs,
        };
        this.requests.set(key, requestData);
      }

      // Check if limit exceeded
      if (requestData.count >= max) {
        return res.status(429).json({
          message,
          retryAfter: Math.ceil((requestData.resetTime - now) / 1000),
        });
      }

      // Increment count
      requestData.count++;

      // Add rate limit headers
      res.setHeader("X-RateLimit-Limit", max);
      res.setHeader(
        "X-RateLimit-Remaining",
        Math.max(0, max - requestData.count)
      );
      res.setHeader(
        "X-RateLimit-Reset",
        new Date(requestData.resetTime).toISOString()
      );

      next();
    };
  }
}

// Singleton instance
export const rateLimiter = new RateLimiter();

// Pre-configured limiters
export const aiGenerationLimiter = rateLimiter.createLimiter({
  windowMs: 15 * 60 * 1000, // 15 minutes
  max: 10, // 10 requests per 15 minutes
  message: "Too many AI generation requests. Please wait before trying again.",
  keyGenerator: (req) => {
    return req.user?.id || req.ip || "anonymous";
  },
});
