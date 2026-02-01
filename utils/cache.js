// Simple in-memory cache with TTL support
// Can be extended to use Redis in the future

class Cache {
  constructor() {
    this.cache = new Map();
    this.timers = new Map();
  }

  // Generate cache key from object
  generateKey(prefix, data) {
    const hash = JSON.stringify(data);
    // Simple hash function
    let hashValue = 0;
    for (let i = 0; i < hash.length; i++) {
      const char = hash.charCodeAt(i);
      hashValue = (hashValue << 5) - hashValue + char;
      hashValue = hashValue & hashValue; // Convert to 32-bit integer
    }
    return `${prefix}_${Math.abs(hashValue)}`;
  }

  // Set cache with TTL (time to live in milliseconds)
  set(key, value, ttl = 24 * 60 * 60 * 1000) {
    // Clear existing timer if any
    if (this.timers.has(key)) {
      clearTimeout(this.timers.get(key));
    }

    this.cache.set(key, {
      value,
      expiresAt: Date.now() + ttl,
    });

    // Set timer to auto-delete
    const timer = setTimeout(() => {
      this.cache.delete(key);
      this.timers.delete(key);
    }, ttl);

    this.timers.set(key, timer);
  }

  // Get from cache
  get(key) {
    const item = this.cache.get(key);
    if (!item) return null;

    // Check if expired
    if (Date.now() > item.expiresAt) {
      this.cache.delete(key);
      if (this.timers.has(key)) {
        clearTimeout(this.timers.get(key));
        this.timers.delete(key);
      }
      return null;
    }

    return item.value;
  }

  // Delete from cache
  delete(key) {
    this.cache.delete(key);
    if (this.timers.has(key)) {
      clearTimeout(this.timers.get(key));
      this.timers.delete(key);
    }
  }

  // Clear all cache
  clear() {
    this.timers.forEach((timer) => clearTimeout(timer));
    this.timers.clear();
    this.cache.clear();
  }

  // Get cache stats
  stats() {
    return {
      size: this.cache.size,
      keys: Array.from(this.cache.keys()),
    };
  }
}

// Singleton instance
export const cache = new Cache();

// Cleanup on process exit
process.on("SIGTERM", () => {
  cache.clear();
});

process.on("SIGINT", () => {
  cache.clear();
});
