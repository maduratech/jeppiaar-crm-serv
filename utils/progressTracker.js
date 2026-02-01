// Progress tracking utility for long-running operations
// Uses Server-Sent Events (SSE) or WebSocket for real-time updates

export class ProgressTracker {
  constructor() {
    this.trackers = new Map();
  }

  // Create a new progress tracker
  createTracker(requestId) {
    const tracker = {
      requestId,
      progress: 0,
      status: "pending",
      stages: [],
      currentStage: null,
      startTime: Date.now(),
      listeners: new Set(),
    };

    this.trackers.set(requestId, tracker);
    return tracker;
  }

  // Get tracker
  getTracker(requestId) {
    return this.trackers.get(requestId);
  }

  // Update progress
  updateProgress(requestId, progress, stage = null) {
    const tracker = this.trackers.get(requestId);
    if (!tracker) return;

    tracker.progress = Math.min(100, Math.max(0, progress));
    if (stage) {
      tracker.currentStage = stage;
      tracker.stages.push({
        stage,
        progress,
        timestamp: Date.now(),
      });
    }

    // Notify listeners
    this.notifyListeners(requestId, {
      progress: tracker.progress,
      stage: tracker.currentStage,
      status: tracker.status,
    });
  }

  // Update status
  updateStatus(requestId, status) {
    const tracker = this.trackers.get(requestId);
    if (!tracker) return;

    tracker.status = status;
    this.notifyListeners(requestId, {
      progress: tracker.progress,
      stage: tracker.currentStage,
      status: tracker.status,
    });
  }

  // Add listener (for SSE/WebSocket)
  addListener(requestId, listener) {
    const tracker = this.trackers.get(requestId);
    if (!tracker) return;

    tracker.listeners.add(listener);
  }

  // Remove listener
  removeListener(requestId, listener) {
    const tracker = this.trackers.get(requestId);
    if (!tracker) return;

    tracker.listeners.delete(listener);
  }

  // Notify all listeners
  notifyListeners(requestId, data) {
    const tracker = this.trackers.get(requestId);
    if (!tracker) return;

    tracker.listeners.forEach((listener) => {
      try {
        listener(data);
      } catch (err) {
        console.error("Error notifying progress listener:", err);
      }
    });
  }

  // Cleanup tracker
  cleanup(requestId) {
    const tracker = this.trackers.get(requestId);
    if (tracker) {
      tracker.listeners.clear();
      this.trackers.delete(requestId);
    }
  }

  // Cleanup old trackers (older than 1 hour)
  cleanupOld() {
    const oneHourAgo = Date.now() - 60 * 60 * 1000;
    for (const [requestId, tracker] of this.trackers.entries()) {
      if (tracker.startTime < oneHourAgo) {
        this.cleanup(requestId);
      }
    }
  }
}

// Singleton instance
export const progressTracker = new ProgressTracker();

// Cleanup old trackers every 30 minutes
setInterval(() => {
  progressTracker.cleanupOld();
}, 30 * 60 * 1000);

// Helper function to create progress callback
export function createProgressCallback(requestId) {
  return (progress, stage) => {
    progressTracker.updateProgress(requestId, progress, stage);
  };
}
