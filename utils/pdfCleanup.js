// PDF Cleanup Utility
// SAFETY: This ONLY deletes generated itinerary PDFs from the "public/itinerary-pdfs/" folder.
// It will NEVER delete:
//   - Customer documents (passports, Aadhaar cards, PAN cards, bank statements, invoices)
//   - Any files outside the "public/itinerary-pdfs/" folder
//   - Any files that don't match the itinerary PDF naming pattern (timestamp prefix)
//   - Any non-PDF files
//
// Deletes itinerary PDFs older than 1 day from Supabase storage
// Keeps itinerary records intact, only deletes storage files

import { createClient } from "@supabase/supabase-js";
import { logger } from "./logger.js";

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY
);

const BUCKET_NAME = "avatars";
const PDF_FOLDER_PREFIX = "public/itinerary-pdfs/";
const RETENTION_DAYS = 1; // Delete PDFs older than 1 day

/**
 * Extract timestamp from PDF filename
 * Format: {timestamp}-{filename}.pdf
 * @param {string} filename - The filename
 * @returns {number|null} - Timestamp in milliseconds or null if not found
 */
function extractTimestampFromFilename(filename) {
  // Filename format: {timestamp}-{rest of filename}.pdf
  const match = filename.match(/^(\d+)-/);
  if (match && match[1]) {
    return parseInt(match[1], 10);
  }
  return null;
}

/**
 * Check if a file is older than retention period
 * @param {string} filePath - Full path to the file
 * @param {Date} createdAt - File creation date from storage metadata
 * @returns {boolean} - True if file should be deleted
 */
function shouldDeleteFile(filePath, createdAt) {
  // First, try to extract timestamp from filename
  const filename = filePath.split("/").pop();
  const timestamp = extractTimestampFromFilename(filename);

  let fileDate;
  if (timestamp) {
    fileDate = new Date(timestamp);
  } else if (createdAt) {
    fileDate = new Date(createdAt);
  } else {
    // If we can't determine the date, don't delete (safety)
    logger.warn("Cannot determine file date, skipping", { filePath });
    return false;
  }

  const now = new Date();
  const daysDiff = (now - fileDate) / (1000 * 60 * 60 * 24);

  return daysDiff > RETENTION_DAYS;
}

/**
 * Clean up old PDFs from a specific customer folder
 * @param {string} customerFolder - Customer folder path (e.g., "public/itinerary-pdfs/123")
 * @returns {Promise<{deleted: number, errors: number}>}
 */
async function cleanupCustomerFolder(customerFolder) {
  let deleted = 0;
  let errors = 0;

  try {
    // List all files in the customer folder
    const { data: files, error: listError } = await supabase.storage
      .from(BUCKET_NAME)
      .list(customerFolder, {
        limit: 1000,
        sortBy: { column: "created_at", order: "desc" },
      });

    if (listError) {
      logger.error("Error listing files in customer folder", {
        customerFolder,
        error: listError.message,
      });
      return { deleted: 0, errors: 1 };
    }

    if (!files || files.length === 0) {
      logger.debug("No files found in customer folder", { customerFolder });
      return { deleted: 0, errors: 0 };
    }

    // Filter files that should be deleted
    // SAFETY: Only delete files that:
    // 1. Are PDF files (.pdf extension)
    // 2. Are in the itinerary-pdfs folder (already verified by customerFolder path)
    // 3. Match the itinerary PDF naming pattern (timestamp prefix) OR are old enough
    const filesToDelete = files.filter((file) => {
      // Only process PDF files
      if (!file.name || !file.name.toLowerCase().endsWith('.pdf')) {
        return false;
      }
      
      // Verify the file path is in the itinerary-pdfs folder (safety check)
      const filePath = `${customerFolder}/${file.name}`;
      if (!filePath.startsWith(PDF_FOLDER_PREFIX)) {
        logger.warn("File path outside itinerary-pdfs folder, skipping", { filePath });
        return false;
      }
      
      // Only delete files that match itinerary PDF pattern (timestamp prefix) or are old enough
      const hasTimestampPrefix = /^\d+-/.test(file.name);
      if (!hasTimestampPrefix) {
        // If file doesn't match itinerary PDF pattern, skip it (safety)
        logger.debug("File doesn't match itinerary PDF pattern, skipping", { fileName: file.name });
        return false;
      }
      
      return shouldDeleteFile(filePath, file.created_at);
    });

    if (filesToDelete.length === 0) {
      logger.debug("No old files to delete in customer folder", {
        customerFolder,
        totalFiles: files.length,
      });
      return { deleted: 0, errors: 0 };
    }

    logger.info("Found files to delete", {
      customerFolder,
      totalFiles: files.length,
      filesToDelete: filesToDelete.length,
    });

    // Delete files in batches (Supabase has limits)
    const batchSize = 50;
    for (let i = 0; i < filesToDelete.length; i += batchSize) {
      const batch = filesToDelete.slice(i, i + batchSize);
      const filePaths = batch.map((file) => `${customerFolder}/${file.name}`);

      const { data: deletedFiles, error: deleteError } = await supabase.storage
        .from(BUCKET_NAME)
        .remove(filePaths);

      if (deleteError) {
        logger.error("Error deleting files batch", {
          customerFolder,
          batchIndex: i,
          error: deleteError.message,
        });
        errors += batch.length;
      } else {
        deleted += deletedFiles?.length || 0;
        logger.debug("Deleted files batch", {
          customerFolder,
          batchIndex: i,
          deletedCount: deletedFiles?.length || 0,
        });
      }
    }

    return { deleted, errors };
  } catch (error) {
    logger.error("Unexpected error in cleanupCustomerFolder", {
      customerFolder,
      error: error.message,
      stack: error.stack,
    });
    return { deleted: 0, errors: 1 };
  }
}

/**
 * Main cleanup function - deletes PDFs older than retention period
 * @param {Object} options - Cleanup options
 * @param {boolean} options.dryRun - If true, only log what would be deleted without actually deleting
 * @returns {Promise<{deleted: number, errors: number, foldersProcessed: number}>}
 */
export async function cleanupOldPdfs(options = {}) {
  const { dryRun = false } = options;
  const startTime = Date.now();

  logger.info("Starting PDF cleanup", {
    retentionDays: RETENTION_DAYS,
    dryRun,
  });

  let totalDeleted = 0;
  let totalErrors = 0;
  let foldersProcessed = 0;

  try {
    // List all folders in the itinerary-pdfs directory
    const { data: folders, error: foldersError } = await supabase.storage
      .from(BUCKET_NAME)
      .list(PDF_FOLDER_PREFIX.replace(/\/$/, ""), {
        limit: 1000,
      });

    if (foldersError) {
      logger.error("Error listing PDF folders", {
        error: foldersError.message,
      });
      throw foldersError;
    }

    if (!folders || folders.length === 0) {
      logger.info("No PDF folders found");
      return {
        deleted: 0,
        errors: 0,
        foldersProcessed: 0,
        duration: Date.now() - startTime,
      };
    }

    logger.info("Found PDF folders to process", {
      folderCount: folders.length,
    });

    // Process each customer folder
    for (const folder of folders) {
      if (!folder.name || folder.name === ".emptyFolderPlaceholder") {
        continue;
      }

      const customerFolder = `${PDF_FOLDER_PREFIX}${folder.name}`;
      foldersProcessed++;

      if (dryRun) {
        // In dry run mode, just log what would be deleted
        const { data: files } = await supabase.storage
          .from(BUCKET_NAME)
          .list(customerFolder, {
            limit: 1000,
          });

          if (files) {
            const filesToDelete = files.filter((file) => {
              // Only process PDF files
              if (!file.name || !file.name.toLowerCase().endsWith('.pdf')) {
                return false;
              }
              
              // Verify the file path is in the itinerary-pdfs folder (safety check)
              const filePath = `${customerFolder}/${file.name}`;
              if (!filePath.startsWith(PDF_FOLDER_PREFIX)) {
                return false;
              }
              
              // Only delete files that match itinerary PDF pattern (timestamp prefix)
              const hasTimestampPrefix = /^\d+-/.test(file.name);
              if (!hasTimestampPrefix) {
                return false;
              }
              
              return shouldDeleteFile(filePath, file.created_at);
            });

          if (filesToDelete.length > 0) {
            logger.info("DRY RUN: Would delete files", {
              customerFolder,
              fileCount: filesToDelete.length,
              files: filesToDelete.map((f) => f.name),
            });
            totalDeleted += filesToDelete.length;
          }
        }
      } else {
        // Actually delete files
        const result = await cleanupCustomerFolder(customerFolder);
        totalDeleted += result.deleted;
        totalErrors += result.errors;
      }
    }

    const duration = Date.now() - startTime;

    logger.info("PDF cleanup completed", {
      deleted: totalDeleted,
      errors: totalErrors,
      foldersProcessed,
      duration,
      dryRun,
    });

    return {
      deleted: totalDeleted,
      errors: totalErrors,
      foldersProcessed,
      duration,
    };
  } catch (error) {
    logger.error("PDF cleanup failed", {
      error: error.message,
      stack: error.stack,
      duration: Date.now() - startTime,
    });
    throw error;
  }
}

/**
 * Schedule cleanup job to run every 1 day at 2 AM IST
 * Uses Indian Standard Time (UTC+5:30)
 */
export function scheduleDailyCleanup() {
  const CLEANUP_HOUR = parseInt(process.env.PDF_CLEANUP_HOUR || "2", 10); // Default 2 AM IST
  const CLEANUP_MINUTE = parseInt(process.env.PDF_CLEANUP_MINUTE || "0", 10); // Default 0 minutes
  const CLEANUP_INTERVAL_DAYS = 1; // Run every 1 day

  // Convert IST to UTC (IST is UTC+5:30)
  const IST_OFFSET_MS = 5.5 * 60 * 60 * 1000; // 5 hours 30 minutes in milliseconds

  function getNextRunTime() {
    const now = new Date();

    // IST offset: UTC+5:30 = 5.5 hours
    const IST_OFFSET_HOURS = 5.5;

    // Convert current UTC time to IST
    const nowIST = new Date(now.getTime() + IST_OFFSET_HOURS * 60 * 60 * 1000);

    // Create target time in IST (2 AM)
    const targetIST = new Date(nowIST);
    targetIST.setUTCHours(CLEANUP_HOUR, CLEANUP_MINUTE, 0, 0);
    targetIST.setUTCMilliseconds(0);

    // If target time has passed today, add 1 day
    if (targetIST <= nowIST) {
      targetIST.setUTCDate(targetIST.getUTCDate() + CLEANUP_INTERVAL_DAYS);
    }

    // Convert back to UTC for setTimeout
    const targetUTC = new Date(
      targetIST.getTime() - IST_OFFSET_HOURS * 60 * 60 * 1000
    );

    return targetUTC;
  }

  function scheduleNextRun() {
    const nextRun = getNextRunTime();
    const msUntilNext = nextRun.getTime() - Date.now();

    const IST_OFFSET_HOURS = 5.5;
    // Convert to IST for logging
    const nextRunIST = new Date(
      nextRun.getTime() + IST_OFFSET_HOURS * 60 * 60 * 1000
    );

    logger.info("Scheduling next PDF cleanup", {
      nextRunUTC: nextRun.toISOString(),
      nextRunIST: nextRunIST.toISOString(),
      daysUntilNext: (msUntilNext / (1000 * 60 * 60 * 24)).toFixed(2),
      hoursUntilNext: (msUntilNext / (1000 * 60 * 60)).toFixed(2),
    });

    setTimeout(async () => {
      try {
        const runTimeIST = new Date(
          Date.now() + IST_OFFSET_HOURS * 60 * 60 * 1000
        );
        logger.info("Starting scheduled PDF cleanup", {
          scheduledTimeUTC: new Date().toISOString(),
          scheduledTimeIST: runTimeIST.toISOString(),
        });
        await cleanupOldPdfs({ dryRun: false });
      } catch (error) {
        logger.error("Scheduled PDF cleanup failed", {
          error: error.message,
          stack: error.stack,
        });
      } finally {
        // Schedule next run (1 day from now)
        scheduleNextRun();
      }
    }, Math.max(0, msUntilNext)); // Ensure non-negative
  }

  // Start scheduling
  scheduleNextRun();

  logger.info("PDF cleanup scheduler started", {
    cleanupHour: CLEANUP_HOUR,
    cleanupMinute: CLEANUP_MINUTE,
    timezone: "IST (UTC+5:30)",
    intervalDays: CLEANUP_INTERVAL_DAYS,
    retentionDays: RETENTION_DAYS,
  });
}
