import express from "express";
import cors from "cors";
import dotenv from "dotenv";
dotenv.config();
import { createClient } from "@supabase/supabase-js";
import nodemailer from "nodemailer";
import fetch from "node-fetch";
import multer from "multer";
import ExcelJS from "exceljs";
import whatsappBotApp from "./whatsapp-bot.js";
import whatsappCrmRouter from "./whatsapp-crm.js";
import { logger } from "./utils/logger.js";
import { cleanupOldPdfs, scheduleDailyCleanup } from "./utils/pdfCleanup.js";
import { normalizePhone } from "./phoneUtils.js";
import { startTokenMonitoring } from "./utils/tokenMonitor.js";
import {
  sendCrmWhatsappText,
  sendCrmWhatsappReplyButtons,
  sendCrmWhatsappCtaUrl,
  sendCrmWhatsappTemplate,
  uploadWhatsappMedia,
} from "./whatsapp-crm.js";

// WhatsApp invoice template definition (hardcoded)
// Body variables (in order):
// {{1}} Customer name
// {{2}} Invoice number
// {{3}} Booking ID
// {{4}} Booking fee amount
// {{5}} Destination
// Button (dynamic URL): set template to https://rzp.io/i/{{1}} and we send the Razorpay slug
const WHATSAPP_INVOICE_TEMPLATE = "booking_invoice_payment";
const WHATSAPP_TEMPLATE_LANG = "en";

const app = express();

// CORS: env-driven allowed origins (comma-separated). Fallback for legacy deployments.
const envOrigins = process.env.CORS_ORIGINS;
const allowedOrigins = envOrigins
  ? envOrigins
      .split(",")
      .map((o) => o.trim().replace(/\/$/, ""))
      .filter(Boolean)
  : [
      "https://jeppiaar.vercel.app",
      "https://crm.jeppiaaracademy.com",
      "https://jeppiaaracademy.com",
      "https://www.jeppiaaracademy.com",
      "http://jeppiaaracademy.com",
      "http://www.jeppiaaracademy.com",
      "http://localhost:5173",
    ];

const corsOptions = {
  origin: function (origin, callback) {
    const normalizedOrigin =
      origin && origin.replace ? origin.replace(/\/$/, "") : origin;
    if (!origin || allowedOrigins.indexOf(normalizedOrigin) !== -1) {
      callback(null, true);
    } else if (envOrigins) {
      callback(new Error("Not allowed by CORS"));
    } else if (
      normalizedOrigin &&
      (normalizedOrigin.includes("jeppiaaracademy.com") ||
        normalizedOrigin.includes("jeppiaar.vercel.app"))
    ) {
      callback(null, true);
    } else {
      callback(new Error("Not allowed by CORS"));
    }
  },
  credentials: true,
  allowedHeaders: ["Content-Type", "Authorization"],
  methods: ["GET", "POST", "PUT", "DELETE", "OPTIONS"],
};

// Academy mode: when APP_MODE=academy, only these path patterns are allowed (others 404).
const APP_MODE = process.env.APP_MODE || "";
const ACADEMY_ALLOWED_PATTERNS = [
  /^\/$/,
  /^\/health$/,
  /^\/webhook$/,
  /^\/api\/lead\/website$/,
  /^\/api\/lead\/whatsapp$/,
  /^\/api\/lead\/notify-immediate$/,
  /^\/api\/sessions\/login$/,
  /^\/api\/sessions\/logout$/,
  /^\/api\/sessions\/heartbeat$/,
  /^\/api\/sessions\/report$/,
  /^\/api\/whatsapp\/send-text$/,
  /^\/api\/whatsapp\/health$/,
  /^\/api\/settings\/.+/, // /api/settings/:key
  /^\/api\/job-applicants\/?.*/, // /api/job-applicants and /api/job-applicants/:id
  /^\/api\/invoicing\/.+/, // create-link, send-whatsapp, etc.
  /^\/api\/razorpay-webhook$/,
  /^\/api\/feedback\/send$/,
  /^\/api\/customers\/bulk-delete$/,
];
function academyAllowlist(req, res, next) {
  if (APP_MODE !== "academy") return next();
  const path = (req.baseUrl || "") + (req.path || "");
  const allowed = ACADEMY_ALLOWED_PATTERNS.some((re) => re.test(path));
  if (allowed) return next();
  return res.status(404).json({ message: "Not found" });
}

app.use(cors(corsOptions));
app.use(express.json({ limit: "50mb" })); // For JSON payloads (increased for file metadata)
app.use(express.urlencoded({ extended: true, limit: "50mb" })); // For form-data payloads from Elementor
app.use(academyAllowlist);

// Configure multer for file uploads (memory storage)
const storage = multer.memoryStorage();
const upload = multer({
  storage: storage,
  limits: {
    fileSize: 50 * 1024 * 1024, // 50MB limit
  },
  fileFilter: (req, file, cb) => {
    // Allow PDF and Excel files
    const allowedTypes = [
      "application/pdf",
      "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
      "application/vnd.ms-excel",
      "application/excel",
      "application/x-excel",
      "application/x-msexcel",
    ];
    if (allowedTypes.includes(file.mimetype)) {
      cb(null, true);
    } else {
      cb(new Error("Invalid file type. Only PDF and Excel files are allowed."));
    }
  },
});

// Configure multer for resume uploads (PDF, DOC, DOCX only)
const resumeUpload = multer({
  storage: storage,
  limits: {
    fileSize: 10 * 1024 * 1024, // 10MB limit
  },
  fileFilter: (req, file, cb) => {
    // Allow PDF, DOC, DOCX files
    const allowedTypes = [
      "application/pdf",
      "application/msword",
      "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    ];
    const allowedExtensions = [".pdf", ".doc", ".docx"];
    const fileExtension =
      "." + file.originalname.split(".").pop().toLowerCase();

    if (
      allowedTypes.includes(file.mimetype) ||
      allowedExtensions.includes(fileExtension)
    ) {
      cb(null, true);
    } else {
      cb(
        new Error(
          "Invalid file type. Only PDF, DOC, and DOCX files are allowed."
        )
      );
    }
  },
});

// Mount the standalone WhatsApp bot app
// This will handle routes like /webhook defined in whatsapp-bot.js
app.use(whatsappBotApp);

// Mount the standalone WhatsApp CRM router
// This will handle routes like /api/whatsapp/conversations, /api/whatsapp/messages, etc.
app.use(whatsappCrmRouter);

const PORT = process.env.PORT || 3001;

// Message ID to Lead ID mapping cache (for tracking delivery failures)
// Format: { messageId: { leadId, staffName, staffPhone, timestamp } }
// Entries expire after 24 hours
export const messageIdToLeadCache = new Map();
const MESSAGE_CACHE_EXPIRY = 24 * 60 * 60 * 1000; // 24 hours in milliseconds

// Clean up expired cache entries periodically
setInterval(() => {
  const now = Date.now();
  for (const [messageId, data] of messageIdToLeadCache.entries()) {
    if (now - data.timestamp > MESSAGE_CACHE_EXPIRY) {
      messageIdToLeadCache.delete(messageId);
    }
  }
}, 60 * 60 * 1000); // Clean up every hour

// Function to handle WhatsApp message delivery failures
export async function handleWhatsAppMessageFailure(
  messageId,
  errorCode,
  errorTitle,
  recipientId
) {
  const cached = messageIdToLeadCache.get(messageId);
  if (!cached) {
    console.log(
      `[CRM] âš ï¸ Message failure for ${messageId} but no cached lead mapping found.`
    );
    return;
  }

  const { leadId, staffName, staffPhone } = cached;

  // Determine error message based on error code
  let errorMessage = `WhatsApp message delivery failed for staff "${staffName}" (${staffPhone}). `;

  if (errorCode === 131049) {
    errorMessage += `Error Code ${errorCode}: ${
      errorTitle ||
      "Message not delivered to maintain healthy ecosystem engagement"
    }. `;
    errorMessage += `Possible reasons: Staff may have blocked the business number, opted out of messages, or phone number is not registered on WhatsApp.`;
  } else if (errorCode === 131047) {
    errorMessage += `Error Code ${errorCode}: ${
      errorTitle || "Re-engagement message"
    }. `;
    errorMessage += `The customer needs to send a message first, or you must use a WhatsApp template message if outside the 24-hour messaging window.`;
  } else {
    errorMessage += `Error Code ${errorCode}: ${
      errorTitle || "Unknown error"
    }.`;
  }

  console.error(
    `[CRM] âŒ ${errorMessage} (Lead: ${leadId}, Message ID: ${messageId})`
  );

  // Log to lead activity
  await logLeadActivity(leadId, "WhatsApp Failed", errorMessage, "System");
}

// --- SUPABASE CLIENT ---
// Create Supabase client with service role key (bypasses RLS)
const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY,
  {
    auth: {
      autoRefreshToken: true,
      persistSession: false,
    },
  }
);

// --- AUTHENTICATION MIDDLEWARE ---
// Middleware to check if user is authenticated
const requireAuth = async (req, res, next) => {
  try {
    // Allow internal calls to bypass authentication
    // Internal calls are made from within the server (e.g., from internal services)
    if (req.headers["x-internal-call"] === "true") {
      // For internal calls, create a system user object
      // This allows internal API calls to work without requiring a real user token
      req.user = {
        id: 0,
        name: "System",
        role: "System",
        role_id: null,
      };
      return next();
    }

    // Check for service role key for internal service-to-service calls
    const { authorization } = req.headers;
    if (authorization && authorization.startsWith("Bearer ")) {
      const token = authorization.split(" ")[1];

      // Check if it's the service role key (for internal calls)
      if (token === process.env.SUPABASE_SERVICE_ROLE_KEY) {
        req.user = {
          id: 0,
          name: "System",
          role: "System",
          role_id: null,
        };
        return next();
      }
    }

    // Standard authentication flow for external requests
    if (!authorization) {
      return res.status(401).json({ message: "Unauthorized" });
    }

    const token = authorization.split(" ")[1];
    if (!token) {
      return res.status(401).json({ message: "Invalid token format" });
    }

    const {
      data: { user },
      error: authError,
    } = await supabase.auth.getUser(token);

    if (authError || !user) {
      const errorMessage =
        authError?.message ||
        authError?.toString() ||
        JSON.stringify(authError) ||
        "No user";

      // Reduce log noise for expected/transient auth failures
      const isConnectionError =
        errorMessage.includes("fetch failed") ||
        errorMessage.includes("Connect Timeout") ||
        errorMessage.includes("UND_ERR_CONNECT_TIMEOUT");
      const isSessionMissing =
        errorMessage.includes("Auth session missing") ||
        errorMessage.includes("session missing") ||
        errorMessage.includes("invalid claim") ||
        errorMessage.includes("JWT expired");

      if (isConnectionError) {
        if (Math.random() < 0.1) {
          console.warn(
            "[Auth Middleware] Connection timeout to Supabase (network issue). This may be temporary."
          );
        }
      } else if (isSessionMissing) {
        // Expected when token expired or request has no valid session - log at debug level only
        if (process.env.NODE_ENV === "development" && Math.random() < 0.05) {
          console.warn(
            "[Auth Middleware] Token validation failed (expired/missing session). Request rejected with 401."
          );
        }
      } else {
        console.error(
          "[Auth Middleware] Token validation error:",
          errorMessage
        );
      }

      return res.status(401).json({ message: "Invalid token" });
    }

    // Get staff profile
    const { data: staffProfile, error: profileError } = await supabase
      .from("staff")
      .select("*")
      .eq("user_id", user.id)
      .single();

    if (profileError || !staffProfile) {
      console.error(
        "[Auth Middleware] Staff profile error:",
        profileError?.message || "No profile"
      );
      return res.status(403).json({ message: "Staff profile not found" });
    }

    // Map role_id to role name
    const roleIdToName = {
      1: "Super Admin",
      2: "Manager",
      3: "Staff",
    };
    const roleName = roleIdToName[staffProfile.role_id] || "Staff";

    // Use staff row flags; single default branch (no multi-branch)
    req.user = {
      ...staffProfile,
      role_id: staffProfile.role_id,
      role: roleName,
      is_lead_manager: Boolean(staffProfile.is_lead_manager),
      is_accountant: Boolean(staffProfile.is_accountant),
      manage_lead_branches: staffProfile.is_lead_manager ? [1] : [],
    };

    next();
  } catch (error) {
    console.error("[Auth Middleware] Unexpected error:", error);
    // Return 401 instead of 500 for auth errors
    return res.status(401).json({
      message: "Authentication error",
      error: process.env.NODE_ENV === "development" ? error.message : undefined,
    });
  }
};

// Middleware to check if user is Super Admin
const requireSuperAdmin = async (req, res, next) => {
  try {
    const { authorization } = req.headers;
    if (!authorization) {
      return res.status(401).json({ message: "Unauthorized" });
    }

    const token = authorization.split(" ")[1];
    const {
      data: { user },
    } = await supabase.auth.getUser(token);

    if (!user) {
      return res.status(401).json({ message: "Invalid token" });
    }

    // Check if user is Super Admin
    const { data: staffProfile, error: profileError } = await supabase
      .from("staff")
      .select("role_id")
      .eq("user_id", user.id)
      .single();

    // Check if role_id is 1 (Super Admin)
    if (profileError || staffProfile?.role_id !== 1) {
      return res
        .status(403)
        .json({ message: "Forbidden: Super Admin access required." });
    }

    next();
  } catch (error) {
    console.error("[Sources API] Auth error:", error);
    return res.status(401).json({ message: "Authentication failed" });
  }
};

// --- NODEMAILER TRANSPORTER ---
const transporter = nodemailer.createTransport({
  host: process.env.SMTP_HOST,
  port: parseInt(process.env.SMTP_PORT || "465", 10),
  secure: true, // true for 465, false for other ports
  auth: {
    user: process.env.SMTP_USER,
    pass: process.env.SMTP_PASS,
  },
});

// --- WHATSAPP NOTIFICATION LOGIC (FOR CRM) ---
// WhatsApp sending functions are now imported from whatsapp-crm.js
// Rate limiting and throttling are handled in whatsapp-crm.js
// These constants are still needed for template sending in sendStaffAssignmentNotification, sendDailyProductivitySummary, etc.
const WHATSAPP_TOKEN = process.env.WHATSAPP_TOKEN;
const WHATSAPP_PHONE_NUMBER_ID = process.env.WHATSAPP_PHONE_NUMBER_ID;
const WHATSAPP_GRAPH_API_BASE = `https://graph.facebook.com/v20.0/${WHATSAPP_PHONE_NUMBER_ID}/messages`;
const BULK_MESSAGE_DELAY = 500; // 500ms delay between different recipients in bulk operations

export async function logLeadActivity(
  leadId,
  type,
  description,
  user = "System"
) {
  try {
    const { data: lead, error: fetchError } = await supabase
      .from("leads")
      .select("activity")
      .eq("id", leadId)
      .single();

    if (fetchError) {
      console.error(
        `[ActivityLogger] Failed to fetch lead ${leadId}: ${fetchError.message}`
      );
      return;
    }

    const newActivity = {
      id: Date.now(),
      type,
      description,
      user,
      timestamp: new Date().toISOString(),
    };

    const updatedActivity = [newActivity, ...(lead.activity || [])];

    const { error: updateError } = await supabase
      .from("leads")
      .update({
        activity: updatedActivity,
        last_updated: new Date().toISOString(),
      })
      .eq("id", leadId);

    if (updateError) {
      console.error(
        `[ActivityLogger] Failed to log activity for lead ${leadId}: ${updateError.message}`
      );
    } else {
      console.log(
        `[ActivityLogger] Successfully logged '${type}' for lead ${leadId}.`
      );
    }
  } catch (error) {
    console.error(
      `[ActivityLogger] CRITICAL error for lead ${leadId}:`,
      error.message
    );
  }
}

// --- GLOBAL REALTIME LISTENERS ---
function setupGlobalListeners() {
  try {
    const channel = supabase.channel("global-listeners");

    // INSERT on leads: Log only (auto staff assignment removed)
    channel.on(
      "postgres_changes",
      { event: "INSERT", schema: "public", table: "leads" },
      async (payload) => {
        const record = payload.new || payload.record || payload;
        console.log(
          "[GlobalListener] New lead inserted:",
          record?.id || record
        );
      }
    );

    // UPDATE on leads: Handle status changes and itinerary generation
    // NOTE: MTS summary is ONLY sent once when lead is created (INSERT listener)
    // Do NOT send summary on any UPDATE events (status changes, destination changes, etc.)
    channel.on(
      "postgres_changes",
      { event: "UPDATE", schema: "public", table: "leads" },
      async (payload) => {
        const startTime = Date.now();
        const oldRec = payload.old || payload.previous || null;
        const newRec = payload.new || payload.record || payload;
        try {
          if (!newRec) return;

          // Log all UPDATE events for debugging
          console.log(
            `[GlobalListener] UPDATE event received for lead ${
              newRec.id
            }. Status: ${newRec.status}, Old status: ${oldRec?.status || "N/A"}`
          );

          // Detect *actual* changes to destination, travel date, services, duration, passenger details, or status.
          // We ONLY act when Supabase provides the previous row (`oldRec`).
          // This prevents repeated WhatsApp sends when the user simply "saves" the lead
          // without changing these key fields.
          const destinationChanged =
            !!oldRec && oldRec.destination !== newRec.destination;

          const travelDateChanged =
            !!oldRec && oldRec.travel_date !== newRec.travel_date;

          const servicesChanged =
            !!oldRec &&
            JSON.stringify(oldRec.services || []) !==
              JSON.stringify(newRec.services || []);

          // Detect duration changes
          const durationChanged =
            !!oldRec && oldRec.duration !== newRec.duration;

          // Detect passenger details changes (requirements.rooms, adults, children)
          const oldRequirements = oldRec?.requirements || {};
          const newRequirements = newRec?.requirements || {};
          const oldRooms = oldRequirements?.rooms || [];
          const newRooms = newRequirements?.rooms || [];
          const oldAdults = oldRequirements?.adults || oldRec?.adults || 0;
          const newAdults = newRequirements?.adults || newRec?.adults || 0;
          const oldChildren =
            oldRequirements?.children || oldRec?.children || 0;
          const newChildren =
            newRequirements?.children || newRec?.children || 0;

          // Check if passenger details changed (rooms array or adults/children counts)
          const passengerDetailsChanged =
            !!oldRec &&
            (JSON.stringify(oldRooms) !== JSON.stringify(newRooms) ||
              oldAdults !== newAdults ||
              newChildren !== newChildren);

          // Check if travel_date was added (changed from null/empty to a valid date)
          // This is important for sending MTS summary when agents fill in missing required fields
          const travelDateAdded =
            !!oldRec &&
            (!oldRec.travel_date ||
              oldRec.travel_date === null ||
              String(oldRec.travel_date).trim() === "" ||
              new Date(oldRec.travel_date).getFullYear() <= 1970) &&
            newRec.travel_date &&
            String(newRec.travel_date).trim() !== "" &&
            new Date(newRec.travel_date).getFullYear() > 1970;

          // WhatsApp messages are only triggered for Feedback status
          // All other status changes do not trigger WhatsApp messages
          const statusChanged = !!oldRec && oldRec.status !== newRec.status;

          // Handle Feedback status separately - send feedback even if oldRec is null
          // This ensures feedback is sent when status is changed to Feedback
          if (newRec.status === "Feedback") {
            console.log(
              `[GlobalListener] Lead ${newRec.id} status is Feedback. Checking if feedback needs to be sent...`
            );
            try {
              // Fetch customer for feedback
              const { data: customer } = await supabase
                .from("customers")
                .select("*")
                .eq("id", newRec.customer_id)
                .single();

              if (customer) {
                await sendFeedbackLinkMessage(newRec, customer);
                console.log(
                  `[GlobalListener] Feedback check complete for lead ${newRec.id}`
                );
              } else {
                console.log(
                  `[GlobalListener] âš ï¸ Customer not found for lead ${newRec.id}. Cannot send feedback.`
                );
              }
            } catch (feedbackError) {
              console.error(
                `[GlobalListener] Error sending feedback template for lead ${newRec.id}:`,
                feedbackError.message,
                feedbackError.stack
              );
            }
          }

          // If we don't have a previous row snapshot, we can't reliably know if
          // anything important changed, so we skip to avoid duplicate messages.
          // (But Feedback is already handled above)
          if (!oldRec) {
            console.log(
              `[GlobalListener] Lead ${newRec.id} update received without previous row; no significant-field diff check possible. Skipping other actions.`
            );
            return;
          }

          // Send summary when lead is created and staff is assigned (status is Enquiry)
          // Also send when status changes to "Processing" (customer confirmed)
          const isTourPackage = newRec.services?.includes("Tour Package");
          const isEnquiryStatus = newRec.status === "Enquiry";
          const isProcessingStatus = newRec.status === "Processing";

          // Check if staff was just assigned (by checking if lead_assignees was inserted)
          // This will be handled by the lead_assignees INSERT listener

          // MTS SUMMARY SHOULD BE SENT ONLY FOR SPECIFIC CHANGES:
          // - Services add or remove
          // - Destination change
          // - Duration change
          // - Passenger Details change
          // - Travel date added (when it was null/empty and now has a valid date)
          // DO NOT send summary when ONLY status changes
          const shouldSendSummary =
            servicesChanged ||
            destinationChanged ||
            durationChanged ||
            passengerDetailsChanged ||
            travelDateAdded;

          // Log all "Enquiry" to "Processing" status changes
          if (
            statusChanged &&
            oldRec.status === "Enquiry" &&
            isProcessingStatus
          ) {
            console.log(
              `[GlobalListener] Lead ${newRec.id} status changed from Enquiry to Processing.`
            );
            // Log this status change to lead activity
            try {
              await logLeadActivity(
                newRec.id,
                "Status Changed",
                `Lead status changed from Enquiry to Processing.`,
                "System"
              );
            } catch (logError) {
              console.error(
                `[GlobalListener] Failed to log status change to activity:`,
                logError.message
              );
            }
          }

          // Handle other status-specific actions (invoice creation, feedback links)
          if (statusChanged) {
            // Fetch customer for status-specific actions
            const { data: customer } = await supabase
              .from("customers")
              .select("*")
              .eq("id", newRec.customer_id)
              .single();

            if (customer) {
              // Note: Feedback status is handled earlier in the function (before oldRec check)
              // This ensures it works even if oldRec is null
            }
          }

          // Send MTS summary ONLY for specific changes: Services, Destination, Duration, Passenger Details, or Travel Date added
          // DO NOT send summary when ONLY status changes
          // Also check if all required fields are now filled before sending
          if (shouldSendSummary) {
            // Validate that all required fields are now filled
            const validation = validateMtsSummaryRequiredFields(newRec);
            if (!validation.isValid) {
              console.log(
                `[GlobalListener] Lead ${
                  newRec.id
                } has changes but still missing required fields: ${Object.entries(
                  validation.missingFields
                )
                  .filter(([_, missing]) => missing)
                  .map(([field]) => field)
                  .join(", ")}. Skipping MTS summary send.`
              );
            } else {
              console.log(
                `[GlobalListener] Lead ${newRec.id} has significant changes (Services: ${servicesChanged}, Destination: ${destinationChanged}, Duration: ${durationChanged}, Passenger Details: ${passengerDetailsChanged}, Travel Date Added: ${travelDateAdded}). All required fields filled. Sending updated summary to customer.`
              );

              // Fetch customer and staff for sending summary
              const { data: customer } = await supabase
                .from("customers")
                .select("*")
                .eq("id", newRec.customer_id)
                .single();

              if (customer && customer.phone) {
                // Fetch lead with assignees to get staff information
                const { data: leadWithAssignees } = await supabase
                  .from("leads")
                  .select("*, all_assignees:lead_assignees(staff(*))")
                  .eq("id", newRec.id)
                  .single();

                if (leadWithAssignees) {
                  // Get primary assigned staff (first assignee)
                  const primaryStaff =
                    leadWithAssignees.all_assignees &&
                    leadWithAssignees.all_assignees.length > 0
                      ? leadWithAssignees.all_assignees[0].staff
                      : {
                          id: 0,
                          name: "Madura Travel Service",
                          phone: process.env.DEFAULT_STAFF_PHONE || "",
                        };

                  // Check if summary was already sent recently (prevent duplicates)
                  const recentSummarySent = (
                    leadWithAssignees.activity || []
                  ).some(
                    (act) =>
                      (act.type === "Summary Sent" ||
                        act.type === "WhatsApp Sent") &&
                      (act.description?.includes("Summary sent") ||
                        act.description?.includes("template")) &&
                      new Date(act.timestamp) > new Date(Date.now() - 60000) // Last 60 seconds
                  );

                  if (!recentSummarySent) {
                    // DISABLED: MTS summary auto-sending
                    // try {
                    //   await sendWelcomeWhatsapp(
                    //     leadWithAssignees,
                    //     customer,
                    //     primaryStaff
                    //   );
                    //   console.log(
                    //     `[GlobalListener] âœ… Updated summary sent successfully to customer for lead ${newRec.id}`
                    //   );
                    // } catch (summaryError) {
                    //   console.error(
                    //     `[GlobalListener] âŒ Error sending updated summary to customer for lead ${newRec.id}:`,
                    //     summaryError.message
                    //   );
                    //   // Log error to lead activity
                    //   await logLeadActivity(
                    //     newRec.id,
                    //     "WhatsApp Failed",
                    //     `Failed to send updated summary to customer: ${summaryError.message}`,
                    //     "System"
                    //   );
                    // }
                    console.log(
                      `[GlobalListener] MTS summary auto-sending is disabled for lead ${newRec.id}`
                    );
                  } else {
                    console.log(
                      `[GlobalListener] Summary already sent recently for lead ${newRec.id}. Skipping duplicate.`
                    );
                  }
                }
              } else {
                console.log(
                  `[GlobalListener] âš ï¸ Cannot send summary for lead ${newRec.id}: Customer phone not available.`
                );
              }
            } // End of validation.isValid check
          } else if (statusChanged && !shouldSendSummary) {
            // Log when status changes but summary is NOT sent (as per requirement)
            console.log(
              `[GlobalListener] Lead ${newRec.id} status changed from "${oldRec.status}" to "${newRec.status}". Summary NOT sent (only status change, no Services/Destination/Duration/Passenger Details changes).`
            );
          }
        } catch (err) {
          console.error(
            "[GlobalListener] Error handling lead update:",
            err.message
          );
        }
      }
    );

    // INSERT on lead_assignees: Handled by dedicated listenForManualAssignments() function
    // This prevents duplicate notifications and ensures consistent messaging

    channel.subscribe((status, err) => {
      if (status === "SUBSCRIBED") {
        console.log("[GlobalListener] âœ… Subscribed to global DB changes.");
      } else if (err) {
        console.error("[GlobalListener] âŒ Subscription error:", err);
      }
    });
  } catch (err) {
    console.error("[GlobalListener] Failed to setup listeners:", err.message);
  }
}

// WhatsApp sending functions are now imported from whatsapp-crm.js
// sendCrmWhatsappText, sendCrmWhatsappReplyButtons, sendCrmWhatsappCtaUrl, sendCrmWhatsappTemplate

// Reusable function to generate summary text for leads
/**
 * Validate if all required fields are filled for MTS summary
 * Required fields:
 * 1. Services Required (services array)
 * 2. Destination
 * 3. Duration
 * 4. Date of Travel (travel_date)
 * 5. Passenger Details (adults/children in requirements)
 */
function validateMtsSummaryRequiredFields(lead) {
  // 1. Services Required
  const hasServices =
    lead.services && Array.isArray(lead.services) && lead.services.length > 0;

  // 2. Destination
  const hasDestination =
    lead.destination &&
    lead.destination !== "N/A" &&
    lead.destination.trim() !== "";

  // 3. Duration
  const hasDuration = lead.duration && lead.duration.trim() !== "";

  // 4. Date of Travel
  // Handle null, undefined, empty string, or invalid dates (like epoch 0 = 1970-01-01)
  // Also check check_in_date if travel_date is not available
  let hasTravelDate = false;
  let travelDateToCheck = lead.travel_date || lead.check_in_date;
  if (travelDateToCheck) {
    const travelDateStr = String(travelDateToCheck).trim();
    if (
      travelDateStr !== "" &&
      travelDateStr !== "null" &&
      travelDateStr !== "undefined"
    ) {
      // Check if it's a valid date (not epoch 0 = 1970-01-01)
      const dateObj = new Date(travelDateStr);
      if (!isNaN(dateObj.getTime()) && dateObj.getFullYear() > 1970) {
        hasTravelDate = true;
      }
    }
  }

  // 5. Passenger Details (adults or children must be filled)
  // First check rooms array, then fall back to requirements.adults/children
  let totalAdults = 0;
  if (
    lead.requirements?.rooms &&
    Array.isArray(lead.requirements.rooms) &&
    lead.requirements.rooms.length > 0
  ) {
    totalAdults = lead.requirements.rooms.reduce(
      (sum, room) => sum + (room.adults || 0),
      0
    );
  } else if (
    lead.requirements?.adults !== null &&
    lead.requirements?.adults !== undefined
  ) {
    totalAdults = parseInt(lead.requirements.adults) || 0;
  }

  let totalChildren = 0;
  if (
    lead.requirements?.rooms &&
    Array.isArray(lead.requirements.rooms) &&
    lead.requirements.rooms.length > 0
  ) {
    totalChildren = lead.requirements.rooms.reduce(
      (sum, room) => sum + (room.children || 0),
      0
    );
  } else if (
    lead.requirements?.children !== null &&
    lead.requirements?.children !== undefined
  ) {
    totalChildren = parseInt(lead.requirements.children) || 0;
  }

  const hasPassengerDetails = totalAdults > 0 || totalChildren > 0;

  // Make travelDate and passengerDetails optional for initial summary send
  // Agents will fill these later, but we can send summary with Services, Destination, and Duration
  // Only require Services, Destination, and Duration - travelDate and passengerDetails are optional
  const isValid = hasServices && hasDestination && hasDuration;
  // Note: hasTravelDate and hasPassengerDetails are now optional - agents will fill them later

  return {
    isValid,
    missingFields: {
      services: !hasServices,
      destination: !hasDestination,
      duration: !hasDuration,
      travelDate: !hasTravelDate, // Optional - shown in missingFields but doesn't block sending
      passengerDetails: !hasPassengerDetails, // Optional - shown in missingFields but doesn't block sending
    },
  };
}

function generateLeadSummary(lead, customer, staff) {
  const bookingId = `JA-${lead.id}`;

  // Calculate passengers
  const totalAdults =
    (lead.requirements?.rooms || []).reduce(
      (sum, room) => sum + (room.adults || 0),
      0
    ) || (lead.adults ? parseInt(lead.adults) : 0);
  const totalChildren =
    (lead.requirements?.rooms || []).reduce(
      (sum, room) => sum + (room.children || 0),
      0
    ) || (lead.children ? parseInt(lead.children) : 0);

  let passengerDetails = `${totalAdults} Adult(s)`;
  if (totalChildren > 0) {
    passengerDetails += `, ${totalChildren} Child(ren)`;
  }

  // Build summary parts
  const summaryParts = [];
  summaryParts.push(`Service: ${lead.services?.join(", ") || "N/A"}`);
  if (lead.destination && lead.destination !== "N/A") {
    summaryParts.push(`Destination: ${lead.destination}`);
  }
  if (lead.travel_date) {
    summaryParts.push(
      `Date of Travel: ${new Date(lead.travel_date).toLocaleDateString(
        "en-GB"
      )}`
    );
  }
  if (lead.duration) {
    summaryParts.push(`Duration: ${lead.duration}`);
  }
  if (totalAdults > 0 || totalChildren > 0) {
    summaryParts.push(`Passengers: ${passengerDetails}`);
  }

  const summaryText = summaryParts.join("\n");

  return {
    bookingId,
    summaryText,
    customerName: customer.first_name,
    staffName: staff.name,
  };
}

async function sendWelcomeWhatsapp(lead, customer, staff) {
  // Send mts_summary template as the single welcome/confirmation message for all leads
  // This template includes "Confirm Enquiry" and "Talk to Agent" buttons
  // This replaces separate welcome and confirmation messages - one template does both

  if (!customer.phone) {
    console.log(
      `[CRM] âš ï¸ Customer alert not sent for lead ${lead.id}: No phone number found for customer ${customer.id}.`
    );
    await logLeadActivity(
      lead.id,
      "WhatsApp Skipped",
      `Welcome/confirmation template not sent to customer "${customer.first_name} ${customer.last_name}" - no phone number.`
    );
    return;
  }

  // Check if summary template was already sent recently (prevent duplicates)
  const recentSummarySent = (lead.activity || []).some(
    (act) =>
      (act.type === "Summary Sent" || act.type === "WhatsApp Sent") &&
      (act.description?.includes("Summary sent") ||
        act.description?.includes("template")) &&
      new Date(act.timestamp) > new Date(Date.now() - 60000) // Last 60 seconds
  );

  if (recentSummarySent) {
    console.log(
      `[CRM] âš ï¸ Summary template already sent recently for lead ${lead.id}. Skipping duplicate.`
    );
    return false;
  }

  // Check if customer already confirmed via button click - don't send summary again when status changes to Confirmed
  const customerAlreadyConfirmed = (lead.activity || []).some(
    (act) =>
      act.type === "Customer Confirmed" &&
      act.description?.includes("confirmed the enquiry via WhatsApp")
  );

  if (customerAlreadyConfirmed && lead.status === "Confirmed") {
    console.log(
      `[CRM] âš ï¸ Customer already confirmed via WhatsApp button for lead ${lead.id}. Skipping duplicate summary send.`
    );
    return false;
  }

  // Validate that all required fields are filled before sending MTS summary
  const validation = validateMtsSummaryRequiredFields(lead);
  if (!validation.isValid) {
    // Only show truly required fields (Services, Destination, Duration)
    const requiredMissingFields = Object.entries(validation.missingFields)
      .filter(
        ([field, missing]) =>
          missing && ["services", "destination", "duration"].includes(field)
      )
      .map(([field]) => field)
      .join(", ");
    console.log(
      `[CRM] âš ï¸ Cannot send MTS summary for lead ${lead.id}: Missing required fields: ${requiredMissingFields}`
    );
    await logLeadActivity(
      lead.id,
      "WhatsApp Skipped",
      `MTS summary not sent - missing required fields: ${requiredMissingFields}. Please fill: Services, Destination, and Duration. (Date of Travel and Passenger Details are optional and can be filled by agents later.)`
    );
    return false;
  }

  // Log optional missing fields for information (but don't block sending)
  const optionalMissingFields = Object.entries(validation.missingFields)
    .filter(
      ([field, missing]) =>
        missing && ["travelDate", "passengerDetails"].includes(field)
    )
    .map(([field]) => field)
    .join(", ");
  if (optionalMissingFields) {
    console.log(
      `[CRM] â„¹ï¸ MTS summary will be sent for lead ${lead.id} but missing optional fields: ${optionalMissingFields}`
    );
  }

  const { bookingId, summaryText, customerName, staffName } =
    generateLeadSummary(lead, customer, staff);

  // Clean summary text for template: Remove newlines, tabs, and multiple consecutive spaces
  // Meta Business Manager templates don't allow newlines/tabs in text parameters
  const cleanSummaryText = (summaryText || "")
    .replace(/\n/g, " ") // Replace newlines with spaces
    .replace(/\t/g, " ") // Replace tabs with spaces
    .replace(/[ ]{5,}/g, " ") // Replace 5+ consecutive spaces with single space
    .replace(/[ ]{2,}/g, " ") // Replace 2+ consecutive spaces with single space
    .trim();

  // Prepare template components for mts_summary template
  // The template must have buttons defined in Meta Business Manager: "Confirm Enquiry" and "Talk to Agent"
  const templateComponents = [
    {
      type: "body",
      parameters: [
        { type: "text", text: customerName || "" }, // {{1}} - Customer name
        { type: "text", text: bookingId || "" }, // {{2}} - Booking ID
        { type: "text", text: staffName || "" }, // {{3}} - Staff name
        { type: "text", text: cleanSummaryText }, // {{4}} - Summary (cleaned)
      ],
    },
  ];

  // Normalize phone number - try multiple methods for better compatibility
  let sanitizedPhone = normalizePhone(customer.phone, "IN");

  // If normalization fails, try manual cleanup for common formats
  if (!sanitizedPhone && customer.phone) {
    const phoneStr = String(customer.phone).trim();
    // Remove spaces and common separators
    const cleaned = phoneStr.replace(/[\s\-\(\)]/g, "");
    // If it's already in +91 format or starts with 91, use it
    if (cleaned.startsWith("+91") || cleaned.startsWith("919")) {
      sanitizedPhone = cleaned.startsWith("+") ? cleaned : `+${cleaned}`;
      console.log(
        `[CRM] ðŸ“ž Manual phone normalization: ${customer.phone} â†’ ${sanitizedPhone}`
      );
    } else if (cleaned.length === 10) {
      // 10 digits - assume India
      sanitizedPhone = `+91${cleaned}`;
      console.log(
        `[CRM] ðŸ“ž Manual phone normalization (10 digits): ${customer.phone} â†’ ${sanitizedPhone}`
      );
    }
  }

  if (!sanitizedPhone) {
    console.error(
      `[CRM] âŒ Could not normalize customer phone for lead ${lead.id}: ${customer.phone}`
    );
    await logLeadActivity(
      lead.id,
      "WhatsApp Failed",
      `Failed to send welcome/confirmation template: Invalid phone number "${customer.phone}" for customer "${customer.first_name} ${customer.last_name}".`
    );
    return false;
  }

  console.log(
    `[CRM] ðŸ“ž Normalized phone to ${sanitizedPhone} for lead ${lead.id} (original: ${customer.phone})`
  );

  // Send mts_summary template (includes welcome message + confirmation buttons)
  // This is the ONLY message sent - it serves as both welcome and confirmation
  console.log(
    `[CRM] ðŸ“¤ Sending mts_summary template (welcome + confirmation) to ${sanitizedPhone} for lead ${lead.id}.`
  );

  const result = await sendCrmWhatsappTemplate(
    sanitizedPhone,
    "mts_summary",
    "en",
    templateComponents
  );

  if (result) {
    const messageId = result.messages?.[0]?.id;
    if (messageId) {
      // Store message ID -> lead ID mapping for button click handling
      messageIdToLeadCache.set(messageId, {
        leadId: lead.id,
        customerId: customer.id,
        customerName: `${customer.first_name} ${customer.last_name}`,
        timestamp: Date.now(),
      });
      console.log(
        `[CRM] âœ… mts_summary template sent successfully to ${sanitizedPhone} for lead ${lead.id}. Message ID: ${messageId}`
      );
    } else {
      console.log(
        `[CRM] âœ… mts_summary template sent successfully to ${sanitizedPhone} for lead ${lead.id} (no message ID in response).`
      );
    }
    await logLeadActivity(
      lead.id,
      "Summary Sent",
      `Welcome/confirmation template (mts_summary) sent to customer "${customer.first_name} ${customer.last_name}" via WhatsApp.`
    );
  } else {
    console.error(
      `[CRM] âŒ Failed to send mts_summary template for lead ${lead.id} to ${sanitizedPhone}. Template may not be approved or phone number invalid.`
    );
    await logLeadActivity(
      lead.id,
      "WhatsApp Failed",
      `Failed to send welcome/confirmation template (mts_summary) to customer "${customer.first_name} ${customer.last_name}" at ${sanitizedPhone}. Template may not be approved in Meta Business Manager.`
    );
    return false;
  }

  return true; // Success
}

async function sendStaffAssignmentNotification(
  lead,
  customer,
  assignee,
  assigneeType,
  primaryAssigneeName = null,
  specificService = null
) {
  console.log(
    `[CRM] Preparing staff notification for ${assignee.name} (Type: ${assigneeType}, Lead: ${lead.id})`
  );

  if (!customer) {
    console.error(
      `[CRM] Cannot send staff notification: Customer data missing for lead ${lead.id}`
    );
    return;
  }

  if (!assignee.phone) {
    console.log(
      `[CRM] âš ï¸ Staff alert not sent for lead ${lead.id}: No phone number found for staff ${assignee.name}.`
    );
    await logLeadActivity(
      lead.id,
      "WhatsApp Skipped",
      `Assignment notification not sent to staff "${assignee.name}" (no phone number).`,
      "System"
    );
    return;
  }

  let message = "";
  const customerPhoneRaw = customer.phone || "";
  const customerPhoneSanitized = customerPhoneRaw.replace(/\s/g, ""); // Sanitize phone number for the URL

  const leadNumber = `JA-${lead.id}`;

  // Services that don't require destination/travel date
  const nonTravelServices = ["Forex", "Passport", "Transport"];
  const hasNonTravelService = lead.services?.some((s) =>
    nonTravelServices.includes(s)
  );
  const hasTravelService = lead.services?.some(
    (s) => !nonTravelServices.includes(s)
  );

  if (assigneeType === "primary") {
    const totalAdults = (lead.requirements?.rooms || []).reduce(
      (sum, room) => sum + room.adults,
      0
    );
    const totalChildren = (lead.requirements?.rooms || []).reduce(
      (sum, room) => sum + room.children,
      0
    );
    const allServices = (lead.services || []).join(", ") || "N/A";

    // Build message parts conditionally based on service type
    const messageParts = [
      `*New Lead Assigned!* ðŸš€`,
      ``,
      `*Lead Number:* ${leadNumber}`,
      `*Services:* ${allServices}`,
      `*Customer:* ${customer.first_name} ${customer.last_name}`,
      `*Phone:* ${customer.phone}`,
    ];

    // Only show destination if it's a travel-related service
    if (hasTravelService && lead.destination && lead.destination !== "N/A") {
      messageParts.push(`*Destination:* ${lead.destination}`);
    }

    // Only show travel date if it's a travel-related service
    if (hasTravelService && lead.travel_date) {
      messageParts.push(
        `*Travel Date:* ${new Date(lead.travel_date).toLocaleDateString(
          "en-GB"
        )}`
      );
    }

    // Only show passengers if it's a travel-related service
    if (hasTravelService && (totalAdults > 0 || totalChildren > 0)) {
      messageParts.push(
        `*Passengers:* ${totalAdults} Adults, ${totalChildren} Children`
      );
    }

    // Build customer enquiry summary
    let enquirySummary = "";
    if (lead.summary && lead.summary.trim()) {
      enquirySummary = `\n\n*Customer Enquiry Summary:*\n${lead.summary}`;
    } else {
      // Fallback: create a basic summary from available data
      const summaryParts = [];
      if (hasTravelService && lead.destination && lead.destination !== "N/A") {
        summaryParts.push(`Travel to ${lead.destination}`);
      }
      if (hasTravelService && lead.travel_date) {
        summaryParts.push(
          `on ${new Date(lead.travel_date).toLocaleDateString("en-GB")}`
        );
      }
      if (allServices) {
        summaryParts.push(`for ${allServices}`);
      }
      if (summaryParts.length > 0) {
        enquirySummary = `\n\n*Customer Enquiry Summary:*\n${summaryParts.join(
          " "
        )}`;
      }
    }

    message = messageParts.join("\n") + enquirySummary;
  } else {
    // Secondary assignee
    const destinationText =
      hasTravelService && lead.destination && lead.destination !== "N/A"
        ? ` to *${lead.destination}*`
        : "";
    message = `*New Task Assigned!* ðŸ›‚\n\nYou've been assigned the *${specificService}* service for Lead ${leadNumber}${destinationText}.\n\nPlease coordinate with the primary agent, *${primaryAssigneeName}*, to process this request.`;
  }

  const initiateCallUrl = `https://api.jeppiaaracademy.com/api/initiate-call?leadId=${lead.id}&staffId=${assignee.id}&phone=${customerPhoneSanitized}`;

  // Use normalizePhone function for better phone number handling
  let sanitizedAssigneePhone = normalizePhone(assignee.phone, "IN");

  // Fallback: if normalizePhone fails, try manual cleanup for common formats
  if (!sanitizedAssigneePhone && assignee.phone) {
    const phoneStr = String(assignee.phone).trim();
    // Remove spaces and common separators
    const cleaned = phoneStr.replace(/[\s\-\(\)]/g, "");
    // If it's already in +91 format or starts with 91, use it
    if (cleaned.startsWith("+91") || cleaned.startsWith("919")) {
      sanitizedAssigneePhone = cleaned.startsWith("+")
        ? cleaned
        : `+${cleaned}`;
      console.log(
        `[CRM] Manual phone normalization for staff ${assignee.name}: ${assignee.phone} â†’ ${sanitizedAssigneePhone}`
      );
    } else if (cleaned.length === 10) {
      // 10 digits - assume India
      sanitizedAssigneePhone = `+91${cleaned}`;
      console.log(
        `[CRM] Manual phone normalization (10 digits) for staff ${assignee.name}: ${assignee.phone} â†’ ${sanitizedAssigneePhone}`
      );
    } else if (cleaned.length > 10 && cleaned.length <= 15) {
      // International number - try adding + prefix
      sanitizedAssigneePhone = cleaned.startsWith("+")
        ? cleaned
        : `+${cleaned}`;
      console.log(
        `[CRM] Manual phone normalization (international) for staff ${assignee.name}: ${assignee.phone} â†’ ${sanitizedAssigneePhone}`
      );
    }
  }

  if (sanitizedAssigneePhone) {
    console.log(
      `[CRM] Attempting to send assignment alert to ${assignee.name} at ${sanitizedAssigneePhone} (original: ${assignee.phone}).`
    );

    let result = null;

    // Try sending via WhatsApp template first
    try {
      if (assigneeType === "primary") {
        // Use staff_lead_assigned template for primary assignments
        const templatePayload = {
          messaging_product: "whatsapp",
          to: sanitizedAssigneePhone,
          type: "template",
          template: {
            name: "staff_lead_assigned",
            language: { code: "en" },
            components: [
              {
                type: "body",
                parameters: [
                  { type: "text", text: leadNumber },
                  {
                    type: "text",
                    text: (lead.services || []).join(", ") || "N/A",
                  },
                  {
                    type: "text",
                    text: `${customer.first_name} ${customer.last_name}`,
                  },
                  { type: "text", text: customer.phone },
                  {
                    type: "text",
                    text:
                      lead.destination && lead.destination !== "N/A"
                        ? lead.destination
                        : "N/A",
                  },
                  {
                    type: "text",
                    text: lead.travel_date
                      ? new Date(lead.travel_date).toLocaleDateString("en-GB")
                      : "N/A",
                  },
                ],
              },
              {
                type: "button",
                sub_type: "url",
                index: 0,
                parameters: [{ type: "text", text: initiateCallUrl }],
              },
            ],
          },
        };

        console.log(
          `[CRM] ðŸ“¤ Sending staff_lead_assigned template to ${sanitizedAssigneePhone}`
        );
        const response = await fetch(WHATSAPP_GRAPH_API_BASE, {
          method: "POST",
          headers: {
            Authorization: `Bearer ${WHATSAPP_TOKEN}`,
            "Content-Type": "application/json",
          },
          body: JSON.stringify(templatePayload),
        });

        const apiResult = await response.json();
        console.log(
          `[CRM] ðŸ“‹ Full WhatsApp API Response for ${assignee.name}:`,
          JSON.stringify(apiResult, null, 2)
        );

        if (response.ok && apiResult.messages) {
          result = apiResult;
          const messageId = apiResult.messages[0]?.id;
          console.log(
            `[CRM] âœ… Template message sent successfully to ${assignee.name} (${sanitizedAssigneePhone}). Message ID: ${messageId}`
          );

          // Log warning if there are any issues in the response
          if (apiResult.messages[0]?.message_status) {
            console.log(
              `[CRM] âš ï¸ Message status: ${apiResult.messages[0].message_status}`
            );
          }
        } else {
          const errorDetails = apiResult.error || apiResult;
          // Check for token expiration (error code 190)
          if (
            errorDetails.code === 190 ||
            errorDetails.type === "OAuthException"
          ) {
            console.error(
              `[CRM] ðŸ”´ TOKEN EXPIRED: WhatsApp token has expired!`,
              errorDetails.message || ""
            );
            console.error(
              `[CRM] âš ï¸ Action required: Generate a new token and update WHATSAPP_TOKEN environment variable`
            );
          }
          console.error(
            `[CRM] âŒ Template message failed for ${
              assignee.name
            } (${sanitizedAssigneePhone}). Status: ${
              response.status
            }, Error: ${JSON.stringify(errorDetails, null, 2)}`
          );
          throw new Error(
            `WhatsApp API error: ${JSON.stringify(errorDetails)}`
          );
        }
      } else {
        // Use staff_task_assigned template for secondary assignments
        const templatePayload = {
          messaging_product: "whatsapp",
          to: sanitizedAssigneePhone,
          type: "template",
          template: {
            name: "staff_task_assigned",
            language: { code: "en" },
            components: [
              {
                type: "body",
                parameters: [
                  { type: "text", text: specificService || "Task" },
                  { type: "text", text: leadNumber },
                  {
                    type: "text",
                    text: primaryAssigneeName || "Primary Agent",
                  },
                ],
              },
              {
                type: "button",
                sub_type: "url",
                index: 0,
                parameters: [{ type: "text", text: initiateCallUrl }],
              },
            ],
          },
        };

        console.log(
          `[CRM] ðŸ“¤ Sending staff_task_assigned template to ${assignee.name} (${sanitizedAssigneePhone})`
        );
        const response = await fetch(WHATSAPP_GRAPH_API_BASE, {
          method: "POST",
          headers: {
            Authorization: `Bearer ${WHATSAPP_TOKEN}`,
            "Content-Type": "application/json",
          },
          body: JSON.stringify(templatePayload),
        });

        const apiResult = await response.json();
        console.log(
          `[CRM] ðŸ“‹ Full WhatsApp API Response for ${assignee.name}:`,
          JSON.stringify(apiResult, null, 2)
        );

        if (response.ok && apiResult.messages) {
          result = apiResult;
          const messageId = apiResult.messages[0]?.id;
          console.log(
            `[CRM] âœ… Template message sent successfully to ${assignee.name} (${sanitizedAssigneePhone}). Message ID: ${messageId}`
          );

          // Log warning if there are any issues in the response
          if (apiResult.messages[0]?.message_status) {
            console.log(
              `[CRM] âš ï¸ Message status: ${apiResult.messages[0].message_status}`
            );
          }
        } else {
          const errorDetails = apiResult.error || apiResult;
          // Check for token expiration (error code 190)
          if (
            errorDetails.code === 190 ||
            errorDetails.type === "OAuthException"
          ) {
            console.error(
              `[CRM] ðŸ”´ TOKEN EXPIRED: WhatsApp token has expired!`,
              errorDetails.message || ""
            );
            console.error(
              `[CRM] âš ï¸ Action required: Generate a new token and update WHATSAPP_TOKEN environment variable`
            );
          }
          console.error(
            `[CRM] âŒ Template message failed for ${
              assignee.name
            } (${sanitizedAssigneePhone}). Status: ${
              response.status
            }, Error: ${JSON.stringify(errorDetails, null, 2)}`
          );
          throw new Error(
            `WhatsApp API error: ${JSON.stringify(errorDetails)}`
          );
        }
      }
    } catch (templateError) {
      console.warn(
        `[CRM] âš ï¸ Template message failed for ${assignee.name}. Trying plain text fallback:`,
        templateError.message
      );
      // Fallback: Send as plain text WITHOUT URL (template should have the button)
      result = await sendCrmWhatsappText(sanitizedAssigneePhone, message);

      if (!result) {
        console.error(
          `[CRM] âŒ Both template and fallback failed for ${assignee.name} (${sanitizedAssigneePhone}). Original error: ${templateError.message}`
        );
      }
    }

    // Check if result actually contains a message ID (real success)
    if (result) {
      const messageId = result.messages?.[0]?.id;
      if (messageId) {
        // Store message ID -> lead ID mapping for failure tracking
        messageIdToLeadCache.set(messageId, {
          leadId: lead.id,
          staffName: assignee.name,
          staffPhone: sanitizedAssigneePhone,
          timestamp: Date.now(),
        });

        await logLeadActivity(
          lead.id,
          "Summary Sent to Staff",
          `Assignment summary sent to staff "${assignee.name}" (${sanitizedAssigneePhone}) via WhatsApp.`,
          "System"
        );
        console.log(
          `[CRM] âœ… Successfully sent assignment notification to ${assignee.name} for lead ${lead.id}. WhatsApp Message ID: ${messageId}`
        );
      } else {
        // Result exists but no message ID - might be a false positive
        const errorMsg = `WhatsApp API returned result but no message ID for staff "${
          assignee.name
        }" (${sanitizedAssigneePhone}). Response: ${JSON.stringify(result)}`;
        await logLeadActivity(lead.id, "WhatsApp Failed", errorMsg, "System");
        console.error(`[CRM] âŒ ${errorMsg}`);
      }
    } else {
      const errorMsg = `Failed to send assignment notification to staff "${assignee.name}" (${sanitizedAssigneePhone}). Template may not be approved, phone number invalid, or WhatsApp API error. Check server logs for details.`;
      await logLeadActivity(lead.id, "WhatsApp Failed", errorMsg, "System");
      console.error(`[CRM] âŒ ${errorMsg}`);
    }
  } else {
    console.log(
      `[CRM] âš ï¸ Staff alert not sent for lead ${lead.id}: Invalid phone number for staff ${assignee.name}.`
    );
    await logLeadActivity(
      lead.id,
      "WhatsApp Skipped",
      `Assignment notification not sent to staff "${assignee.name}" (invalid phone number).`
    );
  }
}

// NOTE: TBO Flight API logic and endpoints removed from this codebase.
// The file preserves other integrations (website forms, WhatsApp webhooks, Razorpay,
// emailing suppliers, and AI itinerary generation). If you need to re-enable the
// flight provider proxy later, reintroduce a dedicated service module with secure
// credential management.

// --- SETTINGS API (for AI Toggle) ---
app.get("/api/settings/:key", requireAuth, async (req, res) => {
  const { key } = req.params;
  const currentUser = req.user;

  // Only Super Admin can view settings
  if (currentUser.role !== "Super Admin") {
    return res
      .status(403)
      .json({ message: "Forbidden: Super Admin access required." });
  }

  try {
    const { data, error } = await supabase
      .from("settings")
      .select("value")
      .eq("key", key)
      .single();
    if (error && error.code !== "PGRST116") throw error; // Ignore "not found"
    // Parse JSON value if it's a string, otherwise return as-is
    const value = data?.value;
    if (typeof value === "string") {
      try {
        const parsed = JSON.parse(value);
        res.json(parsed);
      } catch {
        res.json(value);
      }
    } else {
      res.json(value ?? false);
    }
  } catch (error) {
    console.error(`[Settings] Error fetching setting ${key}:`, error);
    res.status(500).json({ message: error.message });
  }
});

app.post("/api/settings/:key", requireAuth, async (req, res) => {
  const currentUser = req.user;

  // Only Super Admin can update settings
  if (currentUser.role !== "Super Admin") {
    return res
      .status(403)
      .json({ message: "Forbidden: Super Admin access required." });
  }

  const { key } = req.params;
  const { value } = req.body;
  try {
    const { data, error } = await supabase
      .from("settings")
      .upsert({ key, value: JSON.stringify(value) }, { onConflict: "key" })
      .select();
    if (error) throw error;
    res.json(data);
  } catch (error) {
    res.status(500).json({ message: error.message });
  }
});

// Lead bulk-delete: done from CRM via Supabase client (cascade + block paid invoices in Leads.tsx)

app.post("/api/customers/bulk-delete", requireAuth, async (req, res) => {
  try {
    const { customerIds } = req.body || {};
    if (!Array.isArray(customerIds) || customerIds.length === 0) {
      return res.status(400).json({
        message: "customerIds array is required and must not be empty.",
      });
    }
    const ids = customerIds.filter((id) => id != null).map((id) => String(id));
    if (ids.length === 0) {
      return res
        .status(400)
        .json({ message: "Valid customer IDs are required." });
    }

    // Block if any customer has leads
    const { data: leadsForCustomers } = await supabase
      .from("leads")
      .select("id, customer_id")
      .in("customer_id", ids);
    if (leadsForCustomers && leadsForCustomers.length > 0) {
      return res.status(400).json({
        message:
          "One or more customers have leads. Delete those leads first, then delete the customers.",
      });
    }

    // Remove whatsapp_messages that reference these customers (FK: whatsapp_messages_customer_id_fkey)
    await supabase.from("whatsapp_messages").delete().in("customer_id", ids);

    const { error } = await supabase.from("customers").delete().in("id", ids);
    if (error) throw error;

    return res.json({
      deleted: ids.length,
      message: `${ids.length} customer(s) deleted successfully.`,
    });
  } catch (err) {
    console.error("[bulk-delete customers]", err);
    return res
      .status(500)
      .json({ message: err?.message || "Failed to delete customers." });
  }
});

// Travel endpoints (airports, hotels, flights) removed for academy/lead-management.

app.get("/api/initiate-call", async (req, res) => {
  const { leadId, staffId, phone } = req.query;

  if (!leadId || !staffId || !phone) {
    return res
      .status(400)
      .send("Missing leadId, staffId, or phone query parameter.");
  }

  try {
    // Fetch staff name
    const { data: staff, error: staffError } = await supabase
      .from("staff")
      .select("name")
      .eq("id", staffId)
      .single();

    if (staffError || !staff) {
      throw new Error(
        staffError?.message || `Staff with ID ${staffId} not found.`
      );
    }

    // Fetch lead's current activity
    const { data: lead, error: leadError } = await supabase
      .from("leads")
      .select("activity")
      .eq("id", leadId)
      .single();

    if (leadError || !lead) {
      throw new Error(
        leadError?.message || `Lead with ID ${leadId} not found.`
      );
    }

    // Create and add new activity log
    const newActivity = {
      id: Date.now(),
      type: "Call Logged",
      description: `${staff.name} has initiated the call.`,
      user: staff.name,
      timestamp: new Date().toISOString(),
    };
    const updatedActivity = [newActivity, ...(lead.activity || [])];

    // Update the lead
    const { error: updateError } = await supabase
      .from("leads")
      .update({
        activity: updatedActivity,
        last_updated: new Date().toISOString(),
      })
      .eq("id", leadId);

    if (updateError) {
      throw new Error(`Failed to log activity: ${updateError.message}`);
    }

    console.log(
      `[CRM] Logged call initiation for lead ${leadId} by ${staff.name}.`
    );

    // Redirect to tel: link
    const sanitizedPhone = phone.replace(/[^0-9+]/g, ""); // Keep + and numbers
    res.redirect(`tel:${sanitizedPhone}`);
  } catch (error) {
    console.error("Error in /api/initiate-call:", error.message);
    res.status(500).send(`An error occurred: ${error.message}`);
  }
});

// --- PUBLIC STAFF LIST FOR WEBSITE (Branch 1) ---
// Returns minimal staff info for populating the staff dropdown in the website form.
app.get("/api/staff/branch/1", async (req, res) => {
  try {
    const { data, error } = await supabase
      .from("staff")
      .select("id, name, phone, branch_id, status, role_id")
      .eq("branch_id", 1)
      .eq("status", "Active")
      .neq("role_id", 1) // Exclude Super Admins
      .neq("name", "AI Assistant") // Exclude AI / bot user
      .order("name", { ascending: true });

    if (error) throw error;

    res.json(
      (data || []).map((s) => ({
        id: s.id,
        name: s.name,
        phone: s.phone,
      }))
    );
  } catch (err) {
    console.error("Error fetching Branch 1 staff list for website form:", err);
    res.status(500).json({ message: "Failed to load staff list." });
  }
});

// New endpoint to log customer-initiated calls
app.get("/api/log-customer-call", async (req, res) => {
  const { leadId, staffId, customerId } = req.query;
  if (!leadId || !staffId || !customerId) {
    return res.status(400).send("Missing required query parameters.");
  }

  try {
    const { data: staff, error: staffError } = await supabase
      .from("staff")
      .select("name, phone")
      .eq("id", staffId)
      .single();
    const { data: customer, error: customerError } = await supabase
      .from("customers")
      .select("first_name, last_name")
      .eq("id", customerId)
      .single();
    const { data: lead, error: leadError } = await supabase
      .from("leads")
      .select("activity")
      .eq("id", leadId)
      .single();

    if (
      staffError ||
      customerError ||
      leadError ||
      !staff ||
      !customer ||
      !lead
    ) {
      throw new Error("Could not find required information to log the call.");
    }

    const customerName = `${customer.first_name} ${customer.last_name}`;
    const newActivity = {
      id: Date.now(),
      type: "Call Initiated",
      description: `${customerName} initiated a call to ${staff.name}.`,
      user: "System",
      timestamp: new Date().toISOString(),
    };

    const { error: updateError } = await supabase
      .from("leads")
      .update({
        activity: [newActivity, ...(lead.activity || [])],
        last_updated: new Date().toISOString(),
      })
      .eq("id", leadId);

    if (updateError) {
      throw new Error(`Failed to log activity: ${updateError.message}`);
    }

    console.log(
      `[CRM] Logged customer call initiation for lead ${leadId} to ${staff.name}.`
    );

    const sanitizedStaffPhone = (staff.phone || "").replace(/[^0-9+]/g, "");
    if (!sanitizedStaffPhone) {
      throw new Error("Staff member does not have a phone number configured.");
    }

    res.redirect(`tel:${sanitizedStaffPhone}`);
  } catch (error) {
    console.error("Error in /api/log-customer-call:", error.message);
    res
      .status(500)
      .send(`Could not process your call request: ${error.message}`);
  }
});

// --- DAILY PRODUCTIVITY SUMMARY ---
// Sends daily summary at 8 PM to each branch admin
async function sendDailyProductivitySummary() {
  try {
    console.log(
      "[DailySummary] Starting daily productivity summary generation..."
    );

    // Get today's date range in IST (Indian Standard Time - UTC+5:30)
    const getISTDate = () => {
      const now = new Date();
      const istOffset = 5.5 * 60 * 60 * 1000; // 5 hours 30 minutes
      const utcTime = now.getTime() + now.getTimezoneOffset() * 60 * 1000;
      return new Date(utcTime + istOffset);
    };

    const istToday = getISTDate();
    istToday.setHours(0, 0, 0, 0);

    // Convert IST date back to UTC for database query
    const istOffset = 5.5 * 60 * 60 * 1000;
    const todayStart = new Date(istToday.getTime() - istOffset).toISOString();

    const istTomorrow = new Date(istToday);
    istTomorrow.setDate(istTomorrow.getDate() + 1);
    const todayEnd = new Date(istTomorrow.getTime() - istOffset).toISOString();

    // Get all active branches
    const { data: branches, error: branchesError } = await supabase
      .from("branches")
      .select("id, name, primary_contact")
      .eq("status", "Active");

    if (branchesError) {
      throw new Error(`Failed to fetch branches: ${branchesError.message}`);
    }

    if (!branches || branches.length === 0) {
      console.log("[DailySummary] No active branches found.");
      return;
    }

    // Process each branch
    for (const branch of branches) {
      try {
        // Use branch primary contact directly
        if (!branch.primary_contact) {
          console.log(
            `[DailySummary] Branch ${branch.name} (ID: ${branch.id}) has no primary contact. Skipping.`
          );
          continue;
        }

        // Get leads for this branch created today
        // OPTIMIZATION: Use index-friendly query with limit to reduce Disk IO
        // Note: branch_ids is a JSON array, so we check if it contains the branch.id
        const { data: todayLeads, error: leadsError } = await supabase
          .from("leads")
          .select("id, status, created_at, branch_ids")
          .gte("created_at", todayStart)
          .lt("created_at", todayEnd)
          .limit(10000); // Add limit to prevent excessive data fetch

        // Filter leads that belong to this branch (branch_ids is a JSON array)
        const branchLeads =
          todayLeads?.filter((lead) => {
            if (!lead.branch_ids) return false;
            // Handle both array format and JSON string format
            const branchIds = Array.isArray(lead.branch_ids)
              ? lead.branch_ids
              : typeof lead.branch_ids === "string"
              ? JSON.parse(lead.branch_ids)
              : [];
            return branchIds.includes(branch.id);
          }) || [];

        if (leadsError) {
          console.error(
            `[DailySummary] Error fetching leads for branch ${branch.name}:`,
            leadsError.message
          );
          continue;
        }

        // Calculate metrics (using filtered branchLeads instead of todayLeads)
        const totalLeads = branchLeads.length;
        const confirmedLeads = branchLeads.filter(
          (l) => l.status === "Confirmed"
        ).length;
        const rejectedLeads = branchLeads.filter(
          (l) => l.status === "Rejected"
        ).length;
        const paidLeads = branchLeads.filter(
          (l) => l.status === "Billing Completed"
        ).length;

        // Calculate conversion rate
        const conversionRate =
          totalLeads > 0
            ? ((confirmedLeads / totalLeads) * 100).toFixed(1)
            : "0.0";

        // Format date for display in IST
        const getISTDate = () => {
          const now = new Date();
          const istOffset = 5.5 * 60 * 60 * 1000;
          const utcTime = now.getTime() + now.getTimezoneOffset() * 60 * 1000;
          return new Date(utcTime + istOffset);
        };
        const istToday = getISTDate();
        istToday.setHours(0, 0, 0, 0);
        const dateStr = istToday.toLocaleDateString("en-GB", {
          weekday: "long",
          year: "numeric",
          month: "long",
          day: "numeric",
          timeZone: "Asia/Kolkata",
        });

        // Build summary message
        const summaryMessage = `ðŸ“Š *Daily Productivity Summary*\n*${branch.name}*\n\nðŸ“… *Date:* ${dateStr}\n\nðŸ“ˆ *Today's Performance:*\n\nðŸ†• *New Leads:* ${totalLeads}\nâœ… *Confirmed:* ${confirmedLeads}\nðŸ’³ *Billing Completed:* ${paidLeads}\nâŒ *Rejected/Lost:* ${rejectedLeads}\n\nðŸ“Š *Conversion Rate:* ${conversionRate}%\n\nKeep up the great work! ðŸ’ª`;

        // Normalize branch primary contact phone
        let sanitizedPhone = normalizePhone(branch.primary_contact, "IN");
        if (!sanitizedPhone && branch.primary_contact) {
          const phoneStr = String(branch.primary_contact)
            .trim()
            .replace(/[\s\-\(\)]/g, "");
          if (phoneStr.startsWith("+91") || phoneStr.startsWith("919")) {
            sanitizedPhone = phoneStr.startsWith("+")
              ? phoneStr
              : `+${phoneStr}`;
          } else if (phoneStr.length === 10) {
            sanitizedPhone = `+91${phoneStr}`;
          }
        }

        if (!sanitizedPhone) {
          console.log(
            `[DailySummary] Invalid phone for branch ${branch.name} (primary_contact: ${branch.primary_contact}). Skipping.`
          );
          continue;
        }

        // Try sending via template first
        let result = null;
        try {
          const templatePayload = {
            messaging_product: "whatsapp",
            to: sanitizedPhone,
            type: "template",
            template: {
              name: "daily_productivity_summary",
              language: { code: "en" },
              components: [
                {
                  type: "body",
                  parameters: [
                    { type: "text", text: branch.name },
                    { type: "text", text: dateStr },
                    { type: "text", text: totalLeads.toString() },
                    { type: "text", text: confirmedLeads.toString() },
                    { type: "text", text: partialPaymentLeads.toString() },
                    { type: "text", text: paidLeads.toString() },
                    { type: "text", text: rejectedLeads.toString() },
                    { type: "text", text: `${conversionRate}%` },
                  ],
                },
              ],
            },
          };

          console.log(
            `[DailySummary] ðŸ“¤ Sending template to ${branch.name} (${sanitizedPhone})`
          );
          const response = await fetch(WHATSAPP_GRAPH_API_BASE, {
            method: "POST",
            headers: {
              Authorization: `Bearer ${WHATSAPP_TOKEN}`,
              "Content-Type": "application/json",
            },
            body: JSON.stringify(templatePayload),
          });

          const apiResult = await response.json();
          if (response.ok && apiResult.messages) {
            result = apiResult;
            console.log(
              `[DailySummary] âœ… Template sent successfully to ${branch.name}`
            );
          } else {
            const errorDetails = apiResult.error || apiResult;
            // Check for token expiration (error code 190)
            if (
              errorDetails.code === 190 ||
              errorDetails.type === "OAuthException"
            ) {
              console.error(
                `[DailySummary] ðŸ”´ TOKEN EXPIRED: WhatsApp token has expired!`,
                errorDetails.message || ""
              );
              console.error(
                `[DailySummary] âš ï¸ Action required: Generate a new token and update WHATSAPP_TOKEN environment variable`
              );
            }
            console.warn(
              `[DailySummary] âš ï¸ Template failed. Using fallback.`
            );
            throw new Error(`Template failed: ${JSON.stringify(apiResult)}`);
          }
        } catch (templateError) {
          // Fallback to plain text
          console.log(
            `[DailySummary] Using plain text fallback for ${branch.name}`
          );
          result = await sendCrmWhatsappText(sanitizedPhone, summaryMessage);
        }

        if (result) {
          console.log(
            `[DailySummary] âœ… Summary sent to ${branch.name} (${sanitizedPhone})`
          );
        } else {
          console.error(
            `[DailySummary] âŒ Failed to send summary to ${branch.name} (${sanitizedPhone})`
          );
        }

        // Small delay between branches
        await new Promise((resolve) => setTimeout(resolve, 1000));
      } catch (branchError) {
        console.error(
          `[DailySummary] Error processing branch ${branch.name}:`,
          branchError.message
        );
        continue;
      }
    }

    console.log("[DailySummary] Daily productivity summary completed.");
  } catch (error) {
    console.error(
      "[DailySummary] Error in daily productivity summary:",
      error.message
    );
  }
}

// Schedule daily summary at 8 PM IST (Indian Standard Time - UTC+5:30)
function scheduleDailySummary() {
  const getISTTime = () => {
    const now = new Date();
    // IST is UTC+5:30
    const istOffset = 5.5 * 60 * 60 * 1000; // 5 hours 30 minutes in milliseconds
    const utcTime = now.getTime() + now.getTimezoneOffset() * 60 * 1000;
    const istTime = new Date(utcTime + istOffset);
    return istTime;
  };

  const getNext8PMIST = () => {
    const istNow = getISTTime();
    const targetTime = new Date(istNow);
    targetTime.setHours(20, 0, 0, 0); // 8 PM IST

    // If it's already past 8 PM IST today, schedule for tomorrow
    if (istNow >= targetTime) {
      targetTime.setDate(targetTime.getDate() + 1);
    }

    // Convert IST time back to UTC for scheduling
    const istOffset = 5.5 * 60 * 60 * 1000;
    const utcTargetTime = new Date(targetTime.getTime() - istOffset);
    return utcTargetTime;
  };

  const now = new Date();
  const targetTime = getNext8PMIST();
  const msUntilTarget = targetTime.getTime() - now.getTime();

  const istTarget = getISTTime();
  istTarget.setHours(20, 0, 0, 0);
  if (getISTTime() >= istTarget) {
    istTarget.setDate(istTarget.getDate() + 1);
  }

  console.log(
    `[DailySummary] Scheduled for 8 PM IST (${istTarget.toLocaleString(
      "en-IN",
      { timeZone: "Asia/Kolkata" }
    )}). Will run in ${Math.round(msUntilTarget / 1000 / 60)} minutes.`
  );

  setTimeout(() => {
    sendDailyProductivitySummary();
    // Schedule for next day (24 hours later)
    setInterval(sendDailyProductivitySummary, 24 * 60 * 60 * 1000);
  }, msUntilTarget);
}

/**
 * Schedule TBO Static Data Refresh
 * Runs on 1st, 15th, and last day of each month (approximately every 15 days)
 * Time: 2 AM IST (as per TBO recommendations)
 */
function scheduleTboStaticDataRefresh() {
  const REFRESH_HOUR = 2; // 2 AM IST
  const REFRESH_MINUTE = 0;
  const IST_OFFSET_HOURS = 5.5; // IST is UTC+5:30

  function getISTTime() {
    const now = new Date();
    const utcTime = now.getTime() + now.getTimezoneOffset() * 60 * 1000;
    return new Date(utcTime + IST_OFFSET_HOURS * 60 * 60 * 1000);
  }

  function getLastDayOfMonth(year, month) {
    return new Date(year, month + 1, 0).getDate();
  }

  function getNextRefreshDate() {
    const istNow = getISTTime();
    const currentDay = istNow.getDate();
    const currentMonth = istNow.getMonth();
    const currentYear = istNow.getFullYear();
    const lastDay = getLastDayOfMonth(currentYear, currentMonth);

    // Determine next refresh day: 1st, 15th, or last day
    let nextDay;
    if (currentDay < 1) {
      nextDay = 1;
    } else if (currentDay < 15) {
      nextDay = 15;
    } else if (currentDay < lastDay) {
      nextDay = lastDay;
    } else {
      // Move to next month's 1st
      const nextMonth = new Date(currentYear, currentMonth + 1, 1);
      return nextMonth;
    }

    const targetDate = new Date(currentYear, currentMonth, nextDay);
    targetDate.setHours(REFRESH_HOUR, REFRESH_MINUTE, 0, 0);

    // If target time has passed today, move to next refresh date
    if (targetDate <= istNow) {
      if (nextDay === 1) {
        targetDate.setDate(15);
      } else if (nextDay === 15) {
        const lastDayOfMonth = getLastDayOfMonth(currentYear, currentMonth);
        targetDate.setDate(lastDayOfMonth);
      } else {
        // Move to next month's 1st
        targetDate.setMonth(currentMonth + 1);
        targetDate.setDate(1);
      }
    }

    // Convert IST back to UTC
    const utcTarget = new Date(
      targetDate.getTime() - IST_OFFSET_HOURS * 60 * 60 * 1000
    );
    return utcTarget;
  }

  async function runTboRefresh() {
    const startTime = Date.now();
    console.log(
      `\n[${new Date().toISOString()}] ðŸš€ Starting scheduled TBO static data refresh...\n`
    );

    try {
      // Step 1: Refresh countries
      console.log("[TBO Refresh] Step 1/3: Refreshing countries...");
      const countries = await fetchTboCountryList();
      await storeTboCountries(countries);
      console.log(`[TBO Refresh] âœ… Refreshed ${countries.length} countries`);

      // Step 2: Refresh cities
      console.log("[TBO Refresh] Step 2/3: Refreshing cities...");
      const allCities = [];
      let citiesProcessed = 0;

      for (const country of countries) {
        try {
          const cities = await fetchTboCityList(country.code);
          await storeTboCities(cities, country.code);
          allCities.push(
            ...cities.map((c) => ({ ...c, countryCode: country.code }))
          );
          citiesProcessed += cities.length;
          await new Promise((resolve) => setTimeout(resolve, 300)); // Rate limiting
        } catch (error) {
          console.error(
            `[TBO Refresh] âš ï¸  Skipping cities for ${country.code}: ${error.message}`
          );
          continue;
        }
      }
      console.log(`[TBO Refresh] âœ… Refreshed ${citiesProcessed} cities`);

      // Step 3: Refresh hotels
      console.log("[TBO Refresh] Step 3/3: Refreshing hotels...");
      let totalHotels = 0;

      for (const city of allCities) {
        try {
          const hotels = await fetchTboHotelCodeList(city.code);
          await storeTboHotelCodes(
            hotels,
            city.code,
            city.name,
            city.countryCode
          );
          totalHotels += hotels.length;
          await new Promise((resolve) => setTimeout(resolve, 300)); // Rate limiting
        } catch (error) {
          console.error(
            `[TBO Refresh] âš ï¸  Skipping hotels for city ${city.code}: ${error.message}`
          );
          continue;
        }
      }

      const duration = ((Date.now() - startTime) / 1000 / 60).toFixed(2);
      console.log(`[TBO Refresh] âœ… Refresh completed!`);
      console.log(
        `[TBO Refresh]   Countries: ${countries.length}, Cities: ${citiesProcessed}, Hotels: ${totalHotels}`
      );
      console.log(`[TBO Refresh]   Duration: ${duration} minutes\n`);
    } catch (error) {
      console.error(`[TBO Refresh] âŒ Error during refresh:`, error.message);
      logger.error("[TBO Refresh] Scheduled refresh failed", {
        error: error.message,
        stack: error.stack,
      });
    }
  }

  function scheduleNextRun() {
    const nextRun = getNextRefreshDate();
    const msUntilNext = nextRun.getTime() - Date.now();

    const istNextRun = new Date(
      nextRun.getTime() + IST_OFFSET_HOURS * 60 * 60 * 1000
    );
    const daysUntilNext = (msUntilNext / (1000 * 60 * 60 * 24)).toFixed(1);

    console.log(
      `[TBO Refresh] Scheduled for ${istNextRun.toLocaleDateString("en-IN", {
        timeZone: "Asia/Kolkata",
        year: "numeric",
        month: "long",
        day: "numeric",
        hour: "2-digit",
        minute: "2-digit",
      })} IST (${daysUntilNext} days)`
    );

    setTimeout(async () => {
      await runTboRefresh();
      // Schedule next run after completion
      scheduleNextRun();
    }, Math.max(0, msUntilNext));
  }

  // Start scheduling
  scheduleNextRun();
}

// Start the daily summary scheduler
scheduleDailySummary();

// --- AUTOMATIC FEEDBACK LINK SENDING ---
// Sends Google review link when lead status changes to "Feedback"
async function sendFeedbackLinkMessage(lead, customer) {
  try {
    // Check if feedback message was already sent (prevent duplicates)
    const feedbackSent = (lead.activity || []).some(
      (act) =>
        act.type === "Feedback Request Sent" &&
        act.description?.includes("Feedback request sent to customer")
    );

    if (feedbackSent) {
      console.log(
        `[Feedback] Feedback link already sent for lead ${lead.id}. Skipping duplicate.`
      );
      return;
    }

    // Normalize customer phone number
    let sanitizedPhone = normalizePhone(customer.phone, "IN");

    // Fallback phone normalization
    if (!sanitizedPhone && customer.phone) {
      const phoneStr = String(customer.phone).trim();
      const cleaned = phoneStr.replace(/[\s\-\(\)]/g, "");
      if (cleaned.startsWith("+91") || cleaned.startsWith("919")) {
        sanitizedPhone = cleaned.startsWith("+") ? cleaned : `+${cleaned}`;
      } else if (cleaned.length === 10) {
        sanitizedPhone = `+91${cleaned}`;
      }
    }

    if (!sanitizedPhone) {
      console.warn(
        `[Feedback] Could not normalize customer phone for lead ${lead.id}: ${customer.phone}. Skipping feedback message.`
      );
      await logLeadActivity(
        lead.id,
        "Feedback Request Failed",
        `Failed to send feedback request to customer "${customer.first_name} ${customer.last_name}" (invalid phone number: '${customer.phone}').`,
        "System"
      );
      return;
    }

    // Use the approved "feedback_request" WhatsApp template
    let result = null;
    try {
      // Use first name only for the template (as shown in the template: "Hello {{1}}!")
      const customerFirstName = customer.first_name || "Customer";

      // Template structure:
      // Body: Hello {{1}}! ðŸ‘‹ ... (uses customer first name)
      // Button: "Rate Your Experience" - URL is STATIC (hardcoded in Meta Business Manager)
      // Footer: "Madura Travel Service" - static text
      const templatePayload = {
        messaging_product: "whatsapp",
        to: sanitizedPhone,
        type: "template",
        template: {
          name: "feedback_request",
          language: { code: "en" },
          components: [
            {
              type: "body",
              parameters: [{ type: "text", text: customerFirstName }],
            },
            // Note: Button URL is static/hardcoded in template, so no button component needed
          ],
        },
      };

      console.log(
        `[Feedback] ðŸ“¤ Sending feedback_request template to ${sanitizedPhone}`
      );
      const response = await fetch(WHATSAPP_GRAPH_API_BASE, {
        method: "POST",
        headers: {
          Authorization: `Bearer ${WHATSAPP_TOKEN}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify(templatePayload),
      });

      const apiResult = await response.json();
      if (response.ok && apiResult.messages) {
        result = apiResult;
        console.log(
          `[Feedback] âœ… Template message sent successfully for lead ${lead.id}`
        );
      } else {
        console.warn(
          `[Feedback] âš ï¸ Template message failed. Reason: ${JSON.stringify(
            apiResult
          )}`
        );
        throw new Error(`WhatsApp API error: ${JSON.stringify(apiResult)}`);
      }
    } catch (templateError) {
      console.warn(
        `[Feedback] âš ï¸ Template message failed for lead ${lead.id}. Trying plain text fallback:`,
        templateError.message
      );
      // Fallback: Send as plain text with link in message
      const feedbackLink =
        "https://search.google.com/local/writereview?placeid=ChIJnVd0XJ9nUjoRblhbY-Aip8k";
      const fallbackMessage = `Hello ${customer.first_name}! ðŸ‘‹\n\nThank you for choosing Madura Travel Service! We hope you had a wonderful experience with us. ðŸŒŸ\n\nWe would love to hear your feedback! Please take a moment to share your experience by clicking the link below:\n\nðŸ”— ${feedbackLink}\n\nYour feedback helps us serve you better! ðŸ™`;
      result = await sendCrmWhatsappText(sanitizedPhone, fallbackMessage);
    }

    if (result) {
      await logLeadActivity(
        lead.id,
        "Feedback Request Sent",
        `Feedback request with Google review link sent to customer "${customer.first_name} ${customer.last_name}" via WhatsApp.`,
        "System"
      );
      console.log(
        `[Feedback] âœ… Feedback link sent successfully to customer for lead ${lead.id}`
      );
    } else {
      await logLeadActivity(
        lead.id,
        "Feedback Request Failed",
        `Failed to send feedback request to customer "${customer.first_name} ${customer.last_name}" via WhatsApp.`,
        "System"
      );
      console.error(
        `[Feedback] âŒ Failed to send feedback link for lead ${lead.id}`
      );
    }
  } catch (error) {
    console.error(
      `[Feedback] Error sending feedback link for lead ${lead.id}:`,
      error.message
    );
    await logLeadActivity(
      lead.id,
      "Feedback Request Failed",
      `Error sending feedback request: ${error.message}`,
      "System"
    );
  }
}

// Function to create Razorpay payment link for itinerary (without sending invoice template)
async function createRazorpayLinkForItinerary(lead, customer) {
  try {
    // Check if invoice with payment link already exists for this lead
    const { data: existingInvoice } = await supabase
      .from("invoices")
      .select("id, razorpay_payment_link_url")
      .eq("lead_id", lead.id)
      .not("razorpay_payment_link_url", "is", null)
      .limit(1)
      .maybeSingle();

    if (existingInvoice?.razorpay_payment_link_url) {
      console.log(
        `[Razorpay Link] Payment link already exists for lead ${lead.id}. Skipping creation.`
      );
      return existingInvoice.razorpay_payment_link_url;
    }

    // Check if invoice exists but without payment link
    const { data: existingInvoiceWithoutLink } = await supabase
      .from("invoices")
      .select("id, invoice_number, balance_due, total_amount")
      .eq("lead_id", lead.id)
      .limit(1)
      .maybeSingle();

    let invoiceId = null;
    let amount = 5000; // Default booking fees

    if (existingInvoiceWithoutLink) {
      invoiceId = existingInvoiceWithoutLink.id;
      amount =
        existingInvoiceWithoutLink.balance_due ||
        existingInvoiceWithoutLink.total_amount ||
        5000;
      console.log(
        `[Razorpay Link] Found existing invoice #${existingInvoiceWithoutLink.invoice_number} for lead ${lead.id}. Creating payment link.`
      );
    } else {
      // Create a minimal invoice for payment link
      const bookingFees = 5000;
      const today = new Date();
      const dueDate = new Date();
      dueDate.setDate(today.getDate() + 7);

      const invoiceNumber = `INV-${Date.now().toString().slice(-6)}`;
      const bookingId = `JA-${lead.id}`;

      const newInvoice = {
        invoice_number: invoiceNumber,
        lead_id: lead.id,
        customer_id: customer.id,
        issue_date: today.toISOString().split("T")[0],
        due_date: dueDate.toISOString().split("T")[0],
        status: "DRAFT",
        items: [
          {
            id: Date.now(),
            description: `Booking Confirmation & Advance for ${
              lead.destination || "Tour Package"
            }`,
            qty: 1,
            rate: bookingFees,
            amount: bookingFees,
          },
        ],
        total_amount: bookingFees,
        balance_due: bookingFees,
        created_at: new Date().toISOString(),
      };

      const { data: createdInvoice, error: createError } = await supabase
        .from("invoices")
        .insert(newInvoice)
        .select()
        .single();

      if (createError || !createdInvoice) {
        throw new Error(createError?.message || "Failed to create invoice");
      }

      invoiceId = createdInvoice.id;
      amount = bookingFees;
      console.log(
        `[Razorpay Link] Created invoice #${invoiceNumber} for lead ${lead.id}`
      );
    }

    // Generate Razorpay payment link
    if (!RAZORPAY_KEY_ID || !RAZORPAY_KEY_SECRET) {
      console.warn(
        `[Razorpay Link] Razorpay credentials not configured. Cannot generate payment link for lead ${lead.id}.`
      );
      return null;
    }

    const auth = Buffer.from(
      `${RAZORPAY_KEY_ID}:${RAZORPAY_KEY_SECRET}`
    ).toString("base64");

    const phoneDigits = customer.phone.replace(/[^0-9]/g, "");
    const contactPhone = phoneDigits.slice(-10);

    const razorpayResponse = await fetch(`${RAZORPAY_API_URL}/payment_links`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Basic ${auth}`,
      },
      body: JSON.stringify({
        amount: amount * 100, // Razorpay expects amount in paise
        currency: "INR",
        description: `Booking Payment - ${customer.first_name} ${
          customer.last_name
        } - ${lead.destination || "Tour Package"} - JA-${lead.id}`,
        customer: {
          name: `${customer.first_name} ${customer.last_name}`,
          email: customer.email || "",
          contact: contactPhone,
        },
        notify: { sms: false, email: false }, // Don't notify - link is only in PDF
        reminder_enable: false, // No reminders
        callback_url: "https://crm.jeppiaaracademy.com/payments",
        callback_method: "get",
      }),
    });

    const razorpayData = await razorpayResponse.json();
    if (!razorpayResponse.ok) {
      console.error(
        `[Razorpay Link] Razorpay error for lead ${lead.id}:`,
        JSON.stringify(razorpayData, null, 2)
      );
      throw new Error(
        razorpayData.error?.description ||
          "Failed to create Razorpay payment link"
      );
    }

    // Update invoice with payment link
    const { error: updateError } = await supabase
      .from("invoices")
      .update({
        razorpay_payment_link_id: razorpayData.id,
        razorpay_payment_link_url: razorpayData.short_url,
        status: "SENT",
      })
      .eq("id", invoiceId);

    if (updateError) {
      console.error(
        `[Razorpay Link] Failed to update invoice with payment link:`,
        updateError.message
      );
    }

    console.log(
      `[Razorpay Link] Generated Razorpay payment link for lead ${lead.id}: ${razorpayData.short_url}`
    );

    await logLeadActivity(
      lead.id,
      "Payment Link Created",
      `Razorpay payment link created for itinerary. Payment link will be included in PDF.`,
      "System"
    );

    return razorpayData.short_url;
  } catch (error) {
    console.error(
      `[Razorpay Link] Error creating payment link for lead ${lead.id}:`,
      error.message
    );
    // Don't throw - just log the error so itinerary generation can continue
    return null;
  }
}

// REMOVED: Automatic invoice creation functionality
// Invoices should be created manually by staff through the CRM interface

// --- PERIODIC LEAD UPDATE NOTIFIER ---
// DISABLED: Customer message sending on lead changes
// Checks for leads with recent status/services/travel_date changes but does NOT send WhatsApp notifications to customers
// Only handles backend actions like invoice creation (without sending messages)
const checkLeadUpdatesAndNotify = async () => {
  console.log(
    "[UpdateNotifier] Checking for lead changes (customer messages disabled)..."
  );
  try {
    // Get leads that were modified in the last 5 minutes (buffer for consistency)
    // OPTIMIZATION: Increased window since we check every 5 minutes now
    const fiveMinutesAgo = new Date(Date.now() - 5 * 60 * 1000).toISOString();
    // OPTIMIZATION: Only fetch necessary fields to reduce egress
    const { data: recentLeads, error: leadsError } = await supabase
      .from("leads")
      .select(
        "id, status, activity, customer_id, customer:customers(id, first_name, last_name, email, phone), all_assignees:lead_assignees(staff(id, name))"
      )
      .gt("last_updated", fiveMinutesAgo);

    if (leadsError) throw leadsError;
    if (!recentLeads || recentLeads.length === 0) {
      console.log("[UpdateNotifier] No recent lead changes detected.");
      return;
    }

    console.log(
      `[UpdateNotifier] Found ${recentLeads.length} leads updated in last 90 seconds. Customer messages disabled.`
    );

    // For each lead, handle backend actions only (invoice creation, etc.) - NO customer messages
    for (const lead of recentLeads) {
      try {
        const customer = lead.customer;
        if (!customer) continue;

        // WhatsApp messages are only triggered for Feedback status
        // All other status changes do not trigger WhatsApp messages

        // Handle backend actions only - NO customer messages

        // Mark other significant statuses as notified (but don't send messages)
        // Note: significantStatuses removed - this block is now handled by specific status checks below

        // Handle Feedback status - send feedback template (duplicate prevention via activity in sendFeedbackLinkMessage)
        if (lead.status === "Feedback") {
          console.log(
            `[UpdateNotifier] Lead ${lead.id} status is Feedback. Sending feedback template if not already sent...`
          );
          try {
            await sendFeedbackLinkMessage(lead, customer);
            console.log(
              `[UpdateNotifier] Feedback check complete for lead ${lead.id}`
            );
          } catch (feedbackError) {
            console.error(
              `[UpdateNotifier] Error sending feedback template for lead ${lead.id}:`,
              feedbackError.message,
              feedbackError.stack
            );
          }
        }
      } catch (err) {
        console.error(
          `[UpdateNotifier] Error processing lead ${lead.id}:`,
          err.message
        );
      }
    }

    console.log(
      "[UpdateNotifier] Lead update check complete (no customer messages sent)."
    );
  } catch (error) {
    const errorMessage =
      error?.message ||
      error?.toString() ||
      JSON.stringify(error) ||
      "Unknown error";
    console.error("[UpdateNotifier] Error during check:", errorMessage);
  }
};

// OPTIMIZATION: Run every 5 minutes instead of 60 seconds to reduce egress by 83%
// For 9 users, checking every minute is excessive
setInterval(checkLeadUpdatesAndNotify, 5 * 60 * 1000);

// --- WEBSITE LEAD ENDPOINT ---

app.post("/api/lead/website", async (req, res) => {
  try {
    // Handle Elementor's potential 'form_fields' nesting or a flat payload
    const formData = req.body.form_fields || req.body;
    console.log(
      "Received website lead data:",
      JSON.stringify(formData, null, 2)
    );

    // Academy form: extract all supported fields (Elementor may send 'name' or 'Name', etc.)
    const name =
      formData.name ||
      formData.Name ||
      [formData.first_name, formData.last_name].filter(Boolean).join(" ");
    const phone = formData.phone || formData.Phone;
    const enquiry =
      formData.enquiry ||
      formData["Type of Enquiry?"] ||
      formData.type_of_enquiry;
    const nationality = formData.nationality || formData.Nationality;
    const email = formData.email || formData.Email;

    // Student/Customer optional fields
    const first_name =
      formData.first_name ||
      formData["First Name"] ||
      (name ? name.split(" ")[0] : null);
    const last_name =
      formData.last_name ||
      formData["Last Name"] ||
      (name && name.split(" ").length > 1
        ? name.split(" ").slice(1).join(" ")
        : null);
    const gender = formData.gender || formData.Gender || null;
    const date_of_birth =
      formData.date_of_birth ||
      formData["Date of Birth"] ||
      formData.dob ||
      null;
    const aadhaar_number =
      formData.aadhaar_number ||
      formData["Aadhaar Number"] ||
      formData.aadhaar ||
      null;
    const full_name_as_per_aadhaar =
      formData.full_name_as_per_aadhaar ||
      formData["Full Name as per Aadhaar"] ||
      null;
    const alternate_mobile =
      formData.alternate_mobile ||
      formData["Alternate Mobile Number"] ||
      formData.alternate_mobile_number ||
      null;
    const address_for_communication =
      formData.address_for_communication ||
      formData["Address for Communication"] ||
      formData.address ||
      formData.Address ||
      null;
    const city = formData.city || formData.City || null;
    const state = formData.state || formData.State || null;
    const pincode = formData.pincode || formData.Pincode || null;
    const avatar_url =
      formData.avatar_url || formData.avatar || formData.photo_url || null;

    // Alternate contact (flat or nested)
    let alternate_contact = formData.alternate_contact || null;
    if (
      !alternate_contact &&
      (formData.alternate_contact_name ||
        formData.contact_name ||
        formData["Contact Name"])
    ) {
      alternate_contact = {
        contact_name:
          formData.alternate_contact_name ||
          formData["Contact Name"] ||
          formData.contact_name ||
          "",
        relationship_with_student:
          formData.relationship_with_student ||
          formData["Relationship with Student"] ||
          formData.relationship ||
          "",
        contact_mobile:
          formData.alternate_contact_mobile ||
          formData["Contact Mobile Number"] ||
          formData.contact_mobile ||
          "",
        contact_email:
          formData.alternate_contact_email ||
          formData["Contact Email ID"] ||
          formData.contact_email ||
          "",
      };
      if (
        !alternate_contact.contact_name &&
        !alternate_contact.contact_mobile &&
        !alternate_contact.contact_email
      ) {
        alternate_contact = null;
      }
    }

    // Scholarship eligibility: array or comma-separated string
    let scholarship_eligibility =
      formData.scholarship_eligibility ||
      formData["Scholarship Eligibility"] ||
      null;
    if (scholarship_eligibility != null) {
      if (Array.isArray(scholarship_eligibility)) {
        scholarship_eligibility = scholarship_eligibility.filter(Boolean);
      } else {
        scholarship_eligibility = String(scholarship_eligibility)
          .split(",")
          .map((s) => s.trim())
          .filter(Boolean);
      }
      if (scholarship_eligibility.length === 0) scholarship_eligibility = null;
    }

    // Lead optional fields
    const lead_status = formData.status || formData.lead_status || "Enquiry";
    const source = formData.source || formData.lead_source || null;
    const where_did_you_hear =
      formData.where_did_you_hear ||
      formData["Where did you hear about this site?"] ||
      formData.hear_about_site ||
      null;
    const staff_id = formData.staff_id || formData.assigned_staff_id || null;
    const lead_type = formData.lead_type || formData["Lead Type"] || "Cold";
    const priority = formData.priority || formData.Priority || "Low";
    const note_text =
      formData.notes ||
      formData.note ||
      formData["Notes"] ||
      formData.remarks ||
      null;

    // Academy: required = (name or first_name+last_name), phone, enquiry
    const hasName = (name && name.trim()) || (first_name && last_name);
    if (!hasName || !phone || !enquiry) {
      console.error(
        "Validation failed: Missing name (or first_name+last_name), phone, or enquiry.",
        {
          name,
          first_name,
          last_name,
          phone,
          enquiry,
        }
      );
      return res.status(400).json({
        message:
          "Missing required fields: name (or first_name and last_name), phone, and enquiry (Type of Enquiry) are required. Use field IDs: name, phone, enquiry.",
      });
    }

    // Use branch_id from form data if provided, otherwise default to branch 1
    const targetBranchId = formData.branch_id
      ? parseInt(formData.branch_id, 10)
      : 1;
    console.log(
      `[Website Lead] Using branch_id: ${targetBranchId} (from formData.branch_id: ${formData.branch_id})`
    );

    // 1. Find or Create Customer
    let customer;

    // Normalize phone using the normalizePhone utility function
    let phoneNormalized = normalizePhone(phone, "IN");

    // Fallback: If normalizePhone fails, try manual normalization
    if (!phoneNormalized && phone) {
      const phoneStr = String(phone)
        .trim()
        .replace(/[\s\-\(\)]/g, "");

      // Handle phone numbers without + prefix (common from website forms)
      // If it starts with 91 (India) and is 12 digits, add +
      if (phoneStr.startsWith("91") && phoneStr.length === 12) {
        phoneNormalized = `+${phoneStr}`;
      }
      // If it's 10 digits (Indian number without country code), add +91
      else if (phoneStr.length === 10 && /^\d+$/.test(phoneStr)) {
        phoneNormalized = `+91${phoneStr}`;
      }
      // If it doesn't start with +, try to add it if it looks like a valid number
      else if (!phoneStr.startsWith("+") && /^\d+$/.test(phoneStr)) {
        // If it's 11-15 digits, assume it has country code and add +
        if (phoneStr.length >= 11 && phoneStr.length <= 15) {
          phoneNormalized = `+${phoneStr}`;
        }
      }
    }

    // Validate phone format (should start with + and have 7-15 digits after country code)
    if (!phoneNormalized || !phoneNormalized.match(/^\+\d{7,15}$/)) {
      console.error(
        "Invalid phone format:",
        phone,
        "normalized:",
        phoneNormalized
      );
      return res.status(400).json({
        message:
          "Invalid phone number format. Please use format: +919876543210",
      });
    }

    const { data: existingCustomer, error: findError } = await supabase
      .from("customers")
      .select("*")
      .or(
        `phone.eq.${phoneNormalized},phone.eq.${phoneNormalized.replace(
          /^\+/,
          ""
        )}`
      )
      .limit(1)
      .maybeSingle();

    if (findError) throw findError;

    if (existingCustomer) {
      customer = existingCustomer;

      // Sync customer's shared_with_branch_ids with target branch
      // Add target branch to shared_with_branch_ids if it's different from the customer's owner branch
      const updateFields = {};
      const customerOwnerBranch = customer.added_by_branch_id;

      // Only add to shared_with_branch_ids if target branch is different from owner branch
      if (targetBranchId !== customerOwnerBranch) {
        const currentSharedBranches = new Set(
          customer.shared_with_branch_ids || []
        );

        // Add target branch to shared branches if not already present
        if (!currentSharedBranches.has(targetBranchId)) {
          currentSharedBranches.add(targetBranchId);
          updateFields.shared_with_branch_ids = Array.from(
            currentSharedBranches
          );
          console.log(
            `[Website Lead] Adding branch ${targetBranchId} to customer ${customer.id} shared_with_branch_ids. Customer owner branch: ${customerOwnerBranch}`
          );
        }
      }

      // Merge all provided customer/student fields (form overwrites missing or all, depending on use case)
      if (nationality != null) updateFields.nationality = nationality;
      if (email != null) updateFields.email = email;
      if (gender != null) updateFields.gender = gender;
      if (date_of_birth != null) updateFields.date_of_birth = date_of_birth;
      if (aadhaar_number != null) updateFields.aadhaar_number = aadhaar_number;
      if (full_name_as_per_aadhaar != null)
        updateFields.full_name_as_per_aadhaar = full_name_as_per_aadhaar;
      if (alternate_mobile != null)
        updateFields.alternate_mobile = alternate_mobile;
      if (address_for_communication != null)
        updateFields.address_for_communication = address_for_communication;
      if (city != null) updateFields.city = city;
      if (state != null) updateFields.state = state;
      if (pincode != null) updateFields.pincode = pincode;
      if (avatar_url != null) updateFields.avatar_url = avatar_url;
      if (alternate_contact != null)
        updateFields.alternate_contact = alternate_contact;
      if (scholarship_eligibility != null)
        updateFields.scholarship_eligibility = scholarship_eligibility;
      if (first_name != null) updateFields.first_name = first_name;
      if (last_name != null) updateFields.last_name = last_name;

      // Update customer if any fields need updating
      if (Object.keys(updateFields).length > 0) {
        const { data: updatedCustomer, error: updateError } = await supabase
          .from("customers")
          .update(updateFields)
          .eq("id", customer.id)
          .select()
          .single();
        if (updateError)
          console.warn("Could not update customer:", updateError.message);
        else customer = updatedCustomer;
      }
    } else {
      const fName = first_name || (name ? name.split(" ")[0] : "Website");
      const lName =
        last_name ||
        (name && name.split(" ").length > 1
          ? name.split(" ").slice(1).join(" ")
          : "Customer");

      const customerInsert = {
        salutation: "Mr.",
        first_name: fName,
        last_name: lName,
        email: email || null,
        phone: phoneNormalized,
        nationality: nationality || null,
        username: `@${(fName + lName)
          .toLowerCase()
          .replace(/\s/g, "")}${Date.now().toString().slice(-4)}`,
        avatar_url:
          avatar_url ||
          `https://avatar.iran.liara.run/public/boy?username=${Date.now()}`,
        date_added: new Date().toISOString(),
        added_by_branch_id: targetBranchId,
        gender: gender || null,
        date_of_birth: date_of_birth || null,
        aadhaar_number: aadhaar_number || null,
        full_name_as_per_aadhaar: full_name_as_per_aadhaar || null,
        alternate_mobile: alternate_mobile || null,
        address_for_communication: address_for_communication || null,
        city: city || null,
        state: state || null,
        pincode: pincode || null,
        alternate_contact: alternate_contact || null,
        scholarship_eligibility: scholarship_eligibility || null,
      };

      const { data: newCustomer, error: createError } = await supabase
        .from("customers")
        .insert(customerInsert)
        .select()
        .single();

      if (createError) throw createError;
      customer = newCustomer;
    }

    // Academy: services from enquiry (single or comma-separated)
    let services = formData.services || [];
    if (!Array.isArray(services) || services.length === 0) {
      services = enquiry
        ? enquiry.includes(",")
          ? enquiry
              .split(",")
              .map((s) => s.trim())
              .filter(Boolean)
          : [enquiry]
        : [];
    }

    const summary =
      formData.summary ||
      `Lead from website form regarding ${enquiry || "an enquiry"}.`;

    // staff_id already extracted above

    // Fetch staff information if staff_id is provided (for activity log)
    let staffForActivity = null;
    if (staff_id) {
      const staffIdNum = parseInt(staff_id, 10);
      if (!isNaN(staffIdNum)) {
        try {
          const { data: staffData } = await supabase
            .from("staff")
            .select("id, name")
            .eq("id", staffIdNum)
            .single();
          if (staffData) {
            staffForActivity = staffData;
          }
        } catch (err) {
          console.warn(
            "[Website Lead] Could not fetch staff for activity log:",
            err.message
          );
        }
      }
    }

    // 2. Create Lead (academy-only: enquiry, services, summary, status, source, lead_type, priority; optional notes)
    const leadSourceValue = source || (staff_id ? "Staff Link" : "website");
    const activityDescription = staffForActivity
      ? `Lead created via website form (Staff Form) by ${staffForActivity.name} (Staff ID: ${staffForActivity.id}).`
      : "Lead created via website form.";

    const allNotes = [];
    if (note_text && String(note_text).trim()) {
      allNotes.push({
        id: Date.now(),
        text: String(note_text).trim(),
        date: new Date().toISOString(),
        addedBy: {
          id: 0,
          user_id: "system_website_form",
          name: "Website Form",
          email: "form@system.local",
          phone: "",
          status: "Active",
          role_id: 3,
          branch_id: targetBranchId,
          avatar_url: "",
          activity_log: [],
          destinations: "",
          leads_missed: 0,
          last_active_at: null,
          leads_attended: 0,
          on_leave_until: null,
          last_response_at: null,
          work_hours_today: 0,
          avg_response_time: null,
        },
        mentions: [],
      });
    }

    const newLead = {
      customer_id: customer.id,
      status: lead_status || "Enquiry",
      priority: priority || "Low",
      lead_type: lead_type || "Cold",
      requirements: {},
      services,
      summary,
      notes: allNotes,
      activity: [
        {
          id: Date.now(),
          type: "Lead Created",
          description: activityDescription,
          user: staffForActivity ? staffForActivity.name : "System",
          timestamp: new Date().toISOString(),
        },
      ],
      branch_ids: [targetBranchId],
      source: leadSourceValue,
      where_did_you_hear: where_did_you_hear || null,
      created_at: new Date().toISOString(),
      last_updated: new Date().toISOString(),
    };

    const { data: createdLead, error: leadError } = await supabase
      .from("leads")
      .insert(newLead)
      .select()
      .single();

    if (leadError) throw leadError;

    // 3b. If a staff was selected in the website form, auto-assign that staff to this lead
    let assignedStaff = null;
    if (staff_id && staffForActivity) {
      const staffIdNum = parseInt(staff_id, 10);
      if (!isNaN(staffIdNum)) {
        try {
          const { error: assignError } = await supabase
            .from("lead_assignees")
            .insert({
              lead_id: createdLead.id,
              staff_id: staffIdNum,
            });
          if (assignError) {
            console.warn(
              "[Website Lead] Failed to auto-assign staff from form:",
              assignError.message
            );
          } else {
            // Use the staff data we already fetched for activity log
            // Fetch full staff details for sending welcome message
            const { data: staffData } = await supabase
              .from("staff")
              .select("*")
              .eq("id", staffIdNum)
              .single();
            if (staffData) {
              assignedStaff = staffData;
            }
          }
        } catch (assignErr) {
          console.warn(
            "[Website Lead] Exception while assigning staff:",
            assignErr.message
          );
        }
      }
    }

    // 3c. Send welcome/confirmation template if staff is assigned
    // If staff was assigned from form, send immediately. Otherwise, it will be sent when staff is auto-assigned.
    if (assignedStaff) {
      console.log(
        `[Website Lead] Staff already assigned. Sending confirmation template for lead ${createdLead.id}.`
      );
      try {
        // Use default staff if assigned staff doesn't have phone
        const staffForMessage = assignedStaff.phone
          ? assignedStaff
          : {
              id: 0,
              name: "Madura Travel Service",
              phone: process.env.DEFAULT_STAFF_PHONE || "",
            };

        // DISABLED: MTS summary auto-sending
        // await sendWelcomeWhatsapp(createdLead, customer, staffForMessage);
        // console.log(
        //   `[Website Lead] âœ… Confirmation template sent for lead ${createdLead.id}.`
        // );
        console.log(
          `[Website Lead] MTS summary auto-sending is disabled for lead ${createdLead.id}.`
        );
      } catch (welcomeError) {
        console.error(
          `[Website Lead] âš ï¸ Failed to send confirmation template for lead ${createdLead.id}:`,
          welcomeError.message
        );
        // Don't fail the request if WhatsApp sending fails
      }
    }

    // Notify connected clients about the new lead
    await supabase.channel("crm-updates").send({
      type: "broadcast",
      event: "new-lead",
      payload: { leadId: createdLead.id },
    });

    res.status(201).json({
      message: "Lead created successfully.",
      lead: sanitizeLeadResponse(createdLead),
    });
  } catch (error) {
    console.error("Error creating lead from website:", error);
    res
      .status(500)
      .json({ message: error.message || "An internal server error occurred." });
  }
});

// Helper list for region detection
const indianPlaces = [
  "India",
  "Andhra Pradesh",
  "Arunachal Pradesh",
  "Assam",
  "Bihar",
  "Chhattisgarh",
  "Goa",
  "Gujarat",
  "Haryana",
  "Himachal Pradesh",
  "Jharkhand",
  "Karnataka",
  "Kerala",
  "Madhya Pradesh",
  "Maharashtra",
  "Manipur",
  "Meghalaya",
  "Mizoram",
  "Nagaland",
  "Odisha",
  "Punjab",
  "Rajasthan",
  "Sikkim",
  "Tamil Nadu",
  "Telangana",
  "Tripura",
  "Uttar Pradesh",
  "Uttarakhand",
  "West Bengal",
  "Andaman and Nicobar Islands",
  "Chandigarh",
  "Dadra and Nagar Haveli and Daman and Diu",
  "Delhi",
  "Jammu and Kashmir",
  "Ladakh",
  "Lakshadweep",
  "Puducherry",
  "Mumbai",
  "Delhi",
  "Bangalore",
  "Hyderabad",
  "Ahmedabad",
  "Chennai",
  "Kolkata",
  "Surat",
  "Pune",
  "Jaipur",
].map((p) => p.toLowerCase());

// --- RAZORPAY INVOICING ---
const RAZORPAY_KEY_ID = process.env.RAZORPAY_KEY_ID?.trim();
const RAZORPAY_KEY_SECRET = process.env.RAZORPAY_KEY_SECRET?.trim();
const RAZORPAY_API_URL = "https://api.razorpay.com/v1";

if (!RAZORPAY_KEY_ID || !RAZORPAY_KEY_SECRET) {
  console.log(
    "âš ï¸ WARNING: Razorpay Key ID or Key Secret is not defined. Invoicing will fail."
  );
} else {
  console.log(`[Razorpay] Using Key ID: ${RAZORPAY_KEY_ID.substring(0, 8)}...`);
  // New: Add a debug log for the secret key's length.
  console.log(
    `[Razorpay] Key Secret is loaded. Length: ${RAZORPAY_KEY_SECRET.length}`
  );
}

/**
 * Attempt to send an invoice payment message via WhatsApp template first,
 * then fall back to a CTA URL if the template fails. Returns the send result
 * and the channel used ("template" | "cta") or null on failure.
 */
async function sendInvoiceWhatsappMessage(
  invoice,
  customer,
  leadDestination = ""
) {
  if (!customer?.phone) {
    console.warn("[Invoice WhatsApp] Customer phone missing; skipping send.");
    return null;
  }

  let sanitizedPhone = normalizePhone(customer.phone, "IN");
  if (!sanitizedPhone && customer.phone) {
    const phoneStr = String(customer.phone)
      .trim()
      .replace(/[\s\-\(\)]/g, "");
    if (phoneStr.startsWith("+91") || phoneStr.startsWith("919")) {
      sanitizedPhone = phoneStr.startsWith("+") ? phoneStr : `+${phoneStr}`;
    } else if (phoneStr.length === 10) {
      sanitizedPhone = `+91${phoneStr}`;
    }
  }

  if (!sanitizedPhone) {
    console.warn(
      "[Invoice WhatsApp] Could not normalize phone number; skipping send."
    );
    return null;
  }

  const amountText = `â‚¹${(
    invoice.balance_due ??
    invoice.total_amount ??
    0
  ).toLocaleString("en-IN")}`;
  const linkUrl = invoice.razorpay_payment_link_url;
  let bookingId =
    invoice.booking_id ||
    (invoice.lead && invoice.lead.id
      ? `JA-${invoice.lead.id}`
      : invoice.lead_id
      ? `JA-${invoice.lead_id}`
      : invoice.invoice_number || "Booking");
  let linkSlug = null;
  if (linkUrl) {
    try {
      const parsed = new URL(linkUrl);
      const parts = parsed.pathname.split("/").filter(Boolean);
      linkSlug = parts[parts.length - 1] || null;
    } catch (e) {
      linkSlug = null;
    }
  }

  // Try template first (user will create/configure it)
  try {
    const templateComponents = [
      {
        type: "body",
        parameters: [
          { type: "text", text: customer.first_name || "Customer" }, // {{1}}
          { type: "text", text: invoice.invoice_number || "Invoice" }, // {{2}}
          { type: "text", text: bookingId.toString() }, // {{3}}
          { type: "text", text: amountText }, // {{4}}
          { type: "text", text: leadDestination || "N/A" }, // {{5}}
        ],
      },
    ];

    // Button param: dynamic URL uses {{1}} in template button definition
    if (linkUrl) {
      templateComponents.push({
        type: "button",
        sub_type: "url",
        index: 0,
        parameters: [{ type: "text", text: linkSlug || linkUrl }],
      });
    }

    const templateResult = await sendCrmWhatsappTemplate(
      sanitizedPhone,
      WHATSAPP_INVOICE_TEMPLATE,
      WHATSAPP_TEMPLATE_LANG,
      templateComponents
    );

    if (templateResult) {
      console.log(
        `[Invoice WhatsApp] âœ… Template "${WHATSAPP_INVOICE_TEMPLATE}" sent for invoice #${invoice.invoice_number}`
      );
      return { result: templateResult, channel: "template" };
    }
  } catch (templateErr) {
    console.warn(
      `[Invoice WhatsApp] âš ï¸ Template "${WHATSAPP_INVOICE_TEMPLATE}" failed, will fallback to CTA:`,
      templateErr.message
    );
  }

  if (linkUrl) {
    const messageText = `Hello ${
      customer.first_name || "there"
    },\n\nHere is your invoice #${invoice.invoice_number}${
      leadDestination ? ` for *${leadDestination}*` : ""
    }.\n\n*Total Amount:* â‚¹${(invoice.total_amount ?? 0).toLocaleString(
      "en-IN"
    )}\n*Balance Due:* â‚¹${(invoice.balance_due ?? 0).toLocaleString(
      "en-IN"
    )}\n\nPlease use the button below to complete your payment.`;

    const ctaResult = await sendCrmWhatsappCtaUrl(
      sanitizedPhone,
      messageText,
      "Pay Now",
      linkUrl
    );

    if (ctaResult) {
      console.log(
        `[Invoice WhatsApp] âœ… CTA fallback sent for invoice #${invoice.invoice_number}`
      );
      return { result: ctaResult, channel: "cta" };
    }
  }

  console.warn(
    `[Invoice WhatsApp] âŒ Failed to send invoice WhatsApp message for #${invoice.invoice_number}`
  );
  return null;
}

// Build lead response without travel/tourism fields (academy-only). Those keys are never included.
const LEAD_RESPONSE_KEYS = [
  "id",
  "customer_id",
  "status",
  "priority",
  "lead_type",
  "enquiry",
  "services",
  "summary",
  "notes",
  "activity",
  "branch_ids",
  "source",
  "requirements",
  "last_updated",
  "created_at",
  "updated_at",
  "last_staff_response_at",
  "current_staff_name",
  "academy_data",
];
function sanitizeLeadResponse(lead) {
  if (!lead || typeof lead !== "object") return lead;
  const out = {};
  LEAD_RESPONSE_KEYS.forEach((k) => {
    if (Object.prototype.hasOwnProperty.call(lead, k)) out[k] = lead[k];
  });
  const enquiry =
    out.enquiry ||
    (out.academy_data &&
      typeof out.academy_data === "object" &&
      out.academy_data.enquiry) ||
    null;
  if (enquiry != null) out.enquiry = enquiry;
  return out;
}

// --- WHATSAPP LEAD ENDPOINT ---
app.post("/api/lead/whatsapp", async (req, res) => {
  try {
    const formData = req.body;
    console.log(
      "Received WhatsApp lead data:",
      JSON.stringify(formData, null, 2)
    );

    // Academy WhatsApp lead â€“ only these fields are sent from the bot (no travel/tourism)
    const {
      name,
      phone,
      email,
      enquiry,
      services,
      summary,
      conversation_summary_note,
      events_option,
      consultation_for,
      consultation_mode,
      programme_applied_for: whatsapp_programme,
    } = formData;

    // Validate required fields (only phone is mandatory)
    if (!phone) {
      console.error("WhatsApp Validation failed: Missing phone.", {
        name,
        phone,
      });
      return res.status(400).json({
        message: "Missing required field: phone is required.",
      });
    }

    // Get branchId from request body, default to 1 (India) if not provided
    const targetBranchId = formData.branchId || 1;

    console.log(
      `[CRM] ðŸ¢ Processing lead for Branch ID: ${targetBranchId} (${
        targetBranchId === 1 ? "India" : "Australia"
      })`
    );

    // 1. Find or Create Customer (do this first to get name if customer exists)
    let customer;

    // Normalize phone using the normalizePhone utility function
    // This handles various formats including numbers without + prefix from WhatsApp
    let phoneNormalized = normalizePhone(phone, "IN");

    // Fallback: If normalizePhone fails, try manual normalization
    if (!phoneNormalized && phone) {
      const phoneStr = String(phone)
        .trim()
        .replace(/[\s\-\(\)]/g, "");

      // Handle phone numbers without + prefix (common from WhatsApp)
      // If it starts with 91 (India) and is 12 digits, add +
      if (phoneStr.startsWith("91") && phoneStr.length === 12) {
        phoneNormalized = `+${phoneStr}`;
      }
      // If it's 10 digits (Indian number without country code), add +91
      else if (phoneStr.length === 10 && /^\d+$/.test(phoneStr)) {
        phoneNormalized = `+91${phoneStr}`;
      }
      // If it doesn't start with +, try to add it if it looks like a valid number
      else if (!phoneStr.startsWith("+") && /^\d+$/.test(phoneStr)) {
        // If it's 11-15 digits, assume it has country code and add +
        if (phoneStr.length >= 11 && phoneStr.length <= 15) {
          phoneNormalized = `+${phoneStr}`;
        }
      }
    }

    // Validate phone format (should start with + and have 7-15 digits after country code)
    if (!phoneNormalized || !phoneNormalized.match(/^\+\d{7,15}$/)) {
      console.error(
        "Invalid phone format:",
        phone,
        "normalized:",
        phoneNormalized
      );
      return res.status(400).json({
        message:
          "Invalid phone number format. Please use format: +919876543210",
      });
    }

    const { data: existingCustomer, error: findError } = await supabase
      .from("customers")
      .select("*")
      .or(
        `phone.eq.${phoneNormalized},phone.eq.${phoneNormalized.replace(
          /^\+/,
          ""
        )}`
      )
      .limit(1)
      .maybeSingle();

    if (findError) throw findError;

    // Function to extract name and company from conversation text
    const extractNameAndCompanyFromText = (text) => {
      if (!text) return { name: null, company: null };

      let extractedName = null;
      let extractedCompany = null;

      // Pattern 1: "This side [name] from [company]" - handles "This side aijaz Ahmad from Kashmir GAT Holidays"
      const pattern1 =
        /this\s+side\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\s+from\s+([A-Z][a-z]+(?:\s+[A-Z][a-z\s]+)*)/i;
      const match1 = text.match(pattern1);
      if (match1 && match1[1] && match1[2]) {
        extractedName = match1[1].trim();
        // Extract company name - take everything after "from" until end of sentence or comma
        let companyText = match1[2].trim();
        // Remove trailing punctuation and common endings
        companyText = companyText.replace(/[.,;:!?]+$/, "").trim();
        extractedCompany = companyText;
      }

      // Pattern 2: "[name] from [company]" (general pattern) - handles "Aijaz Ahmad from Kashmir GAT Holidays"
      if (!extractedName) {
        const pattern2 =
          /([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)\s+from\s+([A-Z][a-z]+(?:\s+[A-Z][a-z\s]+)*)/i;
        const match2 = text.match(pattern2);
        if (match2 && match2[1] && match2[2]) {
          extractedName = match2[1].trim();
          let companyText = match2[2].trim();
          companyText = companyText.replace(/[.,;:!?]+$/, "").trim();
          extractedCompany = companyText;
        }
      }

      // Pattern 3: "I am [name]", "My name is [name]", "I'm [name]"
      if (!extractedName) {
        const pattern3 =
          /(?:i\s+am|i'm|my\s+name\s+is|name\s+is|myself)\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)/i;
        const match3 = text.match(pattern3);
        if (match3 && match3[1]) {
          extractedName = match3[1].trim();
        }
      }

      // Pattern 4: Just a name at the start (capitalized words)
      if (!extractedName) {
        const pattern4 = /^([A-Z][a-z]+\s+[A-Z][a-z]+)/;
        const match4 = text.match(pattern4);
        if (match4 && match4[1]) {
          extractedName = match4[1].trim();
        }
      }

      // Validate extracted name
      if (extractedName) {
        // Validate it's a reasonable name (2-50 chars, contains letters)
        if (
          extractedName.length < 2 ||
          extractedName.length > 50 ||
          !/^[A-Za-z\s]+$/.test(extractedName)
        ) {
          extractedName = null;
        }
      }

      // Validate extracted company
      if (extractedCompany) {
        // Don't remove "holidays" etc. as they're part of the company name (e.g., "GAT Holidays")
        // Just trim and validate length
        extractedCompany = extractedCompany.trim();
        if (extractedCompany.length < 2 || extractedCompany.length > 100) {
          extractedCompany = null;
        }
      }

      return { name: extractedName, company: extractedCompany };
    };

    // Determine the name to use: prefer provided name, then extract from conversation, then existing customer name
    let customerName = name;
    let extractedCompany = null;

    // If no name provided, try to extract from conversation_summary_note
    if (!customerName && conversation_summary_note) {
      const extracted = extractNameAndCompanyFromText(
        conversation_summary_note
      );
      if (extracted.name) {
        customerName = extracted.name;
        extractedCompany = extracted.company;
        console.log(
          `[WhatsApp Lead] Extracted name from conversation: "${customerName}"${
            extractedCompany ? `, Company: "${extractedCompany}"` : ""
          }`
        );
      }
    }

    // Use existing customer's name if available
    if (!customerName && existingCustomer) {
      customerName = `${existingCustomer.first_name || ""} ${
        existingCustomer.last_name || ""
      }`.trim();
      if (customerName) {
        console.log(
          `[WhatsApp Lead] Using existing customer name: "${customerName}"`
        );
      }
    }

    // If still no name, return error asking for name
    if (!customerName) {
      console.error(
        `[WhatsApp Lead] No name provided, could not extract from conversation, and no existing customer found for phone: ${phone}`
      );
      return res.status(400).json({
        message: "Name is required. Please provide your name to proceed.",
        error_code: "NAME_REQUIRED",
      });
    }

    if (existingCustomer) {
      customer = existingCustomer;
    } else {
      const nameParts = customerName.split(" ");
      const first_name = nameParts[0] || "WhatsApp";
      const last_name = nameParts.slice(1).join(" ") || "Customer";

      const { data: newCustomer, error: createError } = await supabase
        .from("customers")
        .insert({
          salutation: "Mr.",
          first_name,
          last_name,
          email: email || null,
          phone: phoneNormalized, // Store in continuous format (no spaces): +917397670826
          username: `@${(first_name + last_name)
            .toLowerCase()
            .replace(/\s/g, "")}${Date.now().toString().slice(-4)}`,
          avatar_url: `https://avatar.iran.liara.run/public/boy?username=${Date.now()}`,
          date_added: new Date().toISOString(),
          added_by_branch_id: targetBranchId,
        })
        .select()
        .single();

      if (createError) throw createError;
      customer = newCustomer;
    }

    // Define a system user for notes and activities
    const systemUserAsStaff = {
      id: 0, // Using 0 for system/bot user
      user_id: "system_bot",
      name: "WhatsApp Bot",
      avatar_url: "https://i.imgur.com/T4lG3g9.png", // A simple bot icon
      email: "bot@system.local",
      phone: "",
      role_id: 3, // Staff role
      status: "Active",
      branch_id: targetBranchId,
      leads_attended: 0,
      leads_missed: 0,
      avg_response_time: null,
      last_response_at: null,
      last_active_at: null,
      work_hours_today: 0,
      activity_log: [],
      on_leave_until: null,
      destinations: "",
      services: [],
    };

    // Create notes
    const allNotes = [];

    // Add the AI-extracted conversation note first if it exists
    if (conversation_summary_note) {
      let noteText = `Initial user query via AI flow:\n"${conversation_summary_note}"`;
      if (extractedCompany) {
        noteText += `\n\nCompany: ${extractedCompany}`;
      }
      const conversationNote = {
        id: Date.now(),
        text: noteText,
        date: new Date().toISOString(),
        addedBy: systemUserAsStaff,
        mentions: [],
      };
      allNotes.push(conversationNote);
    }

    // Add the structured summary note
    const summaryText =
      summary || `Lead from WhatsApp bot regarding ${enquiry}.`;
    const summaryNote = {
      id: Date.now() + 1, // ensure unique id
      text: summaryText,
      date: new Date().toISOString(),
      addedBy: systemUserAsStaff,
      mentions: [],
    };
    allNotes.push(summaryNote);

    // 2. Build academy_data from WhatsApp flow fields (for Lead Detail panel)
    const academy_data = {};
    if (enquiry) academy_data.enquiry = enquiry;
    if (events_option) academy_data.events_option = events_option;
    if (consultation_for) academy_data.consultation_for = consultation_for;
    if (consultation_mode) academy_data.consultation_mode = consultation_mode;
    if (whatsapp_programme)
      academy_data.programme_applied_for = whatsapp_programme;

    // 3. Create Lead â€“ academy schema only (enquiry + academy_data; no travel/tourism columns)
    const newLead = {
      customer_id: customer.id,
      status: "Enquiry",
      priority: "Low",
      lead_type: "Warm",
      services: services || (enquiry ? [enquiry] : []),
      summary: summaryText,
      notes: allNotes,
      activity: [
        {
          id: Date.now(),
          type: "Lead Created",
          description: "Lead created via WhatsApp Bot.",
          user: "System",
          timestamp: new Date().toISOString(),
        },
      ],
      branch_ids: [targetBranchId],
      source: "whatsapp",
      created_at: new Date().toISOString(),
      last_updated: new Date().toISOString(),
      ...(Object.keys(academy_data).length > 0 ? { academy_data } : {}),
    };

    const { data: createdLead, error: leadError } = await supabase
      .from("leads")
      .insert(newLead)
      .select()
      .single();

    if (leadError) throw leadError;

    // --- START TOUR PACKAGE AUTOMATION ---
    if ((createdLead.services || []).includes("Tour Package")) {
      console.log(
        `[Tour Package Flow] Lead ${createdLead.id} created. Booking flow will start after agent assignment.`
      );
    }
    // --- END TOUR PACKAGE AUTOMATION ---

    // Notify connected clients about the new lead
    await supabase.channel("crm-updates").send({
      type: "broadcast",
      event: "new-lead",
      payload: { leadId: createdLead.id },
    });

    res.status(201).json({
      message: "Lead created successfully from WhatsApp.",
      lead: sanitizeLeadResponse(createdLead),
    });
  } catch (error) {
    console.error("Error creating lead from WhatsApp:", error);
    res
      .status(500)
      .json({ message: error.message || "An internal server error occurred." });
  }
});

app.post("/api/invoicing/create-link", async (req, res) => {
  try {
    const { invoiceId } = req.body;
    if (!invoiceId) {
      return res.status(400).json({ message: "Invoice ID is required." });
    }

    const { data: invoice, error: invoiceError } = await supabase
      .from("invoices")
      .select("*, customer:customers(*)")
      .eq("id", invoiceId)
      .single();

    if (invoiceError || !invoice) {
      throw new Error(invoiceError?.message || "Invoice not found.");
    }
    if (!invoice.customer) {
      throw new Error("Customer details not found for this invoice.");
    }

    if (invoice.balance_due <= 0) {
      return res.status(400).json({
        message:
          "Invoice amount must be greater than zero to generate a payment link.",
      });
    }

    const auth = Buffer.from(
      `${RAZORPAY_KEY_ID}:${RAZORPAY_KEY_SECRET}`
    ).toString("base64");
    const response = await fetch(`${RAZORPAY_API_URL}/payment_links`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Basic ${auth}`,
      },
      body: JSON.stringify({
        amount: invoice.balance_due * 100,
        currency: "INR",
        description: `Payment for Invoice #${invoice.invoice_number}`,
        customer: {
          name: `${invoice.customer.first_name} ${invoice.customer.last_name}`,
          email: invoice.customer.email,
          contact: invoice.customer.phone.replace(/[^0-9]/g, "").slice(-10),
        },
        notify: { sms: true, email: true },
        reminder_enable: true,
        callback_url: "https://crm.jeppiaaracademy.com/payments",
        callback_method: "get",
      }),
    });

    const razorpayData = await response.json();
    if (!response.ok) {
      console.error("Razorpay Error:", razorpayData);
      throw new Error(
        razorpayData.error?.description || "Failed to create Razorpay link."
      );
    }

    const { error: updateError } = await supabase
      .from("invoices")
      .update({
        razorpay_payment_link_id: razorpayData.id,
        razorpay_payment_link_url: razorpayData.short_url,
        status: "SENT",
      })
      .eq("id", invoiceId);

    if (updateError) {
      console.error(
        "Failed to update invoice with Razorpay link:",
        updateError
      );
    }

    res.status(200).json({ paymentLink: razorpayData.short_url });
  } catch (error) {
    console.error("Error creating Razorpay payment link:", error);
    res
      .status(500)
      .json({ message: error.message || "An internal server error occurred." });
  }
});

// --- IMMEDIATE LEAD NOTIFICATION ENDPOINT ---
// Allows CRM UI to trigger WhatsApp notification immediately (no realtime delay)
app.post("/api/lead/notify-immediate", async (req, res) => {
  try {
    const { leadId } = req.body;
    if (!leadId) {
      return res.status(400).json({ message: "leadId is required." });
    }

    const sendStartTime = Date.now();

    // Fetch lead, customer, and assigned staff in parallel
    const [
      { data: lead, error: leadErr },
      { data: customer, error: custErr },
      { data: assignees },
    ] = await Promise.all([
      supabase.from("leads").select("*").eq("id", leadId).single(),
      supabase.from("customers").select("*").eq("id", null).single(), // Placeholder
      supabase
        .from("lead_assignees")
        .select("staff(*)")
        .eq("lead_id", leadId)
        .limit(1),
    ]);

    if (leadErr || !lead) {
      return res
        .status(404)
        .json({ message: "Lead not found.", error: leadErr?.message });
    }

    // Fetch customer using lead's customer_id
    const { data: customerData, error: custErr2 } = await supabase
      .from("customers")
      .select("*")
      .eq("id", lead.customer_id)
      .single();

    if (custErr2 || !customerData) {
      return res
        .status(404)
        .json({ message: "Customer not found.", error: custErr2?.message });
    }

    const staff = (assignees && assignees[0] && assignees[0].staff) || {
      id: 0,
      name: "Madura Travel Service",
      phone: process.env.DEFAULT_STAFF_PHONE || "",
    };

    // Send WhatsApp immediately
    await sendWelcomeWhatsapp(lead, customerData, staff);
    const sendEndTime = Date.now();

    console.log(
      `[Notify API] WhatsApp sent for lead ${leadId} in ${
        sendEndTime - sendStartTime
      }ms.`
    );
    res.status(200).json({
      message: "WhatsApp notification sent successfully.",
      leadId,
      timeMs: sendEndTime - sendStartTime,
    });
  } catch (error) {
    console.error("Error in /api/lead/notify-immediate:", error.message);
    res
      .status(500)
      .json({ message: error.message || "Internal server error." });
  }
});

app.post("/api/invoicing/send-whatsapp", async (req, res) => {
  try {
    const { invoiceId } = req.body;
    if (!invoiceId) {
      return res.status(400).json({ message: "Invoice ID is required." });
    }

    const { data: invoice, error } = await supabase
      .from("invoices")
      .select("*, customer:customers(*), lead:leads(destination)")
      .eq("id", invoiceId)
      .single();

    if (
      error ||
      !invoice ||
      !invoice.customer ||
      !invoice.razorpay_payment_link_url
    ) {
      throw new Error(
        error?.message || "Invoice, customer, or payment link not found."
      );
    }

    const sendResult = await sendInvoiceWhatsappMessage(
      invoice,
      invoice.customer,
      invoice.lead?.destination || ""
    );

    if (sendResult && invoice.lead_id) {
      await logLeadActivity(
        invoice.lead_id,
        "WhatsApp Sent",
        `Invoice #${invoice.invoice_number} sent to customer via WhatsApp (${sendResult.channel}).`
      );
    } else if (!sendResult) {
      if (invoice.lead_id)
        await logLeadActivity(
          invoice.lead_id,
          "WhatsApp Failed",
          `Failed to send invoice #${invoice.invoice_number} to customer.`
        );
      throw new Error("Failed to send WhatsApp message via provider.");
    }

    res.status(200).json({ message: "WhatsApp message sent successfully." });
  } catch (error) {
    console.error("Error sending WhatsApp invoice:", error);
    res
      .status(500)
      .json({ message: error.message || "An internal server error occurred." });
  }
});

// Send lead summary via WhatsApp (uses mts_summary template)
// Send WhatsApp text message endpoint
app.post("/api/whatsapp/send-text", async (req, res) => {
  try {
    const { to, text } = req.body;
    if (!to || !text) {
      return res.status(400).json({ message: "to and text are required." });
    }

    // Normalize phone number
    let sanitizedPhone = normalizePhone(to, "IN");
    if (!sanitizedPhone) {
      const phoneStr = String(to)
        .trim()
        .replace(/[\s\-\(\)]/g, "");
      if (phoneStr.startsWith("+91") || phoneStr.startsWith("91")) {
        sanitizedPhone = phoneStr.startsWith("+") ? phoneStr : `+${phoneStr}`;
      } else if (phoneStr.length === 10) {
        sanitizedPhone = `+91${phoneStr}`;
      } else if (phoneStr.startsWith("+")) {
        sanitizedPhone = phoneStr;
      }
    }
    if (!sanitizedPhone) {
      return res.status(400).json({ message: "Invalid phone number format." });
    }

    const result = await sendCrmWhatsappText(sanitizedPhone, text);
    if (!result) {
      return res
        .status(500)
        .json({ message: "Failed to send WhatsApp message." });
    }

    res.json({ success: true, messageId: result.messages?.[0]?.id });
  } catch (err) {
    console.error("[CRM] Error in /api/whatsapp/send-text:", err);
    res
      .status(500)
      .json({ message: err.message || "Failed to send WhatsApp message." });
  }
});

app.post("/api/whatsapp/send-summary", async (req, res) => {
  try {
    const { leadId } = req.body;
    if (!leadId) {
      return res.status(400).json({ message: "leadId is required." });
    }

    // Fetch lead with customer and assigned staff
    const { data: lead, error: leadError } = await supabase
      .from("leads")
      .select(
        "*, customer:customers(*), all_assignees:lead_assignees(staff(*))"
      )
      .eq("id", leadId)
      .single();

    if (leadError || !lead) {
      throw new Error(leadError?.message || "Lead not found.");
    }
    if (!lead.customer || !lead.customer.phone) {
      throw new Error("Customer phone not available for this lead.");
    }

    // Get primary assigned staff (first assignee)
    const primaryStaff =
      lead.all_assignees && lead.all_assignees.length > 0
        ? lead.all_assignees[0].staff
        : {
            id: 0,
            name: "Madura Travel Service",
            phone: process.env.DEFAULT_STAFF_PHONE || "",
          };

    // Normalize phone number
    let sanitizedPhone = normalizePhone(lead.customer.phone, "IN");
    if (!sanitizedPhone) {
      const phoneStr = String(lead.customer.phone)
        .trim()
        .replace(/[\s\-\(\)]/g, "");
      if (phoneStr.startsWith("+91") || phoneStr.startsWith("91")) {
        sanitizedPhone = phoneStr.startsWith("+") ? phoneStr : `+${phoneStr}`;
      } else if (phoneStr.length === 10) {
        sanitizedPhone = `+91${phoneStr}`;
      }
    }
    if (!sanitizedPhone) {
      throw new Error("Could not normalize customer phone.");
    }

    // Use the same summary generation function as sendWelcomeWhatsapp
    // Validate that all required fields are filled before sending MTS summary
    const validation = validateMtsSummaryRequiredFields(lead);
    if (!validation.isValid) {
      // Only show truly required fields (Services, Destination, Duration)
      const requiredMissingFields = Object.entries(validation.missingFields)
        .filter(
          ([field, missing]) =>
            missing && ["services", "destination", "duration"].includes(field)
        )
        .map(([field]) => field)
        .join(", ");
      console.log(
        `[Send Summary] âš ï¸ Cannot send MTS summary for lead ${lead.id}: Missing required fields: ${requiredMissingFields}`
      );
      return res.status(400).json({
        message: `Cannot send summary. Missing required fields: ${requiredMissingFields}. Please fill: Services, Destination, and Duration. (Date of Travel and Passenger Details are optional and can be filled by agents later.)`,
        missingFields: validation.missingFields,
      });
    }

    const { bookingId, summaryText, customerName, staffName } =
      generateLeadSummary(lead, lead.customer, primaryStaff);

    // Clean summary text for template: Remove newlines, tabs, and multiple consecutive spaces
    // Meta Business Manager templates don't allow newlines/tabs in text parameters
    const cleanSummaryText = (summaryText || "")
      .replace(/\n/g, " ") // Replace newlines with spaces
      .replace(/\t/g, " ") // Replace tabs with spaces
      .replace(/[ ]{5,}/g, " ") // Replace 5+ consecutive spaces with single space
      .replace(/[ ]{2,}/g, " ") // Replace 2+ consecutive spaces with single space
      .trim();

    // Prepare template components for mts_summary template
    // The template must have buttons defined in Meta Business Manager: "Confirm Enquiry" and "Talk to Agent"
    const templateComponents = [
      {
        type: "body",
        parameters: [
          { type: "text", text: customerName || "" }, // {{1}} - Customer name
          { type: "text", text: bookingId || "" }, // {{2}} - Booking ID
          { type: "text", text: staffName || "" }, // {{3}} - Staff name
          { type: "text", text: cleanSummaryText }, // {{4}} - Summary (cleaned)
        ],
      },
    ];

    // Send mts_summary template ONLY (includes welcome message + confirmation buttons)
    // This is the single welcome/confirmation message - no separate messages needed
    console.log(
      `[Send Summary] ðŸ“¤ Sending mts_summary template (welcome + confirmation) to ${sanitizedPhone} for lead ${lead.id}.`
    );

    const result = await sendCrmWhatsappTemplate(
      sanitizedPhone,
      "mts_summary",
      "en",
      templateComponents
    );

    if (result) {
      const messageId = result.messages?.[0]?.id;
      if (messageId) {
        // Store message ID -> lead ID mapping for button click handling
        messageIdToLeadCache.set(messageId, {
          leadId: lead.id,
          customerId: lead.customer.id,
          customerName: `${lead.customer.first_name} ${lead.customer.last_name}`,
          timestamp: Date.now(),
        });
        console.log(
          `[Send Summary] âœ… Template sent successfully. Message ID: ${messageId}, Lead ID: ${lead.id}`
        );
      } else {
        console.log(
          `[Send Summary] âœ… Template sent successfully (no message ID in response) for lead ${lead.id}.`
        );
      }
    } else {
      console.error(
        `[Send Summary] âŒ Failed to send mts_summary template for lead ${lead.id} to ${sanitizedPhone}. Template may not be approved in Meta Business Manager.`
      );
    }

    if (lead.id) {
      await logLeadActivity(
        lead.id,
        "Summary Sent",
        `Summary sent to customer "${lead.customer.first_name} ${lead.customer.last_name}" via WhatsApp.`
      );
    }

    return res
      .status(200)
      .json({ message: "Summary sent via WhatsApp.", result });
  } catch (error) {
    console.error("[Send Summary] Error:", error.message);
    return res
      .status(500)
      .json({ message: error.message || "Failed to send summary." });
  }
});

// Itineraries not in scope for this CRM â€“ endpoint disabled
app.post("/api/whatsapp/send-itinerary", (_req, res) => {
  return res
    .status(501)
    .json({ message: "Itinerary send is not available in this CRM." });
});

// ====================================================================
// USER SESSION TRACKING ENDPOINTS
// ====================================================================

// Record user login (called when user authenticates)
app.post("/api/sessions/login", requireAuth, async (req, res) => {
  try {
    const userId = req.user.user_id || req.user.id;
    const staffId = req.user.id; // staff.id from requireAuth
    const today = new Date().toISOString().split("T")[0]; // YYYY-MM-DD

    // Get or create today's session
    const { data: existingSession } = await supabase
      .from("user_sessions")
      .select("*")
      .eq("user_id", userId)
      .eq("date", today)
      .single();

    const now = new Date().toISOString();

    if (existingSession) {
      // Update existing session - new login for the day
      const { error: updateError } = await supabase
        .from("user_sessions")
        .update({
          first_login_time: existingSession.first_login_time || now, // Keep first login
          last_activity_time: now,
          last_logout_time: null, // Reset logout time
          session_status: "active",
          updated_at: now,
        })
        .eq("id", existingSession.id);

      if (updateError) {
        throw new Error(`Failed to update session: ${updateError.message}`);
      }
    } else {
      // Create new session for today
      const { error: insertError } = await supabase
        .from("user_sessions")
        .insert({
          user_id: userId,
          staff_id: staffId,
          date: today,
          first_login_time: now,
          last_activity_time: now,
          session_status: "active",
          total_active_seconds: 0,
        });

      if (insertError) {
        throw new Error(`Failed to create session: ${insertError.message}`);
      }
    }

    return res.status(200).json({ message: "Login recorded successfully" });
  } catch (error) {
    console.error("[Session Login] Error:", error.message);
    return res
      .status(500)
      .json({ message: error.message || "Failed to record login" });
  }
});

// Heartbeat - Update activity time and accumulate active seconds
app.post("/api/sessions/heartbeat", requireAuth, async (req, res) => {
  try {
    const userId = req.user.user_id || req.user.id;
    const staffId = req.user.id;
    const { activeSeconds = 0, isPageVisible = true } = req.body;
    const today = new Date().toISOString().split("T")[0];
    const now = new Date().toISOString();

    // Determine session status based on activity
    let sessionStatus = "active";
    if (!isPageVisible) {
      sessionStatus = "idle";
    }

    // Get existing session
    const { data: existingSession } = await supabase
      .from("user_sessions")
      .select("*")
      .eq("user_id", userId)
      .eq("date", today)
      .single();

    if (existingSession) {
      // Update existing session
      const newTotalSeconds =
        (existingSession.total_active_seconds || 0) +
        Math.max(0, Math.floor(activeSeconds));

      const { error: updateError } = await supabase
        .from("user_sessions")
        .update({
          last_activity_time: now,
          total_active_seconds: newTotalSeconds,
          session_status: sessionStatus,
          updated_at: now,
        })
        .eq("id", existingSession.id);

      if (updateError) {
        throw new Error(`Failed to update session: ${updateError.message}`);
      }
    } else {
      // Create session if it doesn't exist (shouldn't happen, but handle gracefully)
      const { error: insertError } = await supabase
        .from("user_sessions")
        .insert({
          user_id: userId,
          staff_id: staffId,
          date: today,
          first_login_time: now,
          last_activity_time: now,
          session_status: sessionStatus,
          total_active_seconds: Math.max(0, Math.floor(activeSeconds)),
        });

      if (insertError) {
        throw new Error(`Failed to create session: ${insertError.message}`);
      }
    }

    return res.status(200).json({ message: "Heartbeat recorded" });
  } catch (error) {
    console.error("[Session Heartbeat] Error:", error.message);
    return res
      .status(500)
      .json({ message: error.message || "Failed to record heartbeat" });
  }
});

// Record user logout
app.post("/api/sessions/logout", requireAuth, async (req, res) => {
  try {
    const userId = req.user.user_id || req.user.id;
    const today = new Date().toISOString().split("T")[0];
    const now = new Date().toISOString();

    // Update session with logout time
    const { data: existingSession } = await supabase
      .from("user_sessions")
      .select("*")
      .eq("user_id", userId)
      .eq("date", today)
      .single();

    if (existingSession) {
      const { error: updateError } = await supabase
        .from("user_sessions")
        .update({
          last_logout_time: now,
          session_status: "logged_out",
          updated_at: now,
        })
        .eq("id", existingSession.id);

      if (updateError) {
        throw new Error(`Failed to update session: ${updateError.message}`);
      }
    }

    return res.status(200).json({ message: "Logout recorded successfully" });
  } catch (error) {
    console.error("[Session Logout] Error:", error.message);
    return res
      .status(500)
      .json({ message: error.message || "Failed to record logout" });
  }
});

// Get session report data
app.get("/api/sessions/report", requireAuth, async (req, res) => {
  try {
    const { staffId, startDate, endDate, period = "daily" } = req.query;

    // Check if user is admin/manager (can view all) or staff (can only view own)
    const isAdmin =
      req.user.role === "Super Admin" || req.user.role === "Manager";
    const requestingStaffId = req.user.id; // staff.id from requireAuth

    let query = supabase
      .from("user_sessions")
      .select(
        `
        *,
        staff:staff_id (
          id,
          name,
          email,
          branch_id
        )
      `
      )
      .order("date", { ascending: false });

    // Apply filters
    if (startDate) {
      query = query.gte("date", startDate);
    }
    if (endDate) {
      query = query.lte("date", endDate);
    }

    // If not admin, only show own data
    if (!isAdmin) {
      query = query.eq("staff_id", requestingStaffId);
    } else if (staffId) {
      // Admin can filter by specific staff
      query = query.eq("staff_id", parseInt(staffId));
    }

    const { data: sessions, error: queryError } = await query;

    if (queryError) {
      throw new Error(`Failed to fetch sessions: ${queryError.message}`);
    }

    // Group by period if needed
    let groupedData = sessions || [];
    if (period === "weekly") {
      // Group by week
      const weekMap = new Map();
      sessions.forEach((session) => {
        const date = new Date(session.date);
        const weekStart = new Date(date);
        weekStart.setDate(date.getDate() - date.getDay()); // Start of week (Sunday)
        const weekKey = weekStart.toISOString().split("T")[0];

        if (!weekMap.has(weekKey)) {
          weekMap.set(weekKey, {
            period: weekKey,
            first_login_time: session.first_login_time,
            last_logout_time: session.last_logout_time,
            total_active_seconds: 0,
            sessions: [],
          });
        }

        const weekData = weekMap.get(weekKey);
        weekData.total_active_seconds += session.total_active_seconds || 0;
        weekData.sessions.push(session);

        // Update first login (earliest) and last logout (latest)
        if (
          !weekData.first_login_time ||
          (session.first_login_time &&
            new Date(session.first_login_time) <
              new Date(weekData.first_login_time))
        ) {
          weekData.first_login_time = session.first_login_time;
        }
        if (
          !weekData.last_logout_time ||
          (session.last_logout_time &&
            new Date(session.last_logout_time) >
              new Date(weekData.last_logout_time))
        ) {
          weekData.last_logout_time = session.last_logout_time;
        }
      });
      groupedData = Array.from(weekMap.values());
    } else if (period === "monthly") {
      // Group by month
      const monthMap = new Map();
      sessions.forEach((session) => {
        const date = new Date(session.date);
        const monthKey = `${date.getFullYear()}-${String(
          date.getMonth() + 1
        ).padStart(2, "0")}`;

        if (!monthMap.has(monthKey)) {
          monthMap.set(monthKey, {
            period: monthKey,
            first_login_time: session.first_login_time,
            last_logout_time: session.last_logout_time,
            total_active_seconds: 0,
            sessions: [],
          });
        }

        const monthData = monthMap.get(monthKey);
        monthData.total_active_seconds += session.total_active_seconds || 0;
        monthData.sessions.push(session);

        // Update first login (earliest) and last logout (latest)
        if (
          !monthData.first_login_time ||
          (session.first_login_time &&
            new Date(session.first_login_time) <
              new Date(monthData.first_login_time))
        ) {
          monthData.first_login_time = session.first_login_time;
        }
        if (
          !monthData.last_logout_time ||
          (session.last_logout_time &&
            new Date(session.last_logout_time) >
              new Date(monthData.last_logout_time))
        ) {
          monthData.last_logout_time = session.last_logout_time;
        }
      });
      groupedData = Array.from(monthMap.values());
    }

    return res.status(200).json({ data: groupedData, period });
  } catch (error) {
    console.error("[Session Report] Error:", error.message);
    return res
      .status(500)
      .json({ message: error.message || "Failed to fetch session report" });
  }
});

// Send feedback template to customer when lead status is Feedback
app.post("/api/feedback/send", async (req, res) => {
  try {
    const { leadId } = req.body;
    if (!leadId) {
      return res.status(400).json({ message: "leadId is required." });
    }

    // Fetch lead with customer
    const { data: lead, error: leadError } = await supabase
      .from("leads")
      .select("*, customer:customers(*)")
      .eq("id", leadId)
      .single();

    if (leadError || !lead) {
      throw new Error(leadError?.message || "Lead not found.");
    }

    if (!lead.customer) {
      throw new Error("Customer not found for this lead.");
    }

    // Check if feedback was already sent (via activity; sendFeedbackLinkMessage also checks this)
    const feedbackSent = (lead.activity || []).some(
      (act) =>
        act.type === "Feedback Request Sent" &&
        act.description?.includes("Feedback request sent to customer")
    );
    if (feedbackSent) {
      console.log(
        `[Feedback Endpoint] Feedback already sent for lead ${leadId}. Skipping.`
      );
      return res.status(200).json({
        message: "Feedback already sent for this lead.",
        alreadySent: true,
      });
    }

    // Send feedback template
    await sendFeedbackLinkMessage(lead, lead.customer);

    console.log(
      `[Feedback Endpoint] âœ… Feedback template sent successfully for lead ${leadId}`
    );

    return res.status(200).json({
      message: "Feedback template sent successfully.",
      leadId: leadId,
    });
  } catch (error) {
    console.error("[Feedback Endpoint] Error:", error.message);
    return res
      .status(500)
      .json({ message: error.message || "Failed to send feedback." });
  }
});

app.post("/api/razorpay-webhook", async (req, res) => {
  // TODO: Add webhook signature verification in production
  console.log("[Webhook] Razorpay webhook received:", req.body);

  const event = req.body.event;
  const payload = req.body.payload;

  if (event === "payment_link.paid") {
    const paymentLinkId = payload.payment_link.entity.id;
    const amountPaid = payload.payment.entity.amount / 100; // Amount is in paise

    try {
      // 1. Find the invoice associated with the payment link
      const { data: invoice, error: invoiceError } = await supabase
        .from("invoices")
        .select("*, lead:leads(*), customer:customers(*)")
        .eq("razorpay_payment_link_id", paymentLinkId)
        .single();

      if (invoiceError || !invoice) {
        console.error(
          `[Webhook] Invoice not found for payment_link_id ${paymentLinkId}. Error: ${invoiceError?.message}`
        );
        return res.status(404).json({ message: "Invoice not found." });
      }

      // 2. Record payment in payments table first
      const paymentEntity = payload.payment?.entity;
      const paymentId = paymentEntity?.id || paymentLinkId;
      const paymentDate = paymentEntity?.created_at
        ? new Date(paymentEntity.created_at * 1000).toISOString()
        : new Date().toISOString();

      const { error: paymentInsertError } = await supabase
        .from("payments")
        .insert({
          invoice_id: invoice.id,
          lead_id: invoice.lead_id,
          customer_id: invoice.customer_id,
          payment_date: paymentDate,
          amount: amountPaid,
          method: "Razorpay",
          reference_id: paymentId,
          razorpay_payment_id: paymentId,
          status: "Paid",
          notes: `Razorpay payment_link ${paymentLinkId}`,
          source: "RazorpayWebhook",
          created_at: new Date().toISOString(),
        });
      if (paymentInsertError) {
        throw new Error(
          `Failed to insert payment record: ${paymentInsertError.message}`
        );
      }

      // 3. Recalculate invoice balance using helper function
      const { recalculateInvoiceBalance } = await import(
        "./utils/invoiceBalance.js"
      );
      await recalculateInvoiceBalance(supabase, invoice.id);
      console.log(
        `[Webhook] Invoice ${invoice.id} balance recalculated after payment.`
      );

      // 4. Update Lead Status
      if (invoice.lead) {
        const activityDescription = `Payment of â‚¹${amountPaid.toLocaleString()} received via Razorpay for Invoice #${
          invoice.invoice_number
        }.`;
        const newActivity = {
          id: Date.now(),
          type: "Payment Received",
          description: activityDescription,
          user: "System",
          timestamp: new Date().toISOString(),
        };
        const updatedActivity = [newActivity, ...(invoice.lead.activity || [])];

        const { error: updateLeadError } = await supabase
          .from("leads")
          .update({
            status: "Billing Completed",
            lead_type: "Booked",
            activity: updatedActivity,
            last_updated: new Date().toISOString(),
          })
          .eq("id", invoice.lead.id);

        if (updateLeadError)
          throw new Error(`Failed to update lead: ${updateLeadError.message}`);
        console.log(
          `[Webhook] Lead ${invoice.lead.id} status updated to "Billing Completed" and type to "Booked".`
        );

        // 5. Send confirmation to customer via WhatsApp
        if (invoice.customer && invoice.customer.phone) {
          let sanitizedPhone = invoice.customer.phone.replace(/[^0-9]/g, "");
          if (sanitizedPhone.length === 10)
            sanitizedPhone = "91" + sanitizedPhone;

          const confirmationMessage = `ðŸŽ‰ Your payment of â‚¹${amountPaid.toLocaleString()} for the trip to *${
            invoice.lead.destination
          }* has been received!\n\nYour booking is now confirmed. Our team will get in touch with you shortly with the next steps. Thank you for choosing Madura Travel Service!`;
          await sendCrmWhatsappText(sanitizedPhone, confirmationMessage);
        }
      }
    } catch (error) {
      console.error(
        `[Webhook] Error processing payment_link.paid event:`,
        error.message
      );
      return res
        .status(500)
        .json({ message: "Internal server error during webhook processing." });
    }
  }

  res.status(200).json({ status: "ok" });
});

// --- EMAIL SUPPLIER REQUIREMENTS ---

const sendSupplierRequestEmails = async (
  lead,
  staff,
  suppliers,
  branchEmail,
  triggeredBy = "System (Automatic)"
) => {
  if (!suppliers || suppliers.length === 0) {
    console.log(`No suppliers to email for lead ${lead.id}.`);
    return;
  }

  console.log(
    `Preparing to send ${suppliers.length} requirement emails for lead ${lead.id}...`
  );

  const emailPromises = suppliers.map((supplier) => {
    const subject = `Madura Travel Service Requirement â€“ ${
      lead.starting_point || "N/A"
    } to ${lead.destination} (${new Date(
      lead.travel_date
    ).toLocaleDateString()})`;

    const requirements = lead.requirements || {};
    const totalAdults = (requirements.rooms || []).reduce(
      (sum, room) => sum + room.adults,
      0
    );
    const totalChildren = (requirements.rooms || []).reduce(
      (sum, room) => sum + room.children,
      0
    );

    const roomConfigs = (requirements.rooms || [])
      .map(
        (room, index) =>
          `<li>Room ${index + 1}: ${room.adults} Adults, ${
            room.children
          } Children</li>`
      )
      .join("");

    const body = `
            <p>Dear ${
              supplier.contact_person_name || supplier.company_name
            },</p>
            <p>Greetings from Madura Travel Service.</p>
            <p>We are reaching out to request your quotation and best available options for the following travel requirement:</p>
            
            <h3>Travel Details:</h3>
            <ul>
                <li><strong>Starting Point:</strong> ${
                  lead.starting_point || "N/A"
                }</li>
                <li><strong>Destination:</strong> ${lead.destination}</li>
                <li><strong>Date of Travel:</strong> ${new Date(
                  lead.travel_date
                ).toLocaleDateString("en-GB", {
                  day: "numeric",
                  month: "long",
                  year: "numeric",
                })}</li>
                <li><strong>Duration:</strong> ${lead.duration || "N/A"}</li>
                <li><strong>Type of Tour:</strong> ${
                  lead.tour_type
                    ? lead.tour_type.charAt(0).toUpperCase() +
                      lead.tour_type.slice(1)
                    : "N/A"
                }</li>
            </ul>

            <h3>Customer Requirements:</h3>
            <ul>
                <li><strong>Passengers:</strong> ${totalAdults} Adults, ${totalChildren} Children</li>
                <li><strong>Room Configuration:</strong><ul>${roomConfigs}</ul></li>
                <li><strong>Hotel Preference:</strong> ${
                  requirements.hotelPreference || "N/A"
                }</li>
                <li><strong>Stay Preference:</strong> ${
                  requirements.stayPreference || "N/A"
                }</li>
            </ul>

            <p>Kindly share with us your proposed itinerary, inclusions/exclusions, price details, and any available upgrade or customization options if available.</p>
            <p>Please let us know if you require any additional details to prepare the proposal.</p>
            <p>Looking forward to your prompt response.</p>
            <br>
            <p>Warm regards,</p>
            <p><strong>${staff.name}</strong><br>
            Madura Travel Service<br>
            ${staff.phone}<br>
            ${staff.email}</p>
        `;

    const mailOptions = {
      from: `"Madura Travel Service" <${process.env.SMTP_USER}>`,
      to: supplier.email,
      cc: [branchEmail, staff.email],
      subject: subject,
      html: body,
    };

    console.log(
      `Sending email to ${supplier.company_name} (${supplier.email}) for lead ${lead.id}.`
    );
    return transporter.sendMail(mailOptions);
  });

  try {
    await Promise.all(emailPromises);
    console.log(
      `All ${suppliers.length} emails sent successfully for lead ${lead.id}.`
    );

    // Create activity log entry
    const emailActivity = {
      id: Date.now(),
      type: "Supplier Email Sent",
      description: `Sent requirement emails to ${
        suppliers.length
      } supplier(s): ${suppliers.map((s) => s.company_name).join(", ")}.`,
      user: triggeredBy,
      timestamp: new Date().toISOString(),
    };

    // Fetch current lead to get its activity array
    const { data: currentLead, error: fetchError } = await supabase
      .from("leads")
      .select("activity")
      .eq("id", lead.id)
      .single();

    if (fetchError) {
      console.error(
        `Could not fetch lead ${lead.id} to update activity log for email sending:`,
        fetchError.message
      );
    }

    const updatedActivity = currentLead
      ? [emailActivity, ...(currentLead.activity || [])]
      : [emailActivity];

    // Update the lead with the timestamp AND the new activity log
    const { error: updateError } = await supabase
      .from("leads")
      .update({
        supplier_email_sent_at: new Date().toISOString(),
        activity: updatedActivity,
        last_updated: new Date().toISOString(),
      })
      .eq("id", lead.id);

    if (updateError) {
      console.error(
        `Failed to update email sent status/activity for lead ${lead.id}:`,
        updateError
      );
    } else {
      console.log(
        `Updated email sent status and activity for lead ${lead.id}.`
      );
    }
  } catch (error) {
    console.error(
      `Error sending one or more supplier emails for lead ${lead.id}:`,
      error
    );
    throw error;
  }
};

app.post("/api/email/send-supplier-request", async (req, res) => {
  const { lead, staff, suppliers, branchEmail } = req.body;

  if (
    !lead ||
    !staff ||
    !suppliers ||
    !Array.isArray(suppliers) ||
    !branchEmail
  ) {
    return res.status(400).json({
      message:
        "Missing required data: lead, staff, suppliers, and branchEmail.",
    });
  }

  try {
    await sendSupplierRequestEmails(
      lead,
      staff,
      suppliers,
      branchEmail,
      staff.name
    );
    res
      .status(200)
      .json({ message: `Successfully sent ${suppliers.length} emails.` });
  } catch (error) {
    console.error("Error sending supplier emails:", error);
    res.status(500).json({
      message:
        error.message ||
        "An internal server error occurred while sending emails.",
    });
  }
});

// --- REALTIME LISTENER FOR MANUAL ASSIGNMENTS ---
// This listener handles BOTH primary and secondary staff assignments:
// - When a lead is first assigned (primary staff) â†’ sends notification
// - When a 2nd, 3rd, etc. staff is added later (secondary staff) â†’ sends notification
// - Works for both AI auto-assignments and manual assignments from CRM UI
let retryCount = 0;
const MAX_RETRIES = 10; // Limit retries to prevent infinite loops

const listenForManualAssignments = () => {
  // Prevent infinite retry loops
  if (retryCount >= MAX_RETRIES) {
    console.error(
      `[Realtime] âš ï¸ Max retries (${MAX_RETRIES}) reached for lead assignee subscription. Stopping retries.`
    );
    retryCount = 0; // Reset after a delay
    setTimeout(() => {
      retryCount = 0;
      console.log(
        "[Realtime] Retry counter reset. Will attempt subscription again on next trigger."
      );
    }, 60000); // Reset after 1 minute
    return null;
  }

  const channel = supabase.channel("manual-lead-assignee-changes");
  channel
    .on(
      "postgres_changes",
      { event: "INSERT", schema: "public", table: "lead_assignees" },
      async (payload) => {
        console.log(
          "[Realtime] ðŸ”” New lead assignment detected:",
          JSON.stringify(payload.new, null, 2)
        );
        const { lead_id, staff_id } = payload.new;

        if (!lead_id || !staff_id) {
          console.error(
            "[Realtime] âŒ Invalid assignment payload - missing lead_id or staff_id:",
            payload.new
          );
          return;
        }

        try {
          // Small delay to ensure database consistency after INSERT
          await new Promise((resolve) => setTimeout(resolve, 100));

          // Check if this staff is already assigned to this lead (prevent duplicate notifications)
          // This prevents spam when staff is assigned multiple times or assignment is replayed
          const { data: existingAssignments, error: checkError } =
            await supabase
              .from("lead_assignees")
              .select("id, created_at")
              .eq("lead_id", lead_id)
              .eq("staff_id", staff_id)
              .order("created_at", { ascending: false });

          if (checkError) {
            console.error(
              `[Realtime] Error checking existing assignments: ${checkError.message}`
            );
            return;
          }

          // Get the most recent assignment
          const mostRecentAssignment = existingAssignments?.[0];
          if (!mostRecentAssignment) {
            console.log(
              `[Realtime] No assignment found for staff ${staff_id} to lead ${lead_id}. Skipping.`
            );
            return;
          }

          // Check if this is a very recent assignment (within last 10 seconds)
          // If it's older, it might be a replay event, so skip it
          const assignmentTime = new Date(mostRecentAssignment.created_at);
          const now = new Date();
          const secondsDiff = (now - assignmentTime) / 1000;

          if (secondsDiff > 10) {
            console.log(
              `[Realtime] Assignment for staff ${staff_id} to lead ${lead_id} is older than 10 seconds (${secondsDiff.toFixed(
                1
              )}s). Skipping notification (likely replay event).`
            );
            return;
          }

          // Fetch lead, its customer, and ALL its assignees with their services
          const { data: lead, error: leadError } = await supabase
            .from("leads")
            .select(
              "*, customer:customers(*), all_assignees:lead_assignees(staff(*))"
            )
            .eq("id", lead_id)
            .single();

          if (leadError || !lead) {
            console.error(
              `[Realtime] Error fetching lead details for notification: ${leadError?.message}`
            );
            return;
          }

          const newlyAssignedStaff = lead.all_assignees.find(
            (a) => a.staff.id === staff_id
          )?.staff;
          const customer = lead.customer;

          if (!newlyAssignedStaff) {
            console.error(
              `[Realtime] âŒ Could not find newly assigned staff for lead ${lead_id}. Staff ID: ${staff_id}, Found assignees: ${
                lead.all_assignees?.length || 0
              }, Assignee IDs: ${
                lead.all_assignees?.map((a) => a.staff?.id).join(", ") || "none"
              }`
            );
            await logLeadActivity(
              lead_id,
              "WhatsApp Failed",
              `Failed to find staff member (ID: ${staff_id}) for assignment notification.`,
              "System"
            );
            return;
          }

          if (!customer) {
            console.error(
              `[Realtime] âŒ Could not find customer for lead ${lead_id}. Customer ID: ${lead.customer_id}`
            );
            await logLeadActivity(
              lead_id,
              "WhatsApp Failed",
              `Failed to find customer (ID: ${lead.customer_id}) for assignment notification.`,
              "System"
            );
            return;
          }

          console.log(
            `[Realtime] âœ… Found staff: ${newlyAssignedStaff.name} (Phone: ${newlyAssignedStaff.phone}), Customer: ${customer.first_name} ${customer.last_name}`
          );

          // Check if we've already sent a notification for this assignment recently
          // by checking the lead's activity log (check by staff name, not ID)
          const recentNotification = (lead.activity || []).find(
            (act) =>
              act.type === "Summary Sent to Staff" &&
              act.description?.includes(newlyAssignedStaff.name) &&
              new Date(act.timestamp) > new Date(Date.now() - 60000) // Last 60 seconds
          );

          if (recentNotification) {
            console.log(
              `[Realtime] Notification already sent to ${newlyAssignedStaff.name} for lead ${lead_id} in last 60 seconds. Skipping duplicate.`
            );
            return;
          }

          if (lead.all_assignees.length === 1) {
            // If this is the only assignee, they are the primary.
            console.log(
              `[Realtime] Primary staff assigned. Checking if MTS summary needs to be sent for lead ${lead.id}`
            );

            // Check if summary was already sent (prevent duplicates)
            const recentSummarySent = (lead.activity || []).some(
              (act) =>
                (act.type === "Summary Sent" || act.type === "WhatsApp Sent") &&
                (act.description?.includes("Summary sent") ||
                  act.description?.includes("template")) &&
                new Date(act.timestamp) > new Date(Date.now() - 60000) // Last 60 seconds
            );

            if (!recentSummarySent) {
              // DISABLED: MTS summary auto-sending
              // console.log(
              //   `[Realtime] Sending MTS summary to customer "${customer.first_name} ${customer.last_name}" (${customer.phone}) with assigned staff (${newlyAssignedStaff.name}) for lead ${lead.id}`
              // );
              // try {
              //   await sendWelcomeWhatsapp(lead, customer, newlyAssignedStaff);
              //   console.log(
              //     `[Realtime] âœ… MTS summary sent successfully to customer for lead ${lead.id}`
              //   );
              // } catch (summaryError) {
              //   console.error(
              //     `[Realtime] âŒ Error sending MTS summary to customer for lead ${lead.id}:`,
              //     summaryError.message,
              //     summaryError.stack
              //   );
              //   // Log error to lead activity
              //   await logLeadActivity(
              //     lead.id,
              //     "WhatsApp Failed",
              //     `Failed to send MTS summary to customer: ${summaryError.message}`,
              //     "System"
              //   );
              // }
              console.log(
                `[Realtime] MTS summary auto-sending is disabled for lead ${lead.id}`
              );
            } else {
              console.log(
                `[Realtime] Summary already sent recently for lead ${lead.id}. Skipping duplicate.`
              );
            }

            // Send staff notification
            await sendStaffAssignmentNotification(
              lead,
              customer,
              newlyAssignedStaff,
              "primary"
            );
          } else {
            // Multiple assignees exist. Assume this new one is secondary.
            // The first person on the list is conventionally the primary.
            const primaryAssignee = lead.all_assignees[0]?.staff;
            if (!primaryAssignee) {
              console.error(
                `[Realtime] Could not determine a primary assignee for lead ${lead.id}.`
              );
              return;
            }

            // To provide a helpful message, infer the specific service this new staff member is likely responsible for.
            const primaryServices = new Set(primaryAssignee.services || []);
            const secondaryServices = new Set(
              newlyAssignedStaff.services || []
            );
            const leadServices = new Set(lead.services || []);

            let specificService = null;
            // Find a service the new staff handles, that the lead requires, and the primary staff does NOT handle.
            for (const service of secondaryServices) {
              if (leadServices.has(service) && !primaryServices.has(service)) {
                specificService = service;
                break;
              }
            }
            // Fallback: just find any service they handle that's on the lead.
            if (!specificService) {
              specificService =
                (newlyAssignedStaff.services || []).find((s) =>
                  leadServices.has(s)
                ) || "a task for this lead";
            }

            console.log(
              `[Realtime] Sending SECONDARY assignment notification to ${newlyAssignedStaff.name} (ID: ${newlyAssignedStaff.id}, Phone: ${newlyAssignedStaff.phone}) for lead ${lead.id}`
            );
            try {
              await sendStaffAssignmentNotification(
                lead,
                customer,
                newlyAssignedStaff,
                "secondary",
                primaryAssignee.name,
                specificService
              );
            } catch (notifError) {
              console.error(
                `[Realtime] Error sending secondary assignment notification:`,
                notifError.message
              );
              await logLeadActivity(
                lead.id,
                "WhatsApp Failed",
                `Failed to send assignment notification to staff "${newlyAssignedStaff.name}": ${notifError.message}`,
                "System"
              );
            }
          }
        } catch (error) {
          const errorMessage =
            error?.message ||
            error?.toString() ||
            JSON.stringify(error) ||
            "Unknown error";
          const errorStack = error?.stack || "No stack trace";
          console.error(
            "[Realtime] âŒ Error processing manual assignment notification:",
            errorMessage,
            errorStack
          );
          // Log to lead activity if we have lead_id
          if (payload?.new?.lead_id) {
            try {
              await logLeadActivity(
                payload.new.lead_id,
                "WhatsApp Failed",
                `Error processing assignment notification: ${error.message}. Check server logs for details.`,
                "System"
              );
            } catch (logError) {
              console.error(
                "[Realtime] Failed to log error to activity:",
                logError.message
              );
            }
          }
        }
      }
    )
    .subscribe((status, err) => {
      if (status === "SUBSCRIBED") {
        console.log(
          "[Realtime] âœ… Listening for manual lead assignments to send notifications."
        );
        retryCount = 0; // Reset on success
      } else {
        retryCount++;
        const errorMessage =
          err?.message ||
          err?.toString() ||
          JSON.stringify(err) ||
          "Unknown error";

        // Only log errors that aren't the known "mismatch" error (to reduce noise)
        if (
          !errorMessage.includes("mismatch between server and client bindings")
        ) {
          const isUnknownRealtime = /unknown error|Unknown error/i.test(
            errorMessage
          );
          if (isUnknownRealtime && retryCount <= 2) {
            console.warn(
              `[Realtime] Could not subscribe to lead_assignees (attempt ${retryCount}/${MAX_RETRIES}). If Realtime is not enabled for this table in Supabase, assignment notifications will be skipped. Error:`,
              errorMessage
            );
          } else if (!isUnknownRealtime) {
            console.error(
              `[Realtime] Failed to subscribe to lead assignee changes (attempt ${retryCount}/${MAX_RETRIES}):`,
              errorMessage
            );
          } else {
            // Log mismatch error less frequently (every 5th attempt)
            if (retryCount % 5 === 0) {
              console.warn(
                `[Realtime] âš ï¸ Realtime subscription mismatch error (attempt ${retryCount}/${MAX_RETRIES}). This is a known Supabase issue and may resolve automatically.`
              );
            }
          }

          // Retry with exponential backoff (5s, 10s, 20s, etc., max 30s)
          const retryDelay = Math.min(
            5000 * Math.pow(2, retryCount - 1),
            30000
          );
          setTimeout(() => {
            if (retryCount < MAX_RETRIES) {
              if (retryCount <= 2 || retryCount % 3 === 0) {
                console.log(
                  `[Realtime] Retrying subscription to lead assignee changes... (attempt ${
                    retryCount + 1
                  }/${MAX_RETRIES})`
                );
              }
              listenForManualAssignments();
            }
          }, retryDelay);
        }
      }
    });
  return channel;
};

// Phone status check endpoint is now in whatsapp-crm.js

// --- JOB APPLICANTS ENDPOINTS ---

// Create a new job applicant (public endpoint for form submission)
// Handle OPTIONS preflight request - MUST be before POST route
// ALLOW ALL ORIGINS for this public form endpoint
app.options("/api/job-applicants", (req, res) => {
  res.header("Access-Control-Allow-Origin", "*");
  res.header("Access-Control-Allow-Methods", "POST, OPTIONS");
  res.header("Access-Control-Allow-Headers", "Content-Type");
  res.sendStatus(200);
});

// Middleware to set CORS headers - ALLOW ALL for job applicants endpoint
const setCorsHeaders = (req, res, next) => {
  res.header("Access-Control-Allow-Origin", "*");
  res.header("Access-Control-Allow-Methods", "POST, OPTIONS");
  res.header("Access-Control-Allow-Headers", "Content-Type");
  next();
};

// Support both FormData (multer) and JSON (base64) formats
app.post("/api/job-applicants", setCorsHeaders, async (req, res) => {
  try {
    // Check if request is JSON (with base64 file) or FormData
    const isJsonRequest =
      req.headers["content-type"]?.includes("application/json");

    let fileToUpload = null;
    let fileNameToUse = null;
    let fileTypeToUse = null;

    if (isJsonRequest) {
      // Handle JSON request with base64 file (like dynamic-whatsapp-form.html)
      const { resume_file, resume_file_name, resume_file_type } = req.body;

      if (resume_file) {
        try {
          // Convert base64 to buffer
          const fileBuffer = Buffer.from(resume_file, "base64");
          fileToUpload = fileBuffer;
          fileNameToUse = resume_file_name || "resume.pdf";
          fileTypeToUse = resume_file_type || "application/pdf";
        } catch (err) {
          return res.status(400).json({
            message:
              "Invalid file data. Please ensure the file is properly encoded.",
          });
        }
      }
    } else {
      // Handle FormData request (multer) - use middleware approach
      return resumeUpload.single("resume")(req, res, async (err) => {
        if (err) {
          if (err instanceof multer.MulterError) {
            if (err.code === "LIMIT_FILE_SIZE") {
              console.error(
                "[Job Applicants] âŒ File size error:",
                err.message
              );
              return res.status(413).json({
                message: "File size too large. Maximum size is 10MB.",
              });
            }
            return res.status(400).json({
              message: `File upload error: ${err.message}`,
            });
          }
          return res.status(400).json({
            message: err.message || "File upload error",
          });
        }
        // Process FormData request
        fileToUpload = req.file?.buffer;
        fileNameToUse = req.file?.originalname;
        fileTypeToUse = req.file?.mimetype;
        await processApplication();
      });
    }

    // Process JSON request
    await processApplication();

    async function processApplication() {
      try {
        const {
          first_name,
          last_name,
          email,
          phone,
          educational_qualification,
          experience_level,
          brief_about_yourself,
          role_applied_for,
        } = req.body;

        // Log form submission
        console.log(`[Job Applicants] ðŸ“‹ New job application form submitted`);
        console.log(`[Job Applicants] Name: ${first_name} ${last_name}`);
        console.log(
          `[Job Applicants] Role Applied For: ${
            role_applied_for || "Not specified"
          }`
        );
        console.log(`[Job Applicants] Email: ${email}`);
        console.log(`[Job Applicants] Phone: ${phone}`);

        // Validate required fields
        if (
          !first_name ||
          !last_name ||
          !email ||
          !phone ||
          !role_applied_for ||
          !experience_level ||
          !fileToUpload
        ) {
          return res.status(400).json({
            message:
              "Missing required fields: first_name, last_name, email, phone, role_applied_for, experience_level, and resume are required.",
          });
        }

        // Validate experience_level
        if (!["Fresher", "Experienced"].includes(experience_level)) {
          return res.status(400).json({
            message:
              "Invalid experience_level. Must be 'Fresher' or 'Experienced'.",
          });
        }

        // Upload resume file to Supabase storage
        let resumeUrl = null;
        let finalResumeFileName = null;
        let finalResumeFileType = null;

        if (fileToUpload) {
          try {
            // Determine file extension from file name
            const fileExt = fileNameToUse.split(".").pop().toLowerCase();
            const fileName = `job-applicants/${Math.random()
              .toString(36)
              .substring(7)}_${Date.now()}.${fileExt}`;

            // fileToUpload is already a Buffer for both JSON and FormData cases
            const fileBuffer = Buffer.isBuffer(fileToUpload)
              ? fileToUpload
              : Buffer.from(fileToUpload);

            // Upload to Supabase storage (using avatars bucket as it's already configured)
            const { error: uploadError } = await supabase.storage
              .from("avatars")
              .upload(fileName, fileBuffer, {
                contentType: fileTypeToUse,
              });

            if (uploadError) throw uploadError;

            // Get public URL
            const {
              data: { publicUrl },
            } = supabase.storage.from("avatars").getPublicUrl(fileName);

            resumeUrl = publicUrl;
            finalResumeFileName = fileNameToUse;
            finalResumeFileType = fileTypeToUse;
            console.log(
              `[Job Applicants] âœ… Resume uploaded successfully: ${fileName}`
            );
          } catch (uploadErr) {
            console.error(
              "[Job Applicants] âŒ Resume upload error:",
              uploadErr
            );
            return res.status(500).json({
              message: `Failed to upload resume: ${uploadErr.message}`,
            });
          }
        }

        // Create job applicant record
        const newApplicant = {
          first_name,
          last_name,
          email,
          phone,
          educational_qualification: educational_qualification || null,
          experience_level,
          brief_about_yourself: brief_about_yourself || null,
          resume_url: resumeUrl,
          resume_file_name: finalResumeFileName,
          resume_file_type: finalResumeFileType,
          role_applied_for: role_applied_for || null,
          status: "Applied",
          activity: [
            {
              id: Date.now(),
              type: "Application Submitted",
              description: "Application submitted via website form",
              user: "System",
              timestamp: new Date().toISOString(),
            },
          ],
          created_at: new Date().toISOString(),
          updated_at: new Date().toISOString(),
        };

        const { data: createdApplicant, error: applicantError } = await supabase
          .from("job_applicants")
          .insert(newApplicant)
          .select()
          .single();

        if (applicantError) throw applicantError;

        console.log(
          `[Job Applicants] âœ… Application created successfully with ID: ${createdApplicant.id}`
        );

        res.status(201).json({
          message: "Application submitted successfully.",
          applicant: createdApplicant,
        });
      } catch (error) {
        console.error(
          "[Job Applicants] âŒ Error in processApplication:",
          error
        );
        res.header("Access-Control-Allow-Origin", "*");
        return res.status(500).json({
          message: error.message || "An internal server error occurred.",
        });
      }
    } // End processApplication function
  } catch (error) {
    console.error("[Job Applicants] âŒ Error creating job applicant:", error);

    // Ensure CORS headers are set even on error (allow all)
    res.header("Access-Control-Allow-Origin", "*");
    res.header("Access-Control-Allow-Methods", "POST, OPTIONS");
    res.header("Access-Control-Allow-Headers", "Content-Type");

    res.status(500).json({
      message: error.message || "An internal server error occurred.",
    });
  }
});

// Error handler for multer file size errors (must be after routes)
app.use((error, req, res, next) => {
  // Set CORS headers for error responses - ALLOW ALL for job applicants endpoint
  if (req.path === "/api/job-applicants") {
    res.header("Access-Control-Allow-Origin", "*");
  } else {
    // For other endpoints, use normal CORS
    const origin = req.headers.origin;
    if (origin && origin.includes("jeppiaaracademy.com")) {
      res.header("Access-Control-Allow-Origin", origin);
    } else if (
      origin &&
      (origin.includes("crm.jeppiaaracademy.com") ||
        origin.includes("jeppiaaracademy.com"))
    ) {
      res.header("Access-Control-Allow-Origin", origin);
    } else if (origin && allowedOrigins.includes(origin)) {
      res.header("Access-Control-Allow-Origin", origin);
    } else if (origin) {
      res.header("Access-Control-Allow-Origin", origin);
    } else {
      res.header("Access-Control-Allow-Origin", "*");
    }
  }
  res.header("Access-Control-Allow-Credentials", "true");
  res.header("Access-Control-Allow-Methods", "POST, OPTIONS");
  res.header("Access-Control-Allow-Headers", "Content-Type");

  if (error instanceof multer.MulterError) {
    console.error("[Job Applicants] âŒ Multer error:", error);
    if (error.code === "LIMIT_FILE_SIZE") {
      return res.status(413).json({
        message: "File size too large. Maximum size is 10MB.",
      });
    }
    return res.status(400).json({
      message: `File upload error: ${error.message}`,
    });
  }

  // Handle other errors
  if (error.message && error.message.includes("File")) {
    return res.status(400).json({
      message: error.message,
    });
  }

  // If headers haven't been sent, send error response
  if (!res.headersSent) {
    res.status(500).json({
      message: error.message || "An internal server error occurred.",
    });
  } else {
    next(error);
  }
});

// Get all job applicants (Super Admin, Office Admin/Manager, or Manager with is_lead_manager)
app.get("/api/job-applicants", requireAuth, async (req, res) => {
  try {
    const currentUser = req.user;

    // Check if user is Super Admin, Office Admin (Manager), or Manager with is_lead_manager
    const hasAccess =
      currentUser.role_id === 1 ||
      currentUser.role_id === 2 ||
      currentUser.is_lead_manager === true;
    if (!hasAccess) {
      return res.status(403).json({
        message:
          "Access denied. Only Super Admin, Office Admin, or Managers with Manage Leads permission can view job applicants.",
      });
    }

    const { data: applicants, error } = await supabase
      .from("job_applicants")
      .select("*")
      .order("created_at", { ascending: false });

    if (error) throw error;

    res.json(applicants || []);
  } catch (error) {
    console.error("Error fetching job applicants:", error);
    res.status(500).json({
      message: error.message || "Failed to fetch job applicants.",
    });
  }
});

// Get a single job applicant by ID
app.get("/api/job-applicants/:id", requireAuth, async (req, res) => {
  try {
    const currentUser = req.user;

    // Check if user is Super Admin, Office Admin (Manager), or Manager with is_lead_manager
    const hasAccess =
      currentUser.role_id === 1 ||
      currentUser.role_id === 2 ||
      currentUser.is_lead_manager === true;
    if (!hasAccess) {
      return res.status(403).json({
        message:
          "Access denied. Only Super Admin, Office Admin, or Managers with Manage Leads permission can view job applicants.",
      });
    }

    const { id } = req.params;
    const { data: applicant, error } = await supabase
      .from("job_applicants")
      .select("*")
      .eq("id", id)
      .single();

    if (error) throw error;
    if (!applicant) {
      return res.status(404).json({ message: "Applicant not found." });
    }

    res.json(applicant);
  } catch (error) {
    console.error("Error fetching job applicant:", error);
    res.status(500).json({
      message: error.message || "Failed to fetch job applicant.",
    });
  }
});

// Update job applicant (approve, reject, update status, etc.)
app.put("/api/job-applicants/:id", requireAuth, async (req, res) => {
  try {
    const currentUser = req.user;

    // Check if user is Super Admin, Office Admin (Manager), or Manager with is_lead_manager
    const hasAccess =
      currentUser.role_id === 1 ||
      currentUser.role_id === 2 ||
      currentUser.is_lead_manager === true;
    if (!hasAccess) {
      return res.status(403).json({
        message:
          "Access denied. Only Super Admin, Office Admin, or Managers with Manage Leads permission can update job applicants.",
      });
    }

    const { id } = req.params;
    const {
      status,
      approval_reason,
      rejection_reason,
      role_applied_for,
      first_name,
      last_name,
      email,
      phone,
      educational_qualification,
      experience_level,
      brief_about_yourself,
      activity,
    } = req.body;

    // Get current applicant
    const { data: currentApplicant, error: fetchError } = await supabase
      .from("job_applicants")
      .select("*")
      .eq("id", id)
      .single();

    if (fetchError) throw fetchError;
    if (!currentApplicant) {
      return res.status(404).json({ message: "Applicant not found." });
    }

    // Build update object
    const updateData = {
      updated_at: new Date().toISOString(),
    };

    if (status !== undefined) {
      updateData.status = status;

      // Set approval/rejection fields based on status
      if (status === "Approved") {
        updateData.approved_by_staff_id = currentUser.id;
        updateData.approval_reason = approval_reason || null;
        updateData.rejected_by_staff_id = null;
        updateData.rejection_reason = null;
      } else if (status === "Rejected") {
        updateData.rejected_by_staff_id = currentUser.id;
        updateData.rejection_reason = rejection_reason || null;
        updateData.approved_by_staff_id = null;
        updateData.approval_reason = null;
      }
    }

    if (role_applied_for !== undefined)
      updateData.role_applied_for = role_applied_for;
    if (first_name !== undefined) updateData.first_name = first_name;
    if (last_name !== undefined) updateData.last_name = last_name;
    if (email !== undefined) updateData.email = email;
    if (phone !== undefined) updateData.phone = phone;
    if (educational_qualification !== undefined)
      updateData.educational_qualification = educational_qualification;
    if (experience_level !== undefined)
      updateData.experience_level = experience_level;
    if (brief_about_yourself !== undefined)
      updateData.brief_about_yourself = brief_about_yourself;
    if (activity !== undefined) updateData.activity = activity;

    const { data: updatedApplicant, error: updateError } = await supabase
      .from("job_applicants")
      .update(updateData)
      .eq("id", id)
      .select()
      .single();

    if (updateError) throw updateError;

    console.log(
      `[Job Applicants] Applicant ${id} updated by ${currentUser.name}`
    );

    res.json({
      message: "Applicant updated successfully.",
      applicant: updatedApplicant,
    });
  } catch (error) {
    console.error("Error updating job applicant:", error);
    res.status(500).json({
      message: error.message || "Failed to update job applicant.",
    });
  }
});

// Delete job applicant (Super Admin only)
app.delete("/api/job-applicants/:id", requireAuth, async (req, res) => {
  try {
    const currentUser = req.user;

    // Only Super Admin can delete
    if (currentUser.role_id !== 1) {
      return res.status(403).json({
        message: "Access denied. Only Super Admin can delete job applicants.",
      });
    }

    const { id } = req.params;

    // Get applicant to delete resume file if exists
    const { data: applicant, error: fetchError } = await supabase
      .from("job_applicants")
      .select("resume_url")
      .eq("id", id)
      .single();

    if (fetchError && fetchError.code !== "PGRST116") throw fetchError;

    // Delete resume file from storage if exists
    if (applicant?.resume_url) {
      try {
        // Extract file path from URL
        const urlParts = applicant.resume_url.split("/");
        const fileName = urlParts[urlParts.length - 1].split("?")[0];
        const filePath = `job-applicants/${fileName}`;

        const { error: deleteError } = await supabase.storage
          .from("avatars")
          .remove([filePath]);

        if (deleteError) {
          console.warn(
            `[Job Applicants] Failed to delete resume file: ${deleteError.message}`
          );
        }
      } catch (fileErr) {
        console.warn(
          `[Job Applicants] Error deleting resume file: ${fileErr.message}`
        );
      }
    }

    const { error: deleteError } = await supabase
      .from("job_applicants")
      .delete()
      .eq("id", id);

    if (deleteError) throw deleteError;

    console.log(
      `[Job Applicants] Applicant ${id} deleted by ${currentUser.name}`
    );

    res.json({ message: "Applicant deleted successfully." });
  } catch (error) {
    console.error("Error deleting job applicant:", error);
    res.status(500).json({
      message: error.message || "Failed to delete job applicant.",
    });
  }
});

// --- SUB-AGENT REGISTRATIONS API (Super Admin & Office Admin) ---
const hasSubAgentRegAccess = (currentUser) =>
  currentUser.role_id === 1 ||
  currentUser.role_id === 2 ||
  currentUser.is_lead_manager === true;

// In-memory CAPTCHA store for public sub-agent registration form (WordPress). id -> { code, expires }
const subAgentCaptchaStore = new Map();
const CAPTCHA_TTL_MS = 2 * 60 * 1000; // 2 minutes

function generateSubAgentCaptchaCode() {
  const chars = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789";
  let code = "";
  for (let i = 0; i < 5; i++) {
    code += chars.charAt(Math.floor(Math.random() * chars.length));
  }
  return code;
}

// Public: get a new CAPTCHA for sub-agent registration form (WordPress)
app.get(
  "/api/captcha/sub-agent",
  (req, res, next) => {
    res.header("Access-Control-Allow-Origin", "*");
    next();
  },
  (req, res) => {
    const id = `sa_${Date.now()}_${Math.random().toString(36).slice(2, 10)}`;
    const code = generateSubAgentCaptchaCode();
    subAgentCaptchaStore.set(id, {
      code,
      expires: Date.now() + CAPTCHA_TTL_MS,
    });
    // Clean old entries
    for (const [k, v] of subAgentCaptchaStore.entries()) {
      if (v.expires < Date.now()) subAgentCaptchaStore.delete(k);
    }
    res.json({ id, code });
  }
);

app.options("/api/sub-agent-registrations/public", (req, res) => {
  res.header("Access-Control-Allow-Origin", "*");
  res.header("Access-Control-Allow-Methods", "POST, OPTIONS");
  res.header("Access-Control-Allow-Headers", "Content-Type");
  res.sendStatus(200);
});

// Public: create sub-agent registration from WordPress form (with CAPTCHA + terms)
app.post(
  "/api/sub-agent-registrations/public",
  setCorsHeaders,
  async (req, res) => {
    try {
      const {
        captcha_id,
        captcha_value,
        terms_accepted,
        company_name,
        pan_number,
        do_not_have_pan,
        package: pkg,
        first_name_middle,
        last_name,
        email,
        mobile,
        sales_in_charge_id,
        gst_number,
        gst_name,
        gst_address,
        street,
        pin_code,
        country,
        state,
        city,
      } = req.body;

      if (!captcha_id || !captcha_value) {
        return res.status(400).json({
          message:
            "CAPTCHA is required. Please complete the CAPTCHA and try again.",
        });
      }
      const stored = subAgentCaptchaStore.get(captcha_id);
      if (!stored) {
        return res.status(400).json({
          message:
            "CAPTCHA expired or invalid. Please refresh the page and try again.",
        });
      }
      if (stored.expires < Date.now()) {
        subAgentCaptchaStore.delete(captcha_id);
        return res.status(400).json({
          message: "CAPTCHA expired. Please refresh the page and try again.",
        });
      }
      if (String(captcha_value).trim().toUpperCase() !== stored.code) {
        return res.status(400).json({
          message: "CAPTCHA code does not match. Please try again.",
        });
      }
      subAgentCaptchaStore.delete(captcha_id);

      if (!terms_accepted) {
        return res.status(400).json({
          message:
            "You must accept the Terms and Conditions of the Service Agreement.",
        });
      }

      if (
        !company_name ||
        !first_name_middle ||
        !last_name ||
        !email ||
        !mobile ||
        !street ||
        !pin_code
      ) {
        return res.status(400).json({
          message:
            "Required fields: company_name, first_name_middle, last_name, email, mobile, street, pin_code.",
        });
      }

      const insertRow = {
        company_name,
        pan_number: pan_number || null,
        do_not_have_pan: Boolean(do_not_have_pan),
        package: pkg || "Monthly Package",
        first_name_middle,
        last_name,
        email,
        mobile,
        sales_in_charge_id: sales_in_charge_id
          ? parseInt(sales_in_charge_id, 10)
          : null,
        gst_number: gst_number || null,
        gst_name: gst_name || null,
        gst_address: gst_address || null,
        street,
        pin_code,
        country: country || "",
        state: state || "",
        city: city || "",
        terms_accepted: true,
        status: "Enquiry",
      };

      const { data: created, error } = await supabase
        .from("sub_agent_registrations")
        .insert(insertRow)
        .select()
        .single();

      if (error) throw error;

      // Notify CRM to refresh: broadcast so DataProvider can refetch (if Supabase Realtime broadcast from server is used)
      try {
        const channel = supabase.channel("crm-updates");
        await channel.send({
          type: "broadcast",
          event: "new-sub-agent-registration",
          payload: {},
        });
      } catch (broadcastErr) {
        // Ignore if broadcast not supported from server
      }

      res.status(201).json(created);
    } catch (error) {
      console.error("Error creating sub-agent registration (public):", error);
      res.status(500).json({
        message: error.message || "Failed to submit registration.",
      });
    }
  }
);

app.get("/api/sub-agent-registrations", requireAuth, async (req, res) => {
  try {
    const currentUser = req.user;
    if (!hasSubAgentRegAccess(currentUser)) {
      return res.status(403).json({
        message:
          "Access denied. Only Super Admin and Office Admin can view sub-agent registrations.",
      });
    }

    const { data: rows, error } = await supabase
      .from("sub_agent_registrations")
      .select("*")
      .order("created_at", { ascending: false });

    if (error) throw error;
    res.json(rows || []);
  } catch (error) {
    console.error("Error fetching sub-agent registrations:", error);
    res.status(500).json({
      message: error.message || "Failed to fetch sub-agent registrations.",
    });
  }
});

app.get("/api/sub-agent-registrations/:id", requireAuth, async (req, res) => {
  try {
    const currentUser = req.user;
    if (!hasSubAgentRegAccess(currentUser)) {
      return res.status(403).json({
        message:
          "Access denied. Only Super Admin and Office Admin can view sub-agent registrations.",
      });
    }

    const { id } = req.params;
    const { data: row, error } = await supabase
      .from("sub_agent_registrations")
      .select("*")
      .eq("id", id)
      .single();

    if (error) throw error;
    if (!row) {
      return res
        .status(404)
        .json({ message: "Sub-agent registration not found." });
    }
    res.json(row);
  } catch (error) {
    console.error("Error fetching sub-agent registration:", error);
    res.status(500).json({
      message: error.message || "Failed to fetch sub-agent registration.",
    });
  }
});

app.post("/api/sub-agent-registrations", requireAuth, async (req, res) => {
  try {
    const currentUser = req.user;
    if (!hasSubAgentRegAccess(currentUser)) {
      return res.status(403).json({
        message:
          "Access denied. Only Super Admin and Office Admin can create sub-agent registrations.",
      });
    }

    const {
      company_name,
      pan_number,
      do_not_have_pan,
      package: pkg,
      first_name_middle,
      last_name,
      email,
      mobile,
      sales_in_charge_id,
      gst_number,
      gst_name,
      gst_address,
      street,
      pin_code,
      country,
      state,
      city,
      terms_accepted,
      status,
    } = req.body;

    if (
      !company_name ||
      !first_name_middle ||
      !last_name ||
      !email ||
      !mobile ||
      !street ||
      !pin_code
    ) {
      return res.status(400).json({
        message:
          "Required fields: company_name, first_name_middle, last_name, email, mobile, street, pin_code.",
      });
    }

    const insertRow = {
      company_name,
      pan_number: pan_number || null,
      do_not_have_pan: Boolean(do_not_have_pan),
      package: pkg || "Monthly Package",
      first_name_middle,
      last_name,
      email,
      mobile,
      sales_in_charge_id: sales_in_charge_id
        ? parseInt(sales_in_charge_id, 10)
        : null,
      gst_number: gst_number || null,
      gst_name: gst_name || null,
      gst_address: gst_address || null,
      street,
      pin_code,
      country: country || "",
      state: state || "",
      city: city || "",
      terms_accepted: Boolean(terms_accepted),
      status: status || "Enquiry",
    };

    const { data: created, error } = await supabase
      .from("sub_agent_registrations")
      .insert(insertRow)
      .select()
      .single();

    if (error) throw error;
    res.status(201).json(created);
  } catch (error) {
    console.error("Error creating sub-agent registration:", error);
    res.status(500).json({
      message: error.message || "Failed to create sub-agent registration.",
    });
  }
});

app.put("/api/sub-agent-registrations/:id", requireAuth, async (req, res) => {
  try {
    const currentUser = req.user;
    if (!hasSubAgentRegAccess(currentUser)) {
      return res.status(403).json({
        message:
          "Access denied. Only Super Admin and Office Admin can update sub-agent registrations.",
      });
    }

    const { id } = req.params;
    const body = req.body;

    const allowed = [
      "company_name",
      "pan_number",
      "do_not_have_pan",
      "package",
      "first_name_middle",
      "last_name",
      "email",
      "mobile",
      "sales_in_charge_id",
      "gst_number",
      "gst_name",
      "gst_address",
      "street",
      "pin_code",
      "country",
      "state",
      "city",
      "terms_accepted",
      "status",
    ];
    const updateData = {};
    for (const key of allowed) {
      if (body[key] !== undefined) {
        if (key === "do_not_have_pan" || key === "terms_accepted") {
          updateData[key] = Boolean(body[key]);
        } else if (key === "sales_in_charge_id") {
          updateData[key] = body[key] ? parseInt(body[key], 10) : null;
        } else {
          updateData[key] = body[key];
        }
      }
    }

    const { data: updated, error } = await supabase
      .from("sub_agent_registrations")
      .update(updateData)
      .eq("id", id)
      .select()
      .single();

    if (error) throw error;
    if (!updated) {
      return res
        .status(404)
        .json({ message: "Sub-agent registration not found." });
    }
    res.json(updated);
  } catch (error) {
    console.error("Error updating sub-agent registration:", error);
    res.status(500).json({
      message: error.message || "Failed to update sub-agent registration.",
    });
  }
});

// Delete sub-agent registration (Super Admin only)
app.delete(
  "/api/sub-agent-registrations/:id",
  requireAuth,
  async (req, res) => {
    try {
      const currentUser = req.user;
      if (currentUser.role_id !== 1) {
        return res.status(403).json({
          message:
            "Access denied. Only Super Admin can delete sub-agent registrations.",
        });
      }

      const { id } = req.params;
      const { data: deleted, error } = await supabase
        .from("sub_agent_registrations")
        .delete()
        .eq("id", id)
        .select()
        .single();

      if (error) throw error;
      if (!deleted) {
        return res
          .status(404)
          .json({ message: "Sub-agent registration not found." });
      }
      res.status(200).json(deleted);
    } catch (error) {
      console.error("Error deleting sub-agent registration:", error);
      res.status(500).json({
        message: error.message || "Failed to delete sub-agent registration.",
      });
    }
  }
);

// --- VISAS API ENDPOINTS ---

// Get all visas - ALL STAFF CAN VIEW
app.get("/api/visas", requireAuth, async (req, res) => {
  try {
    const currentUser = req.user;
    // All authenticated staff can view visas

    const { data: visas, error } = await supabase
      .from("visas")
      .select("*")
      .order("visa_name", { ascending: true });

    if (error) throw error;

    // Parse arrays - JSONB returns as arrays, but handle backward compatibility for TEXT columns
    const parsedVisas = (visas || []).map((visa) => {
      // If it's already an array (JSONB), use it directly
      // If it's a string (old TEXT format), parse or convert to array
      if (!Array.isArray(visa.visa_category)) {
        if (visa.visa_category && typeof visa.visa_category === "string") {
          try {
            visa.visa_category = JSON.parse(visa.visa_category);
          } catch (e) {
            // If not JSON, convert single value to array
            visa.visa_category = visa.visa_category ? [visa.visa_category] : [];
          }
        } else {
          visa.visa_category = [];
        }
      }
      if (!Array.isArray(visa.visa_format)) {
        if (visa.visa_format && typeof visa.visa_format === "string") {
          try {
            visa.visa_format = JSON.parse(visa.visa_format);
          } catch (e) {
            // If not JSON, convert single value to array
            visa.visa_format = visa.visa_format ? [visa.visa_format] : [];
          }
        } else {
          visa.visa_format = [];
        }
      }
      return visa;
    });

    res.json(parsedVisas);
  } catch (error) {
    console.error("Error fetching visas:", error);
    res.status(500).json({
      message: error.message || "Failed to fetch visas.",
    });
  }
});

// Download Excel template for bulk visa upload (MUST be before /api/visas/:id route)
app.get("/api/visas/template", requireAuth, async (req, res) => {
  try {
    const currentUser = req.user;

    // Only Super Admin and Office Admin can download template
    if (!checkDestinationsEditAccess(currentUser)) {
      return res.status(403).json({
        message:
          "Access denied. Only Super Admin and Office Admin can download template.",
      });
    }

    const workbook = new ExcelJS.Workbook();
    const worksheet = workbook.addWorksheet("Visa Template");

    // Define columns
    worksheet.columns = [
      { header: "Visa Name*", key: "visa_name", width: 30 },
      {
        header: "Maximum Processing Time",
        key: "maximum_processing_time",
        width: 25,
      },
      {
        header: "Duration of Stay (Length of Stay)",
        key: "duration_of_stay",
        width: 25,
      },
      { header: "Type of Visa", key: "type_of_visa", width: 20 },
      { header: "Visa Format", key: "visa_format", width: 25 },
      { header: "Entry Type", key: "entry_type", width: 20 },
      { header: "Validity Period", key: "validity_period", width: 15 },
      { header: "Cost (INR)", key: "cost", width: 12 },
      {
        header: "Documents Required (comma-separated)",
        key: "documents_required",
        width: 50,
      },
    ];

    // Style header row
    worksheet.getRow(1).font = { bold: true };
    worksheet.getRow(1).fill = {
      type: "pattern",
      pattern: "solid",
      fgColor: { argb: "FFE0E0E0" },
    };

    // Add example row
    worksheet.addRow({
      visa_name: "Example: Tourist Visa for Sri Lanka",
      maximum_processing_time: "5-7 business days",
      duration_of_stay: "30 days",
      type_of_visa: "Tourist Visa",
      visa_format: "E-Visa",
      entry_type: "Single Entry",
      validity_period: "6 months",
      cost: 5000,
      documents_required: "Passport, Photo, Application Form, Bank Statement",
    });

    res.setHeader(
      "Content-Type",
      "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    );
    res.setHeader(
      "Content-Disposition",
      'attachment; filename="Visa_Bulk_Upload_Template.xlsx"'
    );

    await workbook.xlsx.write(res);
    res.end();
  } catch (error) {
    console.error("Error generating template:", error);
    res.status(500).json({
      message: error.message || "Failed to generate template.",
    });
  }
});

// Get a single visa by ID
app.get("/api/visas/:id", requireAuth, async (req, res) => {
  try {
    const currentUser = req.user;
    // All authenticated staff can view visas

    const { id } = req.params;
    const { data: visa, error } = await supabase
      .from("visas")
      .select("*")
      .eq("id", id)
      .single();

    if (error) throw error;
    if (!visa) {
      return res.status(404).json({ message: "Visa not found." });
    }

    // Parse arrays - JSONB returns as arrays, but handle backward compatibility for TEXT columns
    if (!Array.isArray(visa.visa_category)) {
      if (visa.visa_category && typeof visa.visa_category === "string") {
        try {
          visa.visa_category = JSON.parse(visa.visa_category);
        } catch (e) {
          // If not JSON, convert single value to array
          visa.visa_category = visa.visa_category ? [visa.visa_category] : [];
        }
      } else {
        visa.visa_category = [];
      }
    }
    if (!Array.isArray(visa.visa_format)) {
      if (visa.visa_format && typeof visa.visa_format === "string") {
        try {
          visa.visa_format = JSON.parse(visa.visa_format);
        } catch (e) {
          // If not JSON, convert single value to array
          visa.visa_format = visa.visa_format ? [visa.visa_format] : [];
        }
      } else {
        visa.visa_format = [];
      }
    }

    res.json(visa);
  } catch (error) {
    console.error("Error fetching visa:", error);
    res.status(500).json({
      message: error.message || "Failed to fetch visa.",
    });
  }
});

// Create a new visa - SUPER ADMIN & OFFICE ADMIN ONLY
app.post("/api/visas", requireAuth, async (req, res) => {
  try {
    const currentUser = req.user;

    // Only Super Admin and Office Admin can create visas
    if (!checkDestinationsEditAccess(currentUser)) {
      return res.status(403).json({
        message:
          "Access denied. Only Super Admin and Office Admin can create visas.",
      });
    }

    const {
      visa_name,
      duration_of_stay,
      type_of_visa,
      visa_category,
      visa_format,
      validity_period,
      cost,
      documents_required,
    } = req.body;

    if (!visa_name || !visa_name.trim()) {
      return res.status(400).json({ message: "Visa name is required." });
    }

    // Handle arrays - convert to JSON if array, otherwise keep as is
    const visaCategoryValue = Array.isArray(visa_category)
      ? JSON.stringify(visa_category)
      : visa_category || null;
    const visaFormatValue = Array.isArray(visa_format)
      ? JSON.stringify(visa_format)
      : visa_format || null;

    const { data: newVisa, error } = await supabase
      .from("visas")
      .insert({
        visa_name: visa_name.trim(),
        maximum_processing_time: maximum_processing_time || "",
        duration_of_stay: duration_of_stay || "",
        type_of_visa: type_of_visa || "",
        visa_category: visaCategoryValue,
        visa_format: visaFormatValue,
        validity_period: validity_period || "",
        cost: cost || 0,
        documents_required: documents_required || "",
        created_by_staff_id: currentUser.id,
        created_at: new Date().toISOString(),
      })
      .select()
      .single();

    if (error) throw error;

    console.log(`[Visas] Visa created by ${currentUser.name}: ${visa_name}`);

    res.status(201).json({
      message: "Visa created successfully.",
      visa: newVisa,
    });
  } catch (error) {
    console.error("Error creating visa:", error);
    res.status(500).json({
      message: error.message || "Failed to create visa.",
    });
  }
});

// Update a visa - SUPER ADMIN & OFFICE ADMIN ONLY
app.put("/api/visas/:id", requireAuth, async (req, res) => {
  try {
    const currentUser = req.user;

    // Only Super Admin and Office Admin can update visas
    if (!checkDestinationsEditAccess(currentUser)) {
      return res.status(403).json({
        message:
          "Access denied. Only Super Admin and Office Admin can update visas.",
      });
    }

    const { id } = req.params;
    const {
      visa_name,
      maximum_processing_time,
      duration_of_stay,
      type_of_visa,
      visa_category,
      visa_format,
      validity_period,
      cost,
      documents_required,
    } = req.body;

    // Get current visa
    const { data: currentVisa, error: fetchError } = await supabase
      .from("visas")
      .select("*")
      .eq("id", id)
      .single();

    if (fetchError) throw fetchError;
    if (!currentVisa) {
      return res.status(404).json({ message: "Visa not found." });
    }

    // Build update object
    const updateData = {
      updated_at: new Date().toISOString(),
      updated_by_staff_id: currentUser.id,
    };

    if (visa_name !== undefined) updateData.visa_name = visa_name.trim();
    if (maximum_processing_time !== undefined)
      updateData.maximum_processing_time = maximum_processing_time;
    if (duration_of_stay !== undefined)
      updateData.duration_of_stay = duration_of_stay;
    if (type_of_visa !== undefined) updateData.type_of_visa = type_of_visa;
    if (visa_category !== undefined) {
      // Handle arrays - JSONB accepts arrays directly
      // Supabase will handle JSONB conversion automatically
      updateData.visa_category = Array.isArray(visa_category)
        ? visa_category
        : visa_category
        ? [visa_category]
        : null;
    }
    if (visa_format !== undefined) {
      // Handle arrays - JSONB accepts arrays directly
      // Supabase will handle JSONB conversion automatically
      updateData.visa_format = Array.isArray(visa_format)
        ? visa_format
        : visa_format
        ? [visa_format]
        : null;
    }
    if (validity_period !== undefined)
      updateData.validity_period = validity_period;
    if (cost !== undefined) updateData.cost = cost;
    if (documents_required !== undefined)
      updateData.documents_required = documents_required;

    const { data: updatedVisa, error: updateError } = await supabase
      .from("visas")
      .update(updateData)
      .eq("id", id)
      .select()
      .single();

    if (updateError) throw updateError;

    // Parse arrays - JSONB returns as arrays, but handle backward compatibility for TEXT columns
    if (!Array.isArray(updatedVisa.visa_category)) {
      if (
        updatedVisa.visa_category &&
        typeof updatedVisa.visa_category === "string"
      ) {
        try {
          updatedVisa.visa_category = JSON.parse(updatedVisa.visa_category);
        } catch (e) {
          // If not JSON, convert single value to array
          updatedVisa.visa_category = updatedVisa.visa_category
            ? [updatedVisa.visa_category]
            : [];
        }
      } else {
        updatedVisa.visa_category = [];
      }
    }
    if (!Array.isArray(updatedVisa.visa_format)) {
      if (
        updatedVisa.visa_format &&
        typeof updatedVisa.visa_format === "string"
      ) {
        try {
          updatedVisa.visa_format = JSON.parse(updatedVisa.visa_format);
        } catch (e) {
          // If not JSON, convert single value to array
          updatedVisa.visa_format = updatedVisa.visa_format
            ? [updatedVisa.visa_format]
            : [];
        }
      } else {
        updatedVisa.visa_format = [];
      }
    }

    console.log(`[Visas] Visa ${id} updated by ${currentUser.name}`);

    res.json({
      message: "Visa updated successfully.",
      visa: updatedVisa,
    });
  } catch (error) {
    console.error("Error updating visa:", error);
    res.status(500).json({
      message: error.message || "Failed to update visa.",
    });
  }
});

// Delete a visa (Super Admin only)
app.delete("/api/visas/:id", requireAuth, async (req, res) => {
  try {
    const currentUser = req.user;

    // Only Super Admin can delete
    if (currentUser.role_id !== 1) {
      return res.status(403).json({
        message: "Access denied. Only Super Admin can delete visas.",
      });
    }

    const { id } = req.params;

    const { error: deleteError } = await supabase
      .from("visas")
      .delete()
      .eq("id", id);

    if (deleteError) throw deleteError;

    console.log(`[Visas] Visa ${id} deleted by ${currentUser.name}`);

    res.json({ message: "Visa deleted successfully." });
  } catch (error) {
    console.error("Error deleting visa:", error);
    res.status(500).json({
      message: error.message || "Failed to delete visa.",
    });
  }
});

// Bulk upload visas from Excel file
const visaUpload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 10 * 1024 * 1024 }, // 10MB limit
  fileFilter: (req, file, cb) => {
    if (
      file.mimetype ===
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" ||
      file.mimetype === "application/vnd.ms-excel" ||
      file.originalname.endsWith(".xlsx") ||
      file.originalname.endsWith(".xls")
    ) {
      cb(null, true);
    } else {
      cb(new Error("Only Excel files (.xlsx, .xls) are allowed"), false);
    }
  },
});

app.post(
  "/api/visas/bulk-upload",
  requireAuth,
  visaUpload.single("file"),
  async (req, res) => {
    try {
      const currentUser = req.user;

      // Only Super Admin and Office Admin can bulk upload
      if (!checkDestinationsEditAccess(currentUser)) {
        return res.status(403).json({
          message:
            "Access denied. Only Super Admin and Office Admin can bulk upload visas.",
        });
      }

      // Handle multer errors
      if (req.fileValidationError) {
        console.error(
          "[Visas Bulk Upload] File validation error:",
          req.fileValidationError
        );
        return res.status(400).json({ message: req.fileValidationError });
      }

      if (!req.file) {
        console.error("[Visas Bulk Upload] No file in request");
        return res.status(400).json({ message: "No file uploaded." });
      }

      console.log(
        `[Visas Bulk Upload] File received: ${req.file.originalname}, size: ${req.file.size} bytes, mimetype: ${req.file.mimetype}`
      );

      let workbook;
      try {
        workbook = new ExcelJS.Workbook();
        await workbook.xlsx.load(req.file.buffer);
      } catch (error) {
        console.error("[Visas Bulk Upload] Error loading Excel file:", error);
        return res.status(400).json({
          message: `Failed to parse Excel file: ${error.message}`,
        });
      }

      const worksheet = workbook.getWorksheet(1); // Get first worksheet
      if (!worksheet) {
        return res.status(400).json({ message: "Excel file is empty." });
      }

      console.log(
        `[Visas Bulk Upload] Worksheet found: ${worksheet.name}, row count: ${worksheet.rowCount}`
      );

      const rows = [];
      const headers = {};

      // First, get all headers from row 1
      worksheet.getRow(1).eachCell((cell, colNumber) => {
        const headerValue = cell.value;
        if (headerValue) {
          headers[colNumber] = headerValue.toString().trim();
        }
      });

      console.log(`[Visas Bulk Upload] Headers found:`, Object.values(headers));

      // Then process data rows
      worksheet.eachRow((row, rowNumber) => {
        if (rowNumber === 1) return; // Skip header row

        const rowData = {};
        row.eachCell((cell, colNumber) => {
          const header = headers[colNumber];
          if (header) {
            rowData[header] = cell.value;
          }
        });

        if (Object.keys(rowData).length > 0) {
          rows.push(rowData);
        }
      });

      console.log(`[Visas Bulk Upload] Parsed ${rows.length} data rows`);

      if (rows.length === 0) {
        return res.status(400).json({
          message: "No data rows found in Excel file.",
        });
      }

      const results = {
        success: 0,
        errors: [],
      };

      // Process each row
      for (let i = 0; i < rows.length; i++) {
        const row = rows[i];
        const rowNumber = i + 2; // +2 because we skip header and 0-indexed

        try {
          // Map Excel columns to database fields
          const visaName = row["Visa Name*"] || row["Visa Name"];
          if (!visaName || !visaName.toString().trim()) {
            results.errors.push({
              row: rowNumber,
              error: "Visa Name is required",
              data: row,
            });
            continue;
          }

          // Parse Maximum Processing Time
          const maximumProcessingTime = row["Maximum Processing Time"] || "";

          // Parse Duration of Stay (Length of Stay)
          const durationOfStay =
            row["Duration of Stay (Length of Stay)"] ||
            row["Duration of Stay"] ||
            row["Length of Stay"] ||
            "";

          // Parse Visa Format (can be comma-separated or single value)
          const visaFormatValue = row["Visa Format"] || "";
          const visaFormatArray = visaFormatValue
            ? visaFormatValue
                .toString()
                .split(",")
                .map((f) => f.trim())
                .filter((f) => f.length > 0)
            : [];

          // Parse Entry Type
          const entryType = row["Entry Type"] || "";

          // Parse Documents Required (comma-separated) and convert to bullet points
          const documentsRequiredRaw =
            row["Documents Required (comma-separated)"] ||
            row["Documents Required"] ||
            "";
          let documentsRequired = "";
          if (documentsRequiredRaw) {
            const documents = documentsRequiredRaw
              .toString()
              .split(",")
              .map((d) => d.trim())
              .filter((d) => d.length > 0);
            // Convert to bullet points format
            documentsRequired = documents.map((doc) => `â€¢ ${doc}`).join("\n");
          }

          // Parse cost
          let cost = 0;
          const costValue = row["Cost (INR)"] || row["Cost"];
          if (costValue) {
            const parsedCost = parseFloat(
              costValue.toString().replace(/[^0-9.]/g, "")
            );
            if (!isNaN(parsedCost)) {
              cost = parsedCost;
            }
          }

          // Build visa_requirements with Entry Type if provided
          let visaRequirements = "";
          if (entryType) {
            visaRequirements = `Entry Type: ${entryType.toString().trim()}`;
          }

          const visaData = {
            visa_name: visaName.toString().trim(),
            maximum_processing_time: maximumProcessingTime.toString().trim(),
            duration_of_stay: durationOfStay.toString().trim(),
            type_of_visa: (row["Type of Visa"] || "").toString().trim(),
            visa_category: null, // Not in new template
            visa_format: visaFormatArray.length > 0 ? visaFormatArray : null,
            validity_period: (row["Validity Period"] || "").toString().trim(),
            cost: cost,
            documents_required: documentsRequired,
            visa_requirements: visaRequirements,
            travel_checklist: "", // Not in new template
            created_by_staff_id: currentUser.id,
            created_at: new Date().toISOString(),
          };

          const { data: newVisa, error } = await supabase
            .from("visas")
            .insert(visaData)
            .select()
            .single();

          if (error) {
            results.errors.push({
              row: rowNumber,
              error: error.message,
              data: row,
            });
          } else {
            results.success++;
            console.log(
              `[Visas] Visa created via bulk upload by ${currentUser.name}: ${visaData.visa_name}`
            );
          }
        } catch (error) {
          results.errors.push({
            row: rowNumber,
            error: error.message || "Unknown error",
            data: row,
          });
        }
      }

      console.log(
        `[Visas Bulk Upload] Processing complete. Success: ${results.success}, Errors: ${results.errors.length}`
      );
      if (results.errors.length > 0) {
        console.log(
          `[Visas Bulk Upload] Error details:`,
          JSON.stringify(results.errors, null, 2)
        );
      }

      res.json({
        message: `Upload complete. ${results.success} visa(s) created successfully.`,
        success: results.success,
        errors: results.errors,
      });
    } catch (error) {
      console.error("[Visas Bulk Upload] Error bulk uploading visas:", error);
      console.error("[Visas Bulk Upload] Error stack:", error.stack);
      res.status(500).json({
        message: error.message || "Failed to bulk upload visas.",
      });
    }
  }
);

// --- DESTINATIONS & SIGHTSEEING API ENDPOINTS ---

// Helper function to generate slug from name
const generateSlug = (name) => {
  return name
    .toLowerCase()
    .trim()
    .replace(/[^\w\s-]/g, "")
    .replace(/[\s_-]+/g, "-")
    .replace(/^-+|-+$/g, "");
};

// Helper function to check if user is Super Admin or Office Admin
// Check if user can EDIT destinations/attractions (Super Admin & Office Admin only)
const checkDestinationsEditAccess = (currentUser) => {
  return currentUser.role_id === 1 || currentUser.is_lead_manager === true;
};

// All staff can VIEW destinations/attractions, but only Super Admin & Office Admin can EDIT
const checkDestinationsAccess = (currentUser) => {
  // This function is kept for backward compatibility but now only checks edit access
  // View access is allowed for all authenticated users
  return checkDestinationsEditAccess(currentUser);
};

// Get all destinations - ALL STAFF CAN VIEW
app.get("/api/destinations", requireAuth, async (req, res) => {
  try {
    const currentUser = req.user;
    // All authenticated staff can view destinations

    const { data: destinations, error } = await supabase
      .from("destinations")
      .select("*")
      .order("name", { ascending: true });

    if (error) throw error;

    res.json(destinations || []);
  } catch (error) {
    console.error("Error fetching destinations:", error);
    res.status(500).json({
      message: error.message || "Failed to fetch destinations.",
    });
  }
});

// Get a single destination by ID or slug
app.get("/api/destinations/:identifier", requireAuth, async (req, res) => {
  try {
    const currentUser = req.user;
    // All authenticated staff can view destinations

    const { identifier } = req.params;
    const isNumeric = /^\d+$/.test(identifier);

    let query = supabase.from("destinations").select("*");

    if (isNumeric) {
      query = query.eq("id", parseInt(identifier));
    } else {
      query = query.eq("slug", identifier);
    }

    const { data: destination, error } = await query.single();

    if (error) throw error;
    if (!destination) {
      return res.status(404).json({ message: "Destination not found." });
    }

    res.json(destination);
  } catch (error) {
    console.error("Error fetching destination:", error);
    res.status(500).json({
      message: error.message || "Failed to fetch destination.",
    });
  }
});

// Create a new destination - EDIT ACCESS REQUIRED
app.post("/api/destinations", requireAuth, async (req, res) => {
  try {
    const currentUser = req.user;

    if (!checkDestinationsEditAccess(currentUser)) {
      return res.status(403).json({
        message:
          "Access denied. Only Super Admin and Office Admin can create destinations.",
      });
    }

    const { name } = req.body;

    if (!name || !name.trim()) {
      return res.status(400).json({ message: "Destination name is required." });
    }

    const slug = generateSlug(name.trim());

    // Check if slug already exists
    const { data: existing } = await supabase
      .from("destinations")
      .select("id")
      .eq("slug", slug)
      .single();

    if (existing) {
      return res
        .status(400)
        .json({ message: "A destination with this name already exists." });
    }

    const { data: newDestination, error } = await supabase
      .from("destinations")
      .insert({
        name: name.trim(),
        slug: slug,
        created_by_staff_id: currentUser.id,
        created_at: new Date().toISOString(),
        updated_at: new Date().toISOString(),
      })
      .select()
      .single();

    if (error) throw error;

    console.log(
      `[Destinations] Destination created by ${currentUser.name}: ${name}`
    );

    res.status(201).json({
      message: "Destination created successfully.",
      destination: newDestination,
    });
  } catch (error) {
    console.error("Error creating destination:", error);
    res.status(500).json({
      message: error.message || "Failed to create destination.",
    });
  }
});

// Update a destination - EDIT ACCESS REQUIRED
app.put("/api/destinations/:id", requireAuth, async (req, res) => {
  try {
    const currentUser = req.user;

    if (!checkDestinationsEditAccess(currentUser)) {
      return res.status(403).json({
        message:
          "Access denied. Only Super Admin and Office Admin can update destinations.",
      });
    }

    const { id } = req.params;
    const { name } = req.body;

    // Get current destination
    const { data: currentDestination, error: fetchError } = await supabase
      .from("destinations")
      .select("*")
      .eq("id", id)
      .single();

    if (fetchError) throw fetchError;
    if (!currentDestination) {
      return res.status(404).json({ message: "Destination not found." });
    }

    const updateData = {
      updated_at: new Date().toISOString(),
      updated_by_staff_id: currentUser.id,
    };

    if (name !== undefined && name.trim() !== currentDestination.name) {
      updateData.name = name.trim();
      const newSlug = generateSlug(name.trim());

      // Check if new slug already exists (excluding current destination)
      const { data: existing } = await supabase
        .from("destinations")
        .select("id")
        .eq("slug", newSlug)
        .neq("id", id)
        .single();

      if (existing) {
        return res
          .status(400)
          .json({ message: "A destination with this name already exists." });
      }

      updateData.slug = newSlug;
    }

    const { data: updatedDestination, error } = await supabase
      .from("destinations")
      .update(updateData)
      .eq("id", id)
      .select()
      .single();

    if (error) throw error;

    console.log(
      `[Destinations] Destination ${id} updated by ${currentUser.name}`
    );

    res.json({
      message: "Destination updated successfully.",
      destination: updatedDestination,
    });
  } catch (error) {
    console.error("Error updating destination:", error);
    res.status(500).json({
      message: error.message || "Failed to update destination.",
    });
  }
});

// Delete a destination - SUPER ADMIN ONLY
app.delete("/api/destinations/:id", requireAuth, async (req, res) => {
  try {
    const currentUser = req.user;

    // Only Super Admin can delete
    if (currentUser.role_id !== 1) {
      return res.status(403).json({
        message: "Access denied. Only Super Admin can delete destinations.",
      });
    }

    const { id } = req.params;

    const { error: deleteError } = await supabase
      .from("destinations")
      .delete()
      .eq("id", id);

    if (deleteError) throw deleteError;

    console.log(
      `[Destinations] Destination ${id} deleted by ${currentUser.name}`
    );

    res.json({ message: "Destination deleted successfully." });
  } catch (error) {
    console.error("Error deleting destination:", error);
    res.status(500).json({
      message: error.message || "Failed to delete destination.",
    });
  }
});

// --- SIGHTSEEING (ATTRACTIONS) API ENDPOINTS ---

// Get all sightseeing (with optional destination filter) - ALL STAFF CAN VIEW
app.get("/api/sightseeing", requireAuth, async (req, res) => {
  try {
    const currentUser = req.user;
    // All authenticated staff can view sightseeing

    const { destination_id } = req.query;
    let query = supabase
      .from("sightseeing")
      .select("*, destinations(id, name, slug)")
      .order("attraction_name", { ascending: true });

    if (destination_id) {
      query = query.eq("destination_id", parseInt(destination_id));
    }

    const { data: sightseeing, error } = await query;

    if (error) {
      console.error("[Sightseeing API] Query error:", error);
      throw error;
    }

    // Log for debugging - check if data is being returned
    console.log(
      `[Sightseeing API] Returning ${
        sightseeing?.length || 0
      } attractions for user ${currentUser.name} (Role: ${currentUser.role})`
    );

    res.json(sightseeing || []);
  } catch (error) {
    console.error("Error fetching sightseeing:", error);
    res.status(500).json({
      message: error.message || "Failed to fetch sightseeing.",
    });
  }
});

// Get a single sightseeing item by ID - ALL STAFF CAN VIEW
app.get("/api/sightseeing/:id", requireAuth, async (req, res) => {
  try {
    const currentUser = req.user;
    // All authenticated staff can view sightseeing

    const { id } = req.params;
    const { data: sightseeing, error } = await supabase
      .from("sightseeing")
      .select("*, destinations(id, name, slug)")
      .eq("id", id)
      .single();

    if (error) throw error;
    if (!sightseeing) {
      return res.status(404).json({ message: "Sightseeing item not found." });
    }

    res.json(sightseeing);
  } catch (error) {
    console.error("Error fetching sightseeing:", error);
    res.status(500).json({
      message: error.message || "Failed to fetch sightseeing.",
    });
  }
});

// Create a new sightseeing item - EDIT ACCESS REQUIRED
app.post("/api/sightseeing", requireAuth, async (req, res) => {
  try {
    const currentUser = req.user;

    if (!checkDestinationsEditAccess(currentUser)) {
      return res.status(403).json({
        message:
          "Access denied. Only Super Admin and Office Admin can create sightseeing.",
      });
    }

    const {
      destination_id,
      attraction_name,
      per_adult_cost,
      per_child_cost,
      remarks,
    } = req.body;

    if (!destination_id) {
      return res.status(400).json({ message: "Destination ID is required." });
    }
    if (!attraction_name || !attraction_name.trim()) {
      return res.status(400).json({ message: "Attraction name is required." });
    }

    // Verify destination exists
    const { data: destination, error: destError } = await supabase
      .from("destinations")
      .select("id")
      .eq("id", destination_id)
      .single();

    if (destError || !destination) {
      return res.status(400).json({ message: "Invalid destination ID." });
    }

    const { data: newSightseeing, error } = await supabase
      .from("sightseeing")
      .insert({
        destination_id: parseInt(destination_id),
        attraction_name: attraction_name.trim(),
        per_adult_cost: per_adult_cost ? parseFloat(per_adult_cost) : 0,
        per_child_cost: per_child_cost ? parseFloat(per_child_cost) : 0,
        currency: currency || "USD",
        remarks: remarks || "",
        created_by_staff_id: currentUser.id,
        created_at: new Date().toISOString(),
        updated_at: new Date().toISOString(),
      })
      .select("*, destinations(id, name, slug)")
      .single();

    if (error) throw error;

    console.log(
      `[Sightseeing] Attraction created by ${currentUser.name}: ${attraction_name}`
    );

    res.status(201).json({
      message: "Sightseeing item created successfully.",
      sightseeing: newSightseeing,
    });
  } catch (error) {
    console.error("Error creating sightseeing:", error);
    res.status(500).json({
      message: error.message || "Failed to create sightseeing item.",
    });
  }
});

// Update a sightseeing item - EDIT ACCESS REQUIRED
app.put("/api/sightseeing/:id", requireAuth, async (req, res) => {
  try {
    const currentUser = req.user;

    if (!checkDestinationsEditAccess(currentUser)) {
      return res.status(403).json({
        message:
          "Access denied. Only Super Admin and Office Admin can update sightseeing.",
      });
    }

    const { id } = req.params;
    const {
      destination_id,
      attraction_name,
      per_adult_cost,
      per_child_cost,
      currency,
      remarks,
      tag,
      opening_hours,
      average_duration_hours,
      latitude,
      longitude,
      category,
      best_time,
      images,
      pricing,
    } = req.body;

    // Get current sightseeing item
    const { data: currentSightseeing, error: fetchError } = await supabase
      .from("sightseeing")
      .select("*")
      .eq("id", id)
      .single();

    if (fetchError) throw fetchError;
    if (!currentSightseeing) {
      return res.status(404).json({ message: "Sightseeing item not found." });
    }

    const updateData = {
      updated_at: new Date().toISOString(),
      updated_by_staff_id: currentUser.id,
    };

    if (destination_id !== undefined) {
      // Verify destination exists
      const { data: destination, error: destError } = await supabase
        .from("destinations")
        .select("id")
        .eq("id", destination_id)
        .single();

      if (destError || !destination) {
        return res.status(400).json({ message: "Invalid destination ID." });
      }
      updateData.destination_id = parseInt(destination_id);
    }
    if (attraction_name !== undefined)
      updateData.attraction_name = attraction_name.trim();
    if (per_adult_cost !== undefined)
      updateData.per_adult_cost = parseFloat(per_adult_cost) || 0;
    if (per_child_cost !== undefined)
      updateData.per_child_cost = parseFloat(per_child_cost) || 0;
    if (currency !== undefined) updateData.currency = currency || "USD";
    if (remarks !== undefined) updateData.remarks = remarks;
    if (tag !== undefined) updateData.tag = tag || null;
    if (opening_hours !== undefined)
      updateData.opening_hours = opening_hours || null;
    if (average_duration_hours !== undefined)
      updateData.average_duration_hours = average_duration_hours
        ? parseFloat(average_duration_hours)
        : null;
    if (latitude !== undefined)
      updateData.latitude = latitude ? parseFloat(latitude) : null;
    if (longitude !== undefined)
      updateData.longitude = longitude ? parseFloat(longitude) : null;
    if (category !== undefined) updateData.category = category || null;
    if (best_time !== undefined) updateData.best_time = best_time || null;
    if (images !== undefined)
      updateData.images = images && Array.isArray(images) ? images : null;
    // Note: pricing column removed - it doesn't exist in the database schema
    // if (pricing !== undefined) updateData.pricing = pricing || null;

    const { data: updatedSightseeing, error } = await supabase
      .from("sightseeing")
      .update(updateData)
      .eq("id", id)
      .select("*, destinations(id, name, slug)")
      .single();

    if (error) throw error;

    console.log(
      `[Sightseeing] Attraction ${id} updated by ${currentUser.name}`
    );

    res.json({
      message: "Sightseeing item updated successfully.",
      sightseeing: updatedSightseeing,
    });
  } catch (error) {
    console.error("Error updating sightseeing:", error);
    res.status(500).json({
      message: error.message || "Failed to update sightseeing item.",
    });
  }
});

// Delete a sightseeing item - SUPER ADMIN ONLY
app.delete("/api/sightseeing/:id", requireAuth, async (req, res) => {
  try {
    const currentUser = req.user;

    // Only Super Admin can delete
    if (currentUser.role_id !== 1) {
      return res.status(403).json({
        message: "Access denied. Only Super Admin can delete sightseeing.",
      });
    }

    const { id } = req.params;

    const { error: deleteError } = await supabase
      .from("sightseeing")
      .delete()
      .eq("id", id);

    if (deleteError) throw deleteError;

    console.log(
      `[Sightseeing] Attraction ${id} deleted by ${currentUser.name}`
    );

    res.json({ message: "Sightseeing item deleted successfully." });
  } catch (error) {
    console.error("Error deleting sightseeing:", error);
    res.status(500).json({
      message: error.message || "Failed to delete sightseeing item.",
    });
  }
});

// Bulk create sightseeing items from Excel
app.post("/api/sightseeing/bulk-create", requireAuth, async (req, res) => {
  try {
    const currentUser = req.user;

    if (!checkDestinationsEditAccess(currentUser)) {
      return res.status(403).json({
        message:
          "Access denied. Only Super Admin and Office Admin can bulk create sightseeing items.",
      });
    }

    const { attractions } = req.body;

    if (!Array.isArray(attractions) || attractions.length === 0) {
      return res
        .status(400)
        .json({ message: "At least one attraction is required." });
    }

    let successCount = 0;
    let failedCount = 0;
    const errors = [];

    // Insert attractions one by one to handle errors gracefully
    for (const attraction of attractions) {
      try {
        // Validate required fields
        if (!attraction.attraction_name || !attraction.destination_id) {
          failedCount++;
          errors.push(
            `Skipped: Missing required fields for "${
              attraction.attraction_name || "unknown"
            }"`
          );
          continue;
        }

        // Prepare data
        const sightseeingData = {
          destination_id: parseInt(attraction.destination_id),
          attraction_name: String(attraction.attraction_name).trim(),
          per_adult_cost: parseFloat(attraction.per_adult_cost) || 0,
          per_child_cost: parseFloat(attraction.per_child_cost) || 0,
          currency: attraction.currency || "USD",
          remarks: String(attraction.remarks || "").trim(),
          created_by_staff_id: currentUser.id,
          updated_by_staff_id: currentUser.id,
        };

        // Insert into database
        const { data, error } = await supabase
          .from("sightseeing")
          .insert([sightseeingData])
          .select()
          .single();

        if (error) {
          failedCount++;
          errors.push(
            `Failed to create "${attraction.attraction_name}": ${error.message}`
          );
        } else {
          successCount++;
        }
      } catch (error) {
        failedCount++;
        errors.push(
          `Error creating "${attraction.attraction_name}": ${error.message}`
        );
      }
    }

    console.log(
      `[Sightseeing] Bulk create by ${currentUser.name}: ${successCount} successful, ${failedCount} failed out of ${attractions.length} items`
    );

    res.json({
      message: `Successfully created ${successCount} attraction(s). ${
        failedCount > 0 ? `${failedCount} failed.` : ""
      }`,
      success: successCount,
      failed: failedCount,
      total: attractions.length,
      errors: failedCount > 0 ? errors : undefined,
    });
  } catch (error) {
    console.error("Error bulk creating sightseeing:", error);
    res.status(500).json({
      message: error.message || "Failed to bulk create sightseeing items.",
    });
  }
});

// Bulk delete sightseeing items - SUPER ADMIN ONLY
app.post("/api/sightseeing/bulk-delete", requireAuth, async (req, res) => {
  try {
    const currentUser = req.user;

    // Only Super Admin can bulk delete
    if (currentUser.role_id !== 1) {
      return res.status(403).json({
        message:
          "Access denied. Only Super Admin can bulk delete sightseeing items.",
      });
    }

    const { ids } = req.body;

    if (!Array.isArray(ids) || ids.length === 0) {
      return res
        .status(400)
        .json({ message: "At least one sightseeing item ID is required." });
    }

    let deletedCount = 0;
    let failedCount = 0;
    const errors = [];

    // Delete attractions one by one to handle errors gracefully
    for (const id of ids) {
      try {
        const { error } = await supabase
          .from("sightseeing")
          .delete()
          .eq("id", id);

        if (error) {
          failedCount++;
          errors.push(`Failed to delete attraction ID ${id}: ${error.message}`);
        } else {
          deletedCount++;
        }
      } catch (error) {
        failedCount++;
        errors.push(`Error deleting attraction ID ${id}: ${error.message}`);
      }
    }

    console.log(
      `[Sightseeing] Bulk delete by ${currentUser.name}: ${deletedCount} deleted, ${failedCount} failed out of ${ids.length} items`
    );

    res.json({
      message: `Successfully deleted ${deletedCount} attraction(s). ${
        failedCount > 0 ? `${failedCount} failed.` : ""
      }`,
      deleted_count: deletedCount,
      failed: failedCount,
      total: ids.length,
      errors: failedCount > 0 ? errors : undefined,
    });
  } catch (error) {
    console.error("Error bulk deleting sightseeing:", error);
    res.status(500).json({
      message: error.message || "Failed to bulk delete sightseeing items.",
    });
  }
});

// Bulk update prices for sightseeing items
app.post(
  "/api/sightseeing/bulk-update-prices",
  requireAuth,
  async (req, res) => {
    try {
      const currentUser = req.user;

      if (!checkDestinationsEditAccess(currentUser)) {
        return res.status(403).json({
          message:
            "Access denied. Only Super Admin and Office Admin can bulk update prices.",
        });
      }

      const { ids, percentage, operation } = req.body; // operation: 'increase' or 'decrease'

      if (!Array.isArray(ids) || ids.length === 0) {
        return res
          .status(400)
          .json({ message: "At least one sightseeing item ID is required." });
      }
      if (!percentage || percentage <= 0) {
        return res
          .status(400)
          .json({ message: "Valid percentage is required." });
      }
      if (!operation || !["increase", "decrease"].includes(operation)) {
        return res
          .status(400)
          .json({ message: "Operation must be 'increase' or 'decrease'." });
      }

      // Get current items
      const { data: items, error: fetchError } = await supabase
        .from("sightseeing")
        .select("id, per_adult_cost, per_child_cost")
        .in("id", ids);

      if (fetchError) throw fetchError;

      const multiplier =
        operation === "increase" ? 1 + percentage / 100 : 1 - percentage / 100;

      // Update each item
      const updates = items.map((item) => ({
        id: item.id,
        per_adult_cost:
          Math.round(item.per_adult_cost * multiplier * 100) / 100,
        per_child_cost:
          Math.round(item.per_child_cost * multiplier * 100) / 100,
        updated_at: new Date().toISOString(),
        updated_by_staff_id: currentUser.id,
      }));

      // Perform bulk update
      for (const update of updates) {
        const { error: updateError } = await supabase
          .from("sightseeing")
          .update({
            per_adult_cost: update.per_adult_cost,
            per_child_cost: update.per_child_cost,
            updated_at: update.updated_at,
            updated_by_staff_id: update.updated_by_staff_id,
          })
          .eq("id", update.id);

        if (updateError) throw updateError;
      }

      console.log(
        `[Sightseeing] Bulk price update by ${currentUser.name}: ${operation} ${percentage}% for ${ids.length} items`
      );

      res.json({
        message: `Successfully ${
          operation === "increase" ? "increased" : "decreased"
        } prices by ${percentage}% for ${ids.length} items.`,
        updated_count: ids.length,
      });
    } catch (error) {
      console.error("Error bulk updating prices:", error);
      res.status(500).json({
        message: error.message || "Failed to bulk update prices.",
      });
    }
  }
);

// Bulk update currency for sightseeing items
app.post(
  "/api/sightseeing/bulk-update-currency",
  requireAuth,
  async (req, res) => {
    try {
      const currentUser = req.user;

      if (!checkDestinationsEditAccess(currentUser)) {
        return res.status(403).json({
          message:
            "Access denied. Only Super Admin and Office Admin can bulk update currency.",
        });
      }

      const { ids, currency } = req.body;

      if (!Array.isArray(ids) || ids.length === 0) {
        return res
          .status(400)
          .json({ message: "At least one sightseeing item ID is required." });
      }
      if (!currency || typeof currency !== "string") {
        return res.status(400).json({ message: "Valid currency is required." });
      }

      // Valid currency check
      const validCurrencies = [
        "INR",
        "USD",
        "EUR",
        "GBP",
        "AUD",
        "CAD",
        "SGD",
        "JPY",
        "CHF",
        "CNY",
        "NZD",
      ];
      if (!validCurrencies.includes(currency)) {
        return res.status(400).json({
          message: `Invalid currency. Must be one of: ${validCurrencies.join(
            ", "
          )}`,
        });
      }

      // Get current items with their prices and currencies
      const { data: items, error: fetchError } = await supabase
        .from("sightseeing")
        .select("id, per_adult_cost, per_child_cost, currency")
        .in("id", ids);

      if (fetchError) throw fetchError;

      // Fetch FX rates from API
      let fxRates = {};
      try {
        const fxResponse = await fetch(
          "https://api.frankfurter.app/latest?from=INR"
        );
        if (fxResponse.ok) {
          const fxData = await fxResponse.json();
          // Invert rates: API returns FROM INR, we need TO INR
          if (fxData.rates) {
            Object.keys(fxData.rates).forEach((curr) => {
              const rateFromInr = fxData.rates[curr];
              if (rateFromInr > 0 && rateFromInr < 1) {
                fxRates[curr] = 1 / rateFromInr; // Convert to TO INR rate
              } else if (rateFromInr >= 1) {
                fxRates[curr] = rateFromInr; // Already TO INR
              }
            });
          }
          fxRates["INR"] = 1;
        }
      } catch (fxError) {
        console.error("Error fetching FX rates:", fxError);
        // Fallback to static rates if API fails
        fxRates = {
          INR: 1,
          USD: 83.0,
          EUR: 90.0,
          GBP: 105.0,
          AUD: 54.0,
          CAD: 61.0,
          SGD: 61.5,
          JPY: 0.56,
          CHF: 95.0,
          CNY: 11.5,
          NZD: 50.0,
        };
      }

      // Convert prices for each item
      const updates = items.map((item) => {
        const oldCurrency = item.currency || "USD";
        const newCurrency = currency;

        // Get FX rates: convert FROM old currency TO INR, then FROM INR TO new currency
        const rateFromOldToInr =
          fxRates[oldCurrency] || (oldCurrency === "INR" ? 1 : 83.0);
        const rateFromInrToNew =
          fxRates[newCurrency] || (newCurrency === "INR" ? 1 : 83.0);

        // Calculate conversion rate: old currency -> INR -> new currency
        // If converting USD to EUR: USD -> INR -> EUR
        // Rate = (1 USD = X INR) / (1 EUR = Y INR) = X / Y
        const conversionRate = rateFromOldToInr / rateFromInrToNew;

        // Convert base prices using the conversion rate
        // Note: Markup formula ((price Ã— FX_rate) + 2) Ã— 1.15 is applied at calculation time in itinerary costing
        // Here we only convert the base price using FX rate
        const newAdultPrice =
          Math.round((item.per_adult_cost || 0) * conversionRate * 100) / 100;
        const newChildPrice =
          Math.round((item.per_child_cost || 0) * conversionRate * 100) / 100;

        // Prepare update object - only update legacy fields that exist in the database
        return {
          id: item.id,
          currency: newCurrency,
          per_adult_cost: newAdultPrice,
          per_child_cost: newChildPrice,
          updated_at: new Date().toISOString(),
          updated_by_staff_id: currentUser.id,
        };
      });

      // Perform bulk update - only update columns that exist in the database
      for (const update of updates) {
        const { error: updateError } = await supabase
          .from("sightseeing")
          .update({
            currency: update.currency,
            per_adult_cost: update.per_adult_cost,
            per_child_cost: update.per_child_cost,
            updated_at: update.updated_at,
            updated_by_staff_id: update.updated_by_staff_id,
          })
          .eq("id", update.id);

        if (updateError) throw updateError;
      }

      console.log(
        `[Sightseeing] Bulk currency update by ${currentUser.name}: Changed to ${currency} for ${ids.length} items`
      );

      res.json({
        message: `Successfully updated currency to ${currency} for ${ids.length} items.`,
        updated_count: ids.length,
      });
    } catch (error) {
      console.error("Error bulk updating currency:", error);
      res.status(500).json({
        message: error.message || "Failed to bulk update currency.",
      });
    }
  }
);

// Bulk generate attraction details using Google Places API
app.post("/api/sightseeing/generate-details", requireAuth, async (req, res) => {
  try {
    const currentUser = req.user;

    if (!checkDestinationsEditAccess(currentUser)) {
      return res.status(403).json({
        message:
          "Access denied. Only Super Admin and Office Admin can generate attraction details.",
      });
    }

    const { attractions } = req.body; // Array of { name: string, destination_name: string, destination_id: number }

    if (!Array.isArray(attractions) || attractions.length === 0) {
      return res
        .status(400)
        .json({ message: "At least one attraction is required." });
    }

    const GOOGLE_PLACES_API_KEY = process.env.GOOGLE_PLACES_API_KEY;
    if (!GOOGLE_PLACES_API_KEY) {
      return res.status(500).json({
        message: "Google Places API key is not configured.",
      });
    }

    const results = [];
    const errors = [];

    // Process attractions in batches to avoid rate limits
    const batchSize = 10;
    for (let i = 0; i < attractions.length; i += batchSize) {
      const batch = attractions.slice(i, i + batchSize);

      await Promise.all(
        batch.map(async (attraction) => {
          try {
            const { name, destination_name, destination_id } = attraction;

            if (!name) {
              const errorMsg = "Attraction name is required";
              console.error(
                `[Sightseeing AI] ${name || "Unknown"}: ${errorMsg}`
              );
              errors.push({
                name: name || "Unknown",
                error: errorMsg,
                details: { destination_name, destination_id },
              });
              return;
            }

            // Parse attraction name to extract duration, time, and additional info
            let cleanedName = name;
            let extractedDuration = null;
            let extractedOpeningHours = null;
            let extractedRemarks = [];

            // Extract duration patterns: "2.5 Hours", "4 Hours", "2 Hours", "1 Hour", "30 Min", "45 Mins", etc.
            const durationPatterns = [
              /(\d+\.?\d*)\s*(?:Hours?|Hrs?|H)/i, // "2.5 Hours", "4 Hrs"
              /(\d+)\s*(?:Minutes?|Mins?|Min)/i, // "30 Minutes", "45 Mins"
            ];

            for (const pattern of durationPatterns) {
              const match = name.match(pattern);
              if (match) {
                let hours = parseFloat(match[1]);
                // Convert minutes to hours if needed
                if (pattern.toString().includes("Min")) {
                  hours = hours / 60;
                }
                extractedDuration = hours;
                // Remove the duration from the name
                cleanedName = cleanedName.replace(pattern, "").trim();
                break;
              }
            }

            // Extract time patterns: "6:15 PM Departure", "7:30pm Show", "10am", etc.
            const timePatterns = [
              /(\d{1,2}):(\d{2})\s*(AM|PM)\s*(?:Departure|Show|Start|Begin)/i, // "6:15 PM Departure", "7:30pm Show"
              /(\d{1,2}):(\d{2})\s*(AM|PM)/i, // "6:15 PM", "10:30 AM"
              /(\d{1,2})\s*(AM|PM)/i, // "6 PM", "10 AM"
            ];

            for (const pattern of timePatterns) {
              const match = name.match(pattern);
              if (match) {
                let hour = parseInt(match[1]);
                const minutes = match[2] ? parseInt(match[2]) : 0;
                const ampm = match[3] || match[2]; // Handle both formats

                // Convert to 24-hour format
                if (ampm && ampm.toUpperCase() === "PM" && hour !== 12) {
                  hour += 12;
                } else if (ampm && ampm.toUpperCase() === "AM" && hour === 12) {
                  hour = 0;
                }

                extractedOpeningHours = `${String(hour).padStart(
                  2,
                  "0"
                )}:${String(minutes).padStart(2, "0")}`;
                // Remove the time from the name
                cleanedName = cleanedName.replace(pattern, "").trim();
                break;
              }
            }

            // Extract parenthetical information for remarks
            const parenthesesPattern = /\(([^)]+)\)/g;
            const parenthesesMatches = [];
            let parenMatch;
            while ((parenMatch = parenthesesPattern.exec(name)) !== null) {
              parenthesesMatches.push(parenMatch[1]);
            }

            // Clean up the name by removing parentheses and extra spaces
            cleanedName = cleanedName.replace(/\([^)]+\)/g, "").trim();
            cleanedName = cleanedName.replace(/\s+/g, " ").trim();

            // Add parenthetical info to remarks (except if it's just time/duration which we already extracted)
            for (const parenInfo of parenthesesMatches) {
              // Skip if it's just a time pattern or duration we already extracted
              if (
                !parenInfo.match(/(\d+\.?\d*)\s*(?:Hours?|Hrs?|H)/i) &&
                !parenInfo.match(/(\d{1,2}):(\d{2})\s*(AM|PM)/i) &&
                !parenInfo.match(/(\d{1,2})\s*(AM|PM)/i)
              ) {
                extractedRemarks.push(parenInfo);
              }
            }

            // Step 1: Text Search using Places API (New) - use cleaned name for better search
            const searchQuery = `${cleanedName} ${
              destination_name || ""
            }`.trim();
            const searchUrl = `https://places.googleapis.com/v1/places:searchText`;

            console.log(
              `[Sightseeing AI] Searching for: "${searchQuery}" (Attraction: ${name}, Destination: ${
                destination_name || "N/A"
              })`
            );

            // Log extracted information
            if (
              extractedDuration ||
              extractedOpeningHours ||
              extractedRemarks.length > 0
            ) {
              console.log(`[Sightseeing AI] Extracted from name "${name}":`, {
                cleanedName,
                extractedDuration,
                extractedOpeningHours,
                extractedRemarks:
                  extractedRemarks.length > 0 ? extractedRemarks : null,
              });
            }

            const searchResponse = await fetch(searchUrl, {
              method: "POST",
              headers: {
                "Content-Type": "application/json",
                "X-Goog-Api-Key": GOOGLE_PLACES_API_KEY,
                "X-Goog-FieldMask":
                  "places.id,places.displayName,places.formattedAddress,places.location,places.types,places.photos",
              },
              body: JSON.stringify({
                textQuery: searchQuery,
                maxResultCount: 1,
              }),
            });

            if (!searchResponse.ok) {
              const errorText = await searchResponse.text();
              let errorData;
              try {
                errorData = JSON.parse(errorText);
              } catch {
                errorData = { error: { message: errorText } };
              }
              const errorMsg = `Place search failed: ${searchResponse.status}${
                errorData.error?.message ? ` - ${errorData.error.message}` : ""
              }`;
              console.error(`[Sightseeing AI] ${name}: ${errorMsg}`, {
                searchQuery,
                status: searchResponse.status,
                error: errorData,
              });
              errors.push({
                name,
                error: errorMsg,
                details: {
                  searchQuery,
                  status: searchResponse.status,
                  error: errorData,
                  destination_name,
                  destination_id,
                },
              });
              return;
            }

            const searchData = await searchResponse.json();

            if (!searchData.places || searchData.places.length === 0) {
              const errorMsg = `Place not found for query: "${searchQuery}"`;
              console.error(`[Sightseeing AI] ${name}: ${errorMsg}`, {
                searchQuery,
                response: searchData,
              });
              errors.push({
                name,
                error: errorMsg,
                details: {
                  searchQuery,
                  response: searchData,
                  destination_name,
                  destination_id,
                },
              });
              return;
            }

            const place = searchData.places[0];
            const placeId = place.id;
            console.log(
              `[Sightseeing AI] ${name}: Found place "${
                place.displayName?.text || placeId
              }" (Place ID: ${placeId})`
            );

            // Step 2: Get Place Details using Places API (New)
            const detailsUrl = `https://places.googleapis.com/v1/places/${placeId}`;

            const detailsResponse = await fetch(detailsUrl, {
              method: "GET",
              headers: {
                "Content-Type": "application/json",
                "X-Goog-Api-Key": GOOGLE_PLACES_API_KEY,
                "X-Goog-FieldMask":
                  "id,displayName,formattedAddress,location,types,photos,regularOpeningHours,currentOpeningHours",
              },
            });

            if (!detailsResponse.ok) {
              const errorText = await detailsResponse.text();
              let errorData;
              try {
                errorData = JSON.parse(errorText);
              } catch {
                errorData = { error: { message: errorText } };
              }
              const errorMsg = `Details not found: ${detailsResponse.status}${
                errorData.error?.message ? ` - ${errorData.error.message}` : ""
              }`;
              console.error(`[Sightseeing AI] ${name}: ${errorMsg}`, {
                placeId,
                status: detailsResponse.status,
                error: errorData,
              });
              errors.push({
                name,
                error: errorMsg,
                details: {
                  placeId,
                  status: detailsResponse.status,
                  error: errorData,
                  destination_name,
                  destination_id,
                },
              });
              return;
            }

            const details = await detailsResponse.json();

            // Extract opening hours (new API format)
            // Use extracted opening hours if available, otherwise use Google Places data
            let openingHours = extractedOpeningHours;
            if (!openingHours) {
              const openingHoursData =
                details.regularOpeningHours || details.currentOpeningHours;
              if (
                openingHoursData &&
                openingHoursData.weekdayDescriptions &&
                openingHoursData.weekdayDescriptions.length > 0
              ) {
                // Get the first day's hours as a simple format (e.g., "Monday: 10:00 AM â€“ 7:00 PM")
                const firstDay = openingHoursData.weekdayDescriptions[0];
                // Extract time range (e.g., "Monday: 10:00 AM â€“ 7:00 PM" -> "10:00-19:00")
                const timeMatch = firstDay.match(
                  /(\d{1,2}):(\d{2})\s*(AM|PM)\s*â€“\s*(\d{1,2}):(\d{2})\s*(AM|PM)/
                );
                if (timeMatch) {
                  const [, startH, startM, startAMPM, endH, endM, endAMPM] =
                    timeMatch;
                  const startHour =
                    parseInt(startH) +
                    (startAMPM === "PM" && startH !== "12" ? 12 : 0) -
                    (startAMPM === "AM" && startH === "12" ? 12 : 0);
                  const endHour =
                    parseInt(endH) +
                    (endAMPM === "PM" && endH !== "12" ? 12 : 0) -
                    (endAMPM === "AM" && endH === "12" ? 12 : 0);
                  openingHours = `${String(startHour).padStart(
                    2,
                    "0"
                  )}:${startM}-${String(endHour).padStart(2, "0")}:${endM}`;
                } else {
                  // Fallback: use the full weekday text (remove day name prefix)
                  openingHours = firstDay.replace(/^\w+:\s*/, "");
                }
              }
            }

            // Extract location (new API format)
            const latitude = details.location?.latitude || null;
            const longitude = details.location?.longitude || null;

            // Extract category from types (new API format - types are still strings)
            let category = null;
            if (details.types && details.types.length > 0) {
              // Map Google Places types to our categories
              const typeMap = {
                amusement_park: "theme_park",
                theme_park: "theme_park",
                water_park: "water_park",
                zoo: "zoo",
                aquarium: "aquarium",
                museum: "museum",
                art_gallery: "art_gallery",
                park: "park",
                tourist_attraction: "tourist_attraction",
                night_club: "night_attraction",
                bar: "night_attraction",
                restaurant: "restaurant",
                shopping_mall: "shopping_mall",
              };

              for (const type of details.types) {
                // Types in new API might be prefixed with "places/" or just be the type name
                const typeName = type
                  .replace("places/", "")
                  .replace("types/", "");
                if (typeMap[typeName]) {
                  category = typeMap[typeName];
                  break;
                }
              }

              if (!category && details.types.length > 0) {
                // Use first type as fallback, clean it up
                const firstType = details.types[0]
                  .replace("places/", "")
                  .replace("types/", "");
                category = firstType;
              }
            }

            // Determine best_time based on opening hours
            let bestTime = null;
            if (openingHours) {
              const hourMatch = openingHours.match(/(\d{2}):(\d{2})/);
              if (hourMatch) {
                const openHour = parseInt(hourMatch[1]);
                if (openHour >= 18) {
                  bestTime = "Night";
                } else if (openHour >= 15) {
                  bestTime = "Sunset";
                } else if (openHour >= 12) {
                  bestTime = "Afternoon";
                } else {
                  bestTime = "Morning";
                }
              }
            }

            // Estimate average_duration_hours based on category
            // Use extracted duration if available, otherwise use category-based estimate
            let averageDurationHours = extractedDuration; // Use extracted duration first
            if (!averageDurationHours) {
              const durationMap = {
                theme_park: 6,
                water_park: 4,
                zoo: 3,
                aquarium: 2,
                museum: 2,
                art_gallery: 1.5,
                park: 2,
                tourist_attraction: 2,
                night_attraction: 3,
                restaurant: 1.5,
                shopping_mall: 3,
              };
              averageDurationHours = durationMap[category] || 2;
            }

            // Predict tag based on category, opening hours, and duration
            let predictedTag = null;

            // Check if it's Night-only (opens after 6 PM or only operates at night)
            if (openingHours) {
              const hourMatch = openingHours.match(/(\d{2}):(\d{2})/);
              if (hourMatch) {
                const openHour = parseInt(hourMatch[1]);
                // If opens at 6 PM or later, it's likely night-only
                if (openHour >= 18) {
                  predictedTag = "Night-only";
                }
              }
            }

            // Override based on category if it's clearly a night attraction
            if (
              category === "night_attraction" ||
              category === "night_club" ||
              category === "bar"
            ) {
              predictedTag = "Night-only";
            }

            // If not night-only, determine based on duration and category
            if (!predictedTag) {
              // Quick stop: less than 2 hours
              if (averageDurationHours < 2) {
                // Check if it's a simple attraction (viewpoints, quick photo spots)
                const quickStopCategories = [
                  "point_of_interest",
                  "establishment",
                  "store",
                ];
                const quickStopTypes = [
                  "viewpoint",
                  "lookout",
                  "monument",
                  "statue",
                ];

                if (
                  quickStopCategories.includes(category) ||
                  quickStopTypes.some((type) => category?.includes(type))
                ) {
                  predictedTag = "Quick stop";
                } else {
                  // Even if < 2 hours, if not a quick stop type, it's half-day
                  predictedTag = "Half-day";
                }
              }
              // Full-day: 8-9 hours OR theme parks/water parks/zoo (which typically take full day)
              else if (
                averageDurationHours >= 8 ||
                category === "theme_park" ||
                category === "water_park" ||
                category === "zoo"
              ) {
                predictedTag = "Full-day";
              }
              // Half-day: 3-4 hours (or 2-7 hours as fallback)
              else if (averageDurationHours >= 3 && averageDurationHours < 8) {
                predictedTag = "Half-day";
              }
              // Fallback: default to Half-day
              else {
                predictedTag = "Half-day";
              }
            }

            // Get photos (max 4) - new API format
            // TODO: COST OPTIMIZATION - Google Places API costs are high for image requests
            // Instead of storing live API URLs, download images and store them in:
            // 1. Supabase Storage (recommended) - upload images to a bucket
            // 2. Or convert to base64 and store in database (less recommended for large images)
            // This will eliminate ongoing API costs for image display
            const images = [];
            if (details.photos && details.photos.length > 0) {
              const photoCount = Math.min(4, details.photos.length);
              for (let i = 0; i < photoCount; i++) {
                const photo = details.photos[i];
                // New API: photos have name property which is the photo reference
                // Format: places/{place_id}/photos/{photo_reference}
                const photoReference = photo.name
                  ? photo.name.split("/").pop()
                  : photo.name;
                if (photoReference) {
                  // Use the new Places Photo API endpoint
                  // TODO: Download this image and store in Supabase Storage instead
                  const photoUrl = `https://places.googleapis.com/v1/${photo.name}/media?maxWidthPx=800&key=${GOOGLE_PLACES_API_KEY}`;
                  images.push(photoUrl);
                }
              }
            }

            // Combine extracted remarks with any existing remarks
            let combinedRemarks = null;
            if (extractedRemarks.length > 0) {
              combinedRemarks = extractedRemarks.join("; ");
            }

            results.push({
              name: cleanedName, // Use cleaned name (without parentheses and extracted info)
              original_name: name, // Keep original for reference
              destination_id,
              opening_hours: openingHours,
              average_duration_hours: averageDurationHours,
              latitude,
              longitude,
              category,
              best_time: bestTime,
              tag: predictedTag,
              images,
              remarks: combinedRemarks, // Add extracted remarks
            });

            console.log(
              `[Sightseeing AI] ${name}: Successfully generated details`,
              {
                opening_hours: openingHours,
                category,
                best_time: bestTime,
                tag: predictedTag,
                average_duration_hours: averageDurationHours,
                images_count: images.length,
              }
            );
          } catch (error) {
            const errorMsg = error.message || String(error);
            console.error(
              `[Sightseeing AI] Error processing "${attraction.name}":`,
              {
                error: errorMsg,
                stack: error.stack,
                attraction: {
                  name: attraction.name,
                  destination_name: attraction.destination_name,
                  destination_id: attraction.destination_id,
                },
              }
            );
            errors.push({
              name: attraction.name || "Unknown",
              error: errorMsg,
              details: {
                stack: error.stack,
                destination_name: attraction.destination_name,
                destination_id: attraction.destination_id,
              },
            });
          }
        })
      );

      // Small delay between batches to avoid rate limits
      if (i + batchSize < attractions.length) {
        await new Promise((resolve) => setTimeout(resolve, 500));
      }
    }

    console.log(
      `[Sightseeing] AI generation by ${currentUser.name}: Generated ${results.length} attractions, ${errors.length} errors`
    );

    // Log first 10 errors in detail for debugging
    if (errors.length > 0) {
      console.log(
        `[Sightseeing AI] First ${Math.min(10, errors.length)} errors:`
      );
      errors.slice(0, 10).forEach((err, idx) => {
        console.log(`  ${idx + 1}. ${err.name}: ${err.error}`);
        if (err.details) {
          console.log(`     Details:`, err.details);
        }
      });
      if (errors.length > 10) {
        console.log(`  ... and ${errors.length - 10} more errors`);
      }
    }

    res.json({
      results,
      errors,
      success_count: results.length,
      error_count: errors.length,
    });
  } catch (error) {
    console.error("Error generating attraction details:", error);
    res.status(500).json({
      message: error.message || "Failed to generate attraction details.",
    });
  }
});

// --- TRANSFER TYPES API ENDPOINTS ---

// Get all transfer types - ALL STAFF CAN VIEW
app.get("/api/transfer-types", requireAuth, async (req, res) => {
  try {
    const currentUser = req.user;
    // All authenticated staff can view transfer types

    const { category, destination_id } = req.query;
    let query = supabase
      .from("transfer_types")
      .select("*, destinations(id, name, slug)")
      .order("category", { ascending: true })
      .order("name", { ascending: true });

    if (category) {
      query = query.eq("category", category);
    }

    if (destination_id) {
      query = query.eq("destination_id", parseInt(destination_id));
    }

    const { data: transferTypes, error } = await query;

    if (error) throw error;

    res.json(transferTypes || []);
  } catch (error) {
    console.error("Error fetching transfer types:", error);
    res.status(500).json({
      message: error.message || "Failed to fetch transfer types.",
    });
  }
});

// --- TRANSFERS API ENDPOINTS ---

// Get all transfers (with optional destination filter) - ALL STAFF CAN VIEW
app.get("/api/transfers", requireAuth, async (req, res) => {
  try {
    const currentUser = req.user;
    // All authenticated staff can view transfers

    const { destination_id } = req.query;
    let query = supabase
      .from("transfers")
      .select("*, destinations(id, name, slug)")
      .order("name", { ascending: true });

    if (destination_id) {
      query = query.eq("destination_id", parseInt(destination_id));
    }

    const { data: transfers, error } = await query;

    if (error) throw error;

    res.json(transfers || []);
  } catch (error) {
    console.error("Error fetching transfers:", error);
    res.status(500).json({
      message: error.message || "Failed to fetch transfers.",
    });
  }
});

// Download Excel template for bulk transfer upload (MUST be before /api/transfers/:id route)
app.get("/api/transfers/template", requireAuth, async (req, res) => {
  try {
    const currentUser = req.user;

    // Only Super Admin and Office Admin can download template
    if (!checkDestinationsEditAccess(currentUser)) {
      return res.status(403).json({
        message:
          "Access denied. Only Super Admin and Office Admin can download template.",
      });
    }

    const workbook = new ExcelJS.Workbook();
    const worksheet = workbook.addWorksheet("Transfer Template");

    // Define columns
    worksheet.columns = [
      { header: "Transfer Name*", key: "name", width: 30 },
      { header: "Destination", key: "destination", width: 25 },
      { header: "Category", key: "category", width: 20 },
      { header: "Vehicle Type", key: "vehicle_type", width: 20 },
      { header: "Capacity", key: "capacity", width: 12 },
      { header: "Duration", key: "duration", width: 15 },
      { header: "Cost", key: "cost", width: 12 },
    ];

    // Style header row
    worksheet.getRow(1).font = { bold: true };
    worksheet.getRow(1).fill = {
      type: "pattern",
      pattern: "solid",
      fgColor: { argb: "FFE0E0E0" },
    };

    // Add example row
    worksheet.addRow({
      name: "Example: Airport to Hotel Transfer",
      destination: "Sri Lanka",
      category: "Main Segment",
      vehicle_type: "Sedan",
      capacity: 4,
      duration: "30 minutes",
      cost: 50,
    });

    res.setHeader(
      "Content-Type",
      "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    );
    res.setHeader(
      "Content-Disposition",
      'attachment; filename="Transfer_Bulk_Upload_Template.xlsx"'
    );

    await workbook.xlsx.write(res);
    res.end();
  } catch (error) {
    console.error("Error generating template:", error);
    res.status(500).json({
      message: error.message || "Failed to generate template.",
    });
  }
});

// Get a single transfer by ID - ALL STAFF CAN VIEW
app.get("/api/transfers/:id", requireAuth, async (req, res) => {
  try {
    const currentUser = req.user;
    // All authenticated staff can view transfers

    const { id } = req.params;

    const { data: transfer, error } = await supabase
      .from("transfers")
      .select("*, destinations(id, name, slug)")
      .eq("id", parseInt(id))
      .single();

    if (error) throw error;

    res.json(transfer);
  } catch (error) {
    console.error("Error fetching transfer:", error);
    res.status(500).json({
      message: error.message || "Failed to fetch transfer.",
    });
  }
});

// Create a new transfer - EDIT ACCESS REQUIRED
app.post("/api/transfers", requireAuth, async (req, res) => {
  try {
    const currentUser = req.user;

    if (!checkDestinationsEditAccess(currentUser)) {
      return res.status(403).json({
        message:
          "Access denied. Only Super Admin and Office Admin can create transfers.",
      });
    }

    const {
      destination_id,
      name,
      cost,
      currency,
      image_url,
      vehicle_type,
      capacity,
      duration,
      remarks,
    } = req.body;

    if (!name || !name.trim()) {
      return res.status(400).json({ message: "Transfer name is required." });
    }

    if (cost === undefined || cost === null) {
      return res.status(400).json({ message: "Transfer cost is required." });
    }

    // Verify destination exists if provided
    if (destination_id) {
      const { data: destination, error: destError } = await supabase
        .from("destinations")
        .select("id")
        .eq("id", destination_id)
        .single();

      if (destError || !destination) {
        return res.status(400).json({ message: "Invalid destination ID." });
      }
    }

    const { data: newTransfer, error } = await supabase
      .from("transfers")
      .insert({
        destination_id: destination_id ? parseInt(destination_id) : null,
        name: name.trim(),
        cost: parseFloat(cost),
        currency: currency || "USD",
        image_url: image_url || null,
        vehicle_type: vehicle_type || null,
        capacity: capacity ? parseInt(capacity) : null,
        duration: duration || null,
        remarks: remarks || null,
        created_by_staff_id: currentUser.id,
        created_at: new Date().toISOString(),
        updated_at: new Date().toISOString(),
      })
      .select("*, destinations(id, name, slug)")
      .single();

    if (error) throw error;

    console.log(`[Transfers] Transfer created by ${currentUser.name}: ${name}`);

    res.status(201).json({
      message: "Transfer created successfully.",
      transfer: newTransfer,
    });
  } catch (error) {
    console.error("Error creating transfer:", error);
    res.status(500).json({
      message: error.message || "Failed to create transfer.",
    });
  }
});

// Update a transfer - EDIT ACCESS REQUIRED
app.put("/api/transfers/:id", requireAuth, async (req, res) => {
  try {
    const currentUser = req.user;

    if (!checkDestinationsEditAccess(currentUser)) {
      return res.status(403).json({
        message:
          "Access denied. Only Super Admin and Office Admin can update transfers.",
      });
    }

    const { id } = req.params;
    const {
      destination_id,
      name,
      cost,
      currency,
      image_url,
      vehicle_type,
      capacity,
      duration,
      remarks,
    } = req.body;

    if (!name || !name.trim()) {
      return res.status(400).json({ message: "Transfer name is required." });
    }

    if (cost === undefined || cost === null) {
      return res.status(400).json({ message: "Transfer cost is required." });
    }

    // Verify destination exists if provided
    if (destination_id) {
      const { data: destination, error: destError } = await supabase
        .from("destinations")
        .select("id")
        .eq("id", destination_id)
        .single();

      if (destError || !destination) {
        return res.status(400).json({ message: "Invalid destination ID." });
      }
    }

    const { data: updatedTransfer, error } = await supabase
      .from("transfers")
      .update({
        destination_id: destination_id ? parseInt(destination_id) : null,
        name: name.trim(),
        cost: parseFloat(cost),
        currency: currency || "USD",
        image_url: image_url !== undefined ? image_url : undefined,
        vehicle_type: vehicle_type !== undefined ? vehicle_type : undefined,
        capacity:
          capacity !== undefined
            ? capacity
              ? parseInt(capacity)
              : null
            : undefined,
        duration: duration !== undefined ? duration : undefined,
        remarks: remarks !== undefined ? remarks : undefined,
        updated_by_staff_id: currentUser.id,
        updated_at: new Date().toISOString(),
      })
      .eq("id", parseInt(id))
      .select("*, destinations(id, name, slug)")
      .single();

    if (error) throw error;

    console.log(`[Transfers] Transfer updated by ${currentUser.name}: ${name}`);

    res.json({
      message: "Transfer updated successfully.",
      transfer: updatedTransfer,
    });
  } catch (error) {
    console.error("Error updating transfer:", error);
    res.status(500).json({
      message: error.message || "Failed to update transfer.",
    });
  }
});

// Delete a transfer - EDIT ACCESS REQUIRED
app.delete("/api/transfers/:id", requireAuth, async (req, res) => {
  try {
    const currentUser = req.user;

    if (!checkDestinationsEditAccess(currentUser)) {
      return res.status(403).json({
        message:
          "Access denied. Only Super Admin and Office Admin can delete transfers.",
      });
    }

    const { id } = req.params;

    const { error } = await supabase
      .from("transfers")
      .delete()
      .eq("id", parseInt(id));

    if (error) throw error;

    console.log(
      `[Transfers] Transfer deleted by ${currentUser.name}: ID ${id}`
    );

    res.json({ message: "Transfer deleted successfully." });
  } catch (error) {
    console.error("Error deleting transfer:", error);
    res.status(500).json({
      message: error.message || "Failed to delete transfer.",
    });
  }
});

// Bulk upload transfers from Excel file
const transferUpload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 10 * 1024 * 1024 }, // 10MB limit
  fileFilter: (req, file, cb) => {
    console.log(
      `[Transfers Bulk Upload] File filter check: ${file.originalname}, mimetype: ${file.mimetype}`
    );
    if (
      file.mimetype ===
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" ||
      file.mimetype === "application/vnd.ms-excel" ||
      file.originalname.endsWith(".xlsx") ||
      file.originalname.endsWith(".xls")
    ) {
      cb(null, true);
    } else {
      const error = new Error("Only Excel files (.xlsx, .xls) are allowed");
      req.fileValidationError = error.message;
      cb(error, false);
    }
  },
});

// Error handler for multer
const handleTransferMulterError = (err, req, res, next) => {
  if (err) {
    console.error(
      "[Transfers Bulk Upload] Multer/file filter error:",
      err.message
    );
    return res.status(400).json({
      message: err.message || "File upload error",
    });
  }
  next();
};

app.post(
  "/api/transfers/bulk-upload",
  requireAuth,
  transferUpload.single("file"),
  handleTransferMulterError,
  async (req, res) => {
    try {
      const currentUser = req.user;

      // Only Super Admin and Office Admin can bulk upload
      if (!checkDestinationsEditAccess(currentUser)) {
        return res.status(403).json({
          message:
            "Access denied. Only Super Admin and Office Admin can bulk upload transfers.",
        });
      }

      if (!req.file) {
        console.error("[Transfers Bulk Upload] No file in request");
        return res.status(400).json({ message: "No file uploaded." });
      }

      console.log(
        `[Transfers Bulk Upload] File received: ${req.file.originalname}, size: ${req.file.size} bytes, mimetype: ${req.file.mimetype}`
      );

      let workbook;
      try {
        workbook = new ExcelJS.Workbook();
        await workbook.xlsx.load(req.file.buffer);
        console.log("[Transfers Bulk Upload] Excel file loaded successfully");
      } catch (error) {
        console.error(
          "[Transfers Bulk Upload] Error loading Excel file:",
          error
        );
        return res.status(400).json({
          message: `Failed to parse Excel file: ${error.message}`,
        });
      }

      const worksheet = workbook.getWorksheet(1); // Get first worksheet
      if (!worksheet) {
        return res.status(400).json({ message: "Excel file is empty." });
      }

      console.log(
        `[Transfers Bulk Upload] Worksheet found: ${worksheet.name}, row count: ${worksheet.rowCount}`
      );

      const rows = [];
      const headers = {};

      // First, get all headers from row 1
      worksheet.getRow(1).eachCell((cell, colNumber) => {
        const headerValue = cell.value;
        if (headerValue) {
          headers[colNumber] = headerValue.toString().trim();
        }
      });

      console.log(
        `[Transfers Bulk Upload] Headers found:`,
        Object.values(headers)
      );

      // Then process data rows
      worksheet.eachRow((row, rowNumber) => {
        if (rowNumber === 1) return; // Skip header row

        const rowData = {};
        row.eachCell((cell, colNumber) => {
          const header = headers[colNumber];
          if (header) {
            rowData[header] = cell.value;
          }
        });

        if (Object.keys(rowData).length > 0) {
          rows.push(rowData);
        }
      });

      console.log(`[Transfers Bulk Upload] Parsed ${rows.length} data rows`);

      if (rows.length === 0) {
        console.error(
          "[Transfers Bulk Upload] No data rows found after parsing"
        );
        return res.status(400).json({
          message: "No data rows found in Excel file.",
        });
      }

      // Get all destinations for name lookup
      const { data: allDestinations, error: destError } = await supabase
        .from("destinations")
        .select("id, name");

      if (destError) {
        console.error(
          "[Transfers Bulk Upload] Error fetching destinations:",
          destError
        );
      }

      const destinationMap = new Map();
      if (allDestinations) {
        allDestinations.forEach((dest) => {
          destinationMap.set(dest.name.toLowerCase().trim(), dest.id);
        });
      }

      const results = {
        success: 0,
        errors: [],
      };

      // Process each row
      for (let i = 0; i < rows.length; i++) {
        const row = rows[i];
        const rowNumber = i + 2; // +2 because we skip header and 0-indexed

        try {
          // Map Excel columns to database fields
          const transferName =
            row["Transfer Name*"] || row["Transfer Name"] || row["name"];
          if (!transferName || !transferName.toString().trim()) {
            results.errors.push({
              row: rowNumber,
              error: "Transfer Name is required",
              data: row,
            });
            continue;
          }

          // Parse Destination (by name)
          const destinationName = (row["Destination"] || "").toString().trim();
          let destinationId = null;
          if (destinationName) {
            const foundDest = destinationMap.get(destinationName.toLowerCase());
            if (foundDest) {
              destinationId = foundDest;
            } else {
              results.errors.push({
                row: rowNumber,
                error: `Destination "${destinationName}" not found`,
                data: row,
              });
              continue;
            }
          }

          // Parse Category
          const category = (row["Category"] || "").toString().trim();
          const validCategories = ["Main Segment", "Attraction Transfer"];
          const transferType = validCategories.includes(category)
            ? category
            : null;

          // Parse Vehicle Type
          const vehicleType =
            (row["Vehicle Type"] || "").toString().trim() || null;

          // Parse Capacity
          let capacity = null;
          const capacityValue = row["Capacity"];
          if (capacityValue !== undefined && capacityValue !== null) {
            const parsedCapacity = parseInt(capacityValue.toString());
            if (!isNaN(parsedCapacity)) {
              capacity = parsedCapacity;
            }
          }

          // Parse Duration
          const duration = (row["Duration"] || "").toString().trim() || null;

          // Parse Cost
          let cost = 0;
          const costValue = row["Cost"];
          if (costValue !== undefined && costValue !== null) {
            const parsedCost = parseFloat(
              costValue.toString().replace(/[^0-9.]/g, "")
            );
            if (!isNaN(parsedCost)) {
              cost = parsedCost;
            } else {
              results.errors.push({
                row: rowNumber,
                error: "Invalid cost value",
                data: row,
              });
              continue;
            }
          } else {
            results.errors.push({
              row: rowNumber,
              error: "Cost is required",
              data: row,
            });
            continue;
          }

          const transferData = {
            destination_id: destinationId,
            name: transferName.toString().trim(),
            cost: cost,
            currency: "USD", // Default currency
            vehicle_type: vehicleType,
            capacity: capacity,
            duration: duration,
            type: transferType,
            created_by_staff_id: currentUser.id,
            created_at: new Date().toISOString(),
            updated_at: new Date().toISOString(),
          };

          const { data: newTransfer, error } = await supabase
            .from("transfers")
            .insert(transferData)
            .select()
            .single();

          if (error) {
            results.errors.push({
              row: rowNumber,
              error: error.message,
              data: row,
            });
          } else {
            results.success++;
            console.log(
              `[Transfers] Transfer created via bulk upload by ${currentUser.name}: ${transferData.name}`
            );
          }
        } catch (error) {
          results.errors.push({
            row: rowNumber,
            error: error.message || "Unknown error",
            data: row,
          });
        }
      }

      console.log(
        `[Transfers Bulk Upload] Processing complete. Success: ${results.success}, Errors: ${results.errors.length}`
      );
      if (results.errors.length > 0) {
        console.log(
          `[Transfers Bulk Upload] Error details:`,
          JSON.stringify(results.errors, null, 2)
        );
      }

      res.json({
        message: `Upload complete. ${results.success} transfer(s) created successfully.`,
        success: results.success,
        errors: results.errors,
      });
    } catch (error) {
      console.error(
        "[Transfers Bulk Upload] Error bulk uploading transfers:",
        error
      );
      console.error("[Transfers Bulk Upload] Error stack:", error.stack);
      res.status(500).json({
        message: error.message || "Failed to bulk upload transfers.",
      });
    }
  }
);

// ============================================================================
// AI ITINERARY ACTIVITY GENERATION ENDPOINTS
// ============================================================================

// Utility function: Calculate distance between two coordinates using Haversine formula (returns distance in km)
const calculateDistance = (lat1, lon1, lat2, lon2) => {
  if (!lat1 || !lon1 || !lat2 || !lon2) return Infinity;

  const R = 6371; // Earth's radius in km
  const dLat = ((lat2 - lat1) * Math.PI) / 180;
  const dLon = ((lon2 - lon1) * Math.PI) / 180;
  const a =
    Math.sin(dLat / 2) * Math.sin(dLat / 2) +
    Math.cos((lat1 * Math.PI) / 180) *
      Math.cos((lat2 * Math.PI) / 180) *
      Math.sin(dLon / 2) *
      Math.sin(dLon / 2);
  const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
  return R * c;
};

// Utility function: Check if two attraction names are similar (for duplicate detection)
const areAttractionsSimilar = (name1, name2) => {
  if (!name1 || !name2) return false;

  // Normalize names: remove extra spaces, convert to lowercase
  const normalize = (str) => str.toLowerCase().replace(/\s+/g, " ").trim();
  const n1 = normalize(name1);
  const n2 = normalize(name2);

  // Exact match
  if (n1 === n2) return true;

  // Extract base name (before first dash or parentheses)
  const getBaseName = (str) => {
    const dashIndex = str.indexOf(" - ");
    const parenIndex = str.indexOf(" (");
    if (dashIndex > 0) return str.substring(0, dashIndex).trim();
    if (parenIndex > 0) return str.substring(0, parenIndex).trim();
    return str;
  };

  const base1 = getBaseName(n1);
  const base2 = getBaseName(n2);

  // If base names are similar (at least 80% match), consider them duplicates
  if (base1.length > 10 && base2.length > 10) {
    const longer = base1.length > base2.length ? base1 : base2;
    const shorter = base1.length > base2.length ? base2 : base1;

    // Check if shorter is contained in longer (with some tolerance)
    if (
      longer.includes(shorter) ||
      shorter.includes(
        longer.substring(0, Math.min(shorter.length + 5, longer.length))
      )
    ) {
      return true;
    }

    // Calculate similarity using Levenshtein distance
    const similarity = calculateSimilarity(base1, base2);
    if (similarity > 0.8) return true;
  }

  return false;
};

// Calculate string similarity (0-1)
const calculateSimilarity = (str1, str2) => {
  const longer = str1.length > str2.length ? str1 : str2;
  const shorter = str1.length > str2.length ? str2 : str1;
  if (longer.length === 0) return 1.0;

  const editDistance = levenshteinDistance(longer, shorter);
  return (longer.length - editDistance) / longer.length;
};

// Levenshtein distance calculation
const levenshteinDistance = (str1, str2) => {
  const matrix = [];
  for (let i = 0; i <= str2.length; i++) {
    matrix[i] = [i];
  }
  for (let j = 0; j <= str1.length; j++) {
    matrix[0][j] = j;
  }
  for (let i = 1; i <= str2.length; i++) {
    for (let j = 1; j <= str1.length; j++) {
      if (str2.charAt(i - 1) === str1.charAt(j - 1)) {
        matrix[i][j] = matrix[i - 1][j - 1];
      } else {
        matrix[i][j] = Math.min(
          matrix[i - 1][j - 1] + 1,
          matrix[i][j - 1] + 1,
          matrix[i - 1][j] + 1
        );
      }
    }
  }
  return matrix[str2.length][str1.length];
};

// Utility function: Parse duration string to number of days
const parseDurationToDays = (durationStr) => {
  if (!durationStr) return 0;
  const numberMatch = durationStr.match(/(\d+)\s*(?:day|days)/i);
  if (numberMatch) {
    return parseInt(numberMatch[1], 10);
  }
  return 4; // Default fallback
};

// Utility function: Calculate date for a specific day number
const getDayDate = (travelDate, dayNumber) => {
  if (!travelDate) return "";
  const date = new Date(travelDate);
  date.setDate(date.getDate() + (dayNumber - 1));
  return date.toISOString().split("T")[0];
};

// Utility function: Generate time slots for an attraction
const generateTimeSlots = (openingHours, bestTime, durationHours) => {
  const slots = [];

  if (!openingHours || !durationHours) {
    // Default slots based on best_time
    if (bestTime === "Morning") {
      slots.push({ start: "08:00", end: "12:00", label: "Morning" });
      slots.push({ start: "09:00", end: "13:00", label: "Morning" });
    } else if (bestTime === "Afternoon") {
      slots.push({ start: "12:00", end: "16:00", label: "Afternoon" });
      slots.push({ start: "13:00", end: "17:00", label: "Afternoon" });
    } else if (bestTime === "Sunset") {
      slots.push({ start: "15:00", end: "19:00", label: "Sunset" });
      slots.push({ start: "16:00", end: "20:00", label: "Sunset" });
    } else if (bestTime === "Night") {
      slots.push({ start: "18:00", end: "22:00", label: "Night" });
      slots.push({ start: "19:00", end: "23:00", label: "Night" });
    } else {
      slots.push({ start: "09:00", end: "13:00", label: "Morning" });
      slots.push({ start: "14:00", end: "18:00", label: "Afternoon" });
    }
    return slots;
  }

  // Parse opening hours (format: "10:00-19:00")
  const hoursMatch = openingHours.match(/(\d{2}):(\d{2})-(\d{2}):(\d{2})/);
  if (hoursMatch) {
    const openHour = parseInt(hoursMatch[1], 10);
    const openMin = parseInt(hoursMatch[2], 10);
    const closeHour = parseInt(hoursMatch[3], 10);
    const closeMin = parseInt(hoursMatch[4], 10);

    const openTime = openHour * 60 + openMin;
    const closeTime = closeHour * 60 + closeMin;
    const durationMinutes = durationHours * 60;

    // Generate 2-3 slots based on best_time
    if (bestTime === "Morning") {
      const slot1Start = openTime;
      const slot1End = Math.min(
        slot1Start + durationMinutes,
        openTime + 4 * 60
      );
      if (slot1End <= closeTime) {
        slots.push({
          start: `${Math.floor(slot1Start / 60)
            .toString()
            .padStart(2, "0")}:${(slot1Start % 60)
            .toString()
            .padStart(2, "0")}`,
          end: `${Math.floor(slot1End / 60)
            .toString()
            .padStart(2, "0")}:${(slot1End % 60).toString().padStart(2, "0")}`,
          label: "Morning",
        });
      }

      const slot2Start = openTime + 60;
      const slot2End = Math.min(
        slot2Start + durationMinutes,
        openTime + 5 * 60
      );
      if (slot2End <= closeTime && slot2Start < 12 * 60) {
        slots.push({
          start: `${Math.floor(slot2Start / 60)
            .toString()
            .padStart(2, "0")}:${(slot2Start % 60)
            .toString()
            .padStart(2, "0")}`,
          end: `${Math.floor(slot2End / 60)
            .toString()
            .padStart(2, "0")}:${(slot2End % 60).toString().padStart(2, "0")}`,
          label: "Morning",
        });
      }
    } else if (bestTime === "Afternoon") {
      const slot1Start = Math.max(12 * 60, openTime);
      const slot1End = Math.min(slot1Start + durationMinutes, closeTime);
      if (slot1End <= closeTime) {
        slots.push({
          start: `${Math.floor(slot1Start / 60)
            .toString()
            .padStart(2, "0")}:${(slot1Start % 60)
            .toString()
            .padStart(2, "0")}`,
          end: `${Math.floor(slot1End / 60)
            .toString()
            .padStart(2, "0")}:${(slot1End % 60).toString().padStart(2, "0")}`,
          label: "Afternoon",
        });
      }

      const slot2Start = slot1Start + 60;
      const slot2End = Math.min(slot2Start + durationMinutes, closeTime);
      if (slot2End <= closeTime && slot2Start < 16 * 60) {
        slots.push({
          start: `${Math.floor(slot2Start / 60)
            .toString()
            .padStart(2, "0")}:${(slot2Start % 60)
            .toString()
            .padStart(2, "0")}`,
          end: `${Math.floor(slot2End / 60)
            .toString()
            .padStart(2, "0")}:${(slot2End % 60).toString().padStart(2, "0")}`,
          label: "Afternoon",
        });
      }
    } else if (bestTime === "Sunset") {
      const slot1Start = Math.max(15 * 60, openTime);
      const slot1End = Math.min(slot1Start + durationMinutes, closeTime);
      if (slot1End <= closeTime) {
        slots.push({
          start: `${Math.floor(slot1Start / 60)
            .toString()
            .padStart(2, "0")}:${(slot1Start % 60)
            .toString()
            .padStart(2, "0")}`,
          end: `${Math.floor(slot1End / 60)
            .toString()
            .padStart(2, "0")}:${(slot1End % 60).toString().padStart(2, "0")}`,
          label: "Sunset",
        });
      }
    } else if (bestTime === "Night") {
      const slot1Start = Math.max(18 * 60, openTime);
      const slot1End = Math.min(slot1Start + durationMinutes, closeTime);
      if (slot1End <= closeTime) {
        slots.push({
          start: `${Math.floor(slot1Start / 60)
            .toString()
            .padStart(2, "0")}:${(slot1Start % 60)
            .toString()
            .padStart(2, "0")}`,
          end: `${Math.floor(slot1End / 60)
            .toString()
            .padStart(2, "0")}:${(slot1End % 60).toString().padStart(2, "0")}`,
          label: "Night",
        });
      }
    } else {
      // Default slots
      const slot1Start = openTime;
      const slot1End = Math.min(slot1Start + durationMinutes, closeTime);
      if (slot1End <= closeTime) {
        slots.push({
          start: `${Math.floor(slot1Start / 60)
            .toString()
            .padStart(2, "0")}:${(slot1Start % 60)
            .toString()
            .padStart(2, "0")}`,
          end: `${Math.floor(slot1End / 60)
            .toString()
            .padStart(2, "0")}:${(slot1End % 60).toString().padStart(2, "0")}`,
          label: "Morning",
        });
      }
    }
  }

  return slots.length > 0
    ? slots
    : [{ start: "09:00", end: "17:00", label: "Default" }];
};

// Helper function to generate activities (can be called internally or via endpoint)
async function generateActivitiesInternal({
  travelDate,
  duration,
  destination,
  adults = 2,
  children = 0,
  existingActivities = [],
}) {
  const numDays = parseDurationToDays(duration);
  if (numDays === 0) {
    throw new Error(
      "Invalid duration format. Expected format: 'X Days' or 'X Days / Y Nights'."
    );
  }

  // Fetch destinations to match by name
  const { data: destinations, error: destError } = await supabase
    .from("destinations")
    .select("id, name");

  if (destError) throw destError;

  // Find matching destination IDs
  const matchingDestinations = destinations.filter(
    (d) =>
      d.name.toLowerCase().includes(destination.toLowerCase()) ||
      destination.toLowerCase().includes(d.name.toLowerCase())
  );

  if (matchingDestinations.length === 0) {
    throw new Error(`No destinations found matching "${destination}".`);
  }

  const destIds = matchingDestinations.map((d) => d.id);

  // Fetch all attractions for matching destinations
  const { data: sightseeing, error: sightError } = await supabase
    .from("sightseeing")
    .select("*")
    .in("destination_id", destIds);

  if (sightError) throw sightError;

  if (!sightseeing || sightseeing.length === 0) {
    throw new Error("No attractions available for this destination.");
  }

  // Filter out already added attractions by ID and name similarity
  const addedSightseeingIds = new Set(
    existingActivities.map((a) => a.sightseeing_id).filter(Boolean)
  );
  const addedAttractionNames = existingActivities
    .map((a) => a.name)
    .filter(Boolean);

  let availableAttractions = sightseeing.filter((s) => {
    // Filter by ID
    if (addedSightseeingIds.has(s.id)) return false;

    // Filter by name similarity
    return !addedAttractionNames.some((name) =>
      areAttractionsSimilar(name, s.attraction_name)
    );
  });

  if (availableAttractions.length === 0) {
    throw new Error(
      "All attractions have already been added to this itinerary."
    );
  }

  // Track used attractions to prevent duplicates
  const usedAttractionNames = new Set();
  const usedAttractionIds = new Set();

  // Helper function to check if attraction is already used
  const isAttractionUsed = (attraction) => {
    if (usedAttractionIds.has(attraction.id)) return true;
    return Array.from(usedAttractionNames).some((name) =>
      areAttractionsSimilar(name, attraction.attraction_name)
    );
  };

  // Helper function to mark attraction as used
  const markAttractionUsed = (attraction) => {
    usedAttractionIds.add(attraction.id);
    usedAttractionNames.add(attraction.attraction_name);
  };

  // Helper function to check if attractions are within distance
  const areWithinDistance = (attraction1, attraction2, maxDistance = 12) => {
    if (
      !attraction1.latitude ||
      !attraction1.longitude ||
      !attraction2.latitude ||
      !attraction2.longitude
    ) {
      return false; // Can't calculate distance, assume not nearby
    }
    const distance = calculateDistance(
      attraction1.latitude,
      attraction1.longitude,
      attraction2.latitude,
      attraction2.longitude
    );
    return distance <= maxDistance;
  };

  // Classify attractions
  const fullDayAttractions = availableAttractions.filter(
    (s) => s.tag === "Full-day" && !isAttractionUsed(s)
  );
  const nightOnlyAttractions = availableAttractions.filter(
    (s) => s.tag === "Night-only" && !isAttractionUsed(s)
  );
  const halfDayAttractions = availableAttractions.filter(
    (s) => s.tag === "Half-day" && !isAttractionUsed(s)
  );
  const quickStopAttractions = availableAttractions.filter(
    (s) => s.tag === "Quick stop" && !isAttractionUsed(s)
  );
  const unclassifiedAttractions = availableAttractions.filter(
    (s) => !s.tag && !isAttractionUsed(s)
  );

  // Distribute attractions across days
  const dayAssignments = {};
  for (let day = 1; day <= numDays; day++) {
    dayAssignments[day] = [];
  }

  // DAY 1 (Arrival Day): Only activities after 5 PM, 2-3 hours duration OR Night-only tours after 6 PM
  const arrivalDayCandidates = [
    ...nightOnlyAttractions.filter(
      (s) =>
        (s.average_duration_hours || 0) >= 2 &&
        (s.average_duration_hours || 0) <= 3
    ),
    ...halfDayAttractions.filter(
      (s) =>
        (s.average_duration_hours || 0) >= 2 &&
        (s.average_duration_hours || 0) <= 3
    ),
    ...quickStopAttractions.filter(
      (s) =>
        (s.average_duration_hours || 0) >= 2 &&
        (s.average_duration_hours || 0) <= 3
    ),
    ...unclassifiedAttractions.filter(
      (s) =>
        (s.average_duration_hours || 0) >= 2 &&
        (s.average_duration_hours || 0) <= 3
    ),
  ];

  // Prefer night-only for arrival day
  const arrivalNightOnly = arrivalDayCandidates.filter(
    (s) => s.tag === "Night-only"
  );
  if (arrivalNightOnly.length > 0 && !isAttractionUsed(arrivalNightOnly[0])) {
    dayAssignments[1].push(arrivalNightOnly[0]);
    markAttractionUsed(arrivalNightOnly[0]);
  } else if (arrivalDayCandidates.length > 0) {
    // Pick one 2-3 hour activity
    const candidate = arrivalDayCandidates.find((s) => !isAttractionUsed(s));
    if (candidate) {
      dayAssignments[1].push(candidate);
      markAttractionUsed(candidate);
    }
  }

  // DEPARTURE DAY (Last Day): Only light 2-3 hour activities before 12 PM
  const departureDayCandidates = [
    ...halfDayAttractions.filter(
      (s) =>
        (s.average_duration_hours || 0) >= 2 &&
        (s.average_duration_hours || 0) <= 3
    ),
    ...quickStopAttractions.filter(
      (s) =>
        (s.average_duration_hours || 0) >= 2 &&
        (s.average_duration_hours || 0) <= 3
    ),
    ...unclassifiedAttractions.filter(
      (s) =>
        (s.average_duration_hours || 0) >= 2 &&
        (s.average_duration_hours || 0) <= 3
    ),
  ];

  if (departureDayCandidates.length > 0) {
    const candidate = departureDayCandidates.find((s) => !isAttractionUsed(s));
    if (candidate) {
      dayAssignments[numDays].push(candidate);
      markAttractionUsed(candidate);
    }
  }

  // MIDDLE DAYS: Can mix - max 1 full-day OR max 2 nearby 2-3 hour attractions within 10-15km OR max 3 nearby 3-4 hour attractions within 10km
  const middleDays =
    numDays > 2 ? Array.from({ length: numDays - 2 }, (_, i) => i + 2) : [];

  middleDays.forEach((day) => {
    let dayActivities = dayAssignments[day];
    let dayHours = dayActivities.reduce(
      (sum, a) => sum + (a.average_duration_hours || 0),
      0
    );

    // Strategy 1: Try to add 1 full-day attraction
    if (dayHours === 0) {
      const fullDayCandidate = fullDayAttractions.find(
        (s) => !isAttractionUsed(s)
      );
      if (
        fullDayCandidate &&
        (fullDayCandidate.average_duration_hours || 0) <= 8
      ) {
        dayAssignments[day].push(fullDayCandidate);
        markAttractionUsed(fullDayCandidate);
        dayHours += fullDayCandidate.average_duration_hours || 0;
        dayActivities = dayAssignments[day]; // Update reference
      }
    }

    // Strategy 2: Add nearby 2-3 hour attractions (max 2, within 12km)
    if (dayHours < 8) {
      const twoToThreeHourAttractions = [
        ...halfDayAttractions.filter(
          (s) =>
            (s.average_duration_hours || 0) >= 2 &&
            (s.average_duration_hours || 0) <= 3
        ),
        ...unclassifiedAttractions.filter(
          (s) =>
            (s.average_duration_hours || 0) >= 2 &&
            (s.average_duration_hours || 0) <= 3
        ),
      ].filter((s) => !isAttractionUsed(s));

      let addedCount = 0;
      const lastAdded =
        dayActivities.length > 0
          ? dayActivities[dayActivities.length - 1]
          : null;

      for (const candidate of twoToThreeHourAttractions) {
        if (addedCount >= 2) break;
        if (dayHours + (candidate.average_duration_hours || 0) > 8) continue;

        // Check distance if we have a previous attraction
        if (lastAdded && !areWithinDistance(lastAdded, candidate, 12)) {
          continue; // Skip if too far
        }

        dayAssignments[day].push(candidate);
        markAttractionUsed(candidate);
        dayHours += candidate.average_duration_hours || 0;
        addedCount++;
        dayActivities = dayAssignments[day]; // Update reference
      }
    }

    // Strategy 3: Add nearby 3-4 hour attractions (max 3, within 10km)
    if (dayHours < 8 && dayAssignments[day].length < 3) {
      const threeToFourHourAttractions = [
        ...halfDayAttractions.filter(
          (s) =>
            (s.average_duration_hours || 0) >= 3 &&
            (s.average_duration_hours || 0) <= 4
        ),
        ...unclassifiedAttractions.filter(
          (s) =>
            (s.average_duration_hours || 0) >= 3 &&
            (s.average_duration_hours || 0) <= 4
        ),
      ].filter((s) => !isAttractionUsed(s));

      let addedCount = 0;
      dayActivities = dayAssignments[day]; // Update reference
      const lastAdded =
        dayActivities.length > 0
          ? dayActivities[dayActivities.length - 1]
          : null;

      for (const candidate of threeToFourHourAttractions) {
        if (addedCount >= 3 || dayAssignments[day].length >= 3) break;
        if (dayHours + (candidate.average_duration_hours || 0) > 8) continue;

        // Check distance - stricter for 3-4 hour attractions (10km)
        if (lastAdded && !areWithinDistance(lastAdded, candidate, 10)) {
          continue;
        }

        dayAssignments[day].push(candidate);
        markAttractionUsed(candidate);
        dayHours += candidate.average_duration_hours || 0;
        addedCount++;
        dayActivities = dayAssignments[day]; // Update reference
      }
    }

    // Add night-only attractions if there's room (after 6 PM)
    if (dayHours < 8) {
      const nightCandidate = nightOnlyAttractions.find(
        (s) =>
          !isAttractionUsed(s) &&
          (s.average_duration_hours || 0) <= 8 - dayHours
      );
      if (nightCandidate) {
        dayAssignments[day].push(nightCandidate);
        markAttractionUsed(nightCandidate);
      }
    }
  });

  // Generate activities with proper time slots
  const generatedActivities = [];

  for (let day = 1; day <= numDays; day++) {
    const dayAttractions = dayAssignments[day];

    dayAttractions.forEach((attraction, index) => {
      let startTime = "09:00";
      let endTime = "17:00";
      const durationHours = attraction.average_duration_hours || 2;
      const durationMinutes = durationHours * 60;

      // DAY 1 (Arrival Day): Activities after 5 PM
      if (day === 1) {
        if (attraction.tag === "Night-only") {
          // Night-only tours after 6 PM
          startTime = "18:00";
        } else {
          // Other activities after 5 PM
          startTime = "17:00";
        }
        const [startH, startM] = startTime.split(":").map(Number);
        const endMinutes = startH * 60 + startM + durationMinutes;
        const endH = Math.floor(endMinutes / 60);
        const endM = endMinutes % 60;
        endTime = `${endH.toString().padStart(2, "0")}:${endM
          .toString()
          .padStart(2, "0")}`;
      }
      // DEPARTURE DAY (Last Day): Activities before 12 PM (morning only)
      else if (day === numDays) {
        // Start early morning, ensure it ends before 12 PM
        startTime = "09:00";
        const [startH, startM] = startTime.split(":").map(Number);
        const endMinutes = startH * 60 + startM + durationMinutes;
        const endH = Math.floor(endMinutes / 60);
        const endM = endMinutes % 60;
        endTime = `${endH.toString().padStart(2, "0")}:${endM
          .toString()
          .padStart(2, "0")}`;
      }
      // MIDDLE DAYS: Use best_time and opening_hours
      else {
        const timeSlots = generateTimeSlots(
          attraction.opening_hours,
          durationMinutes,
          attraction.best_time
        );
        if (timeSlots.length > 0) {
          const slot = timeSlots[0]; // Use first suggested slot
          startTime = slot.start;
          endTime = slot.end;
        }
      }

      generatedActivities.push({
        name: attraction.attraction_name,
        date: getDayDate(travelDate, day),
        day_number: day,
        start_time: startTime,
        end_time: endTime,
        duration: attraction.average_duration_hours
          ? `${attraction.average_duration_hours} hours`
          : "",
        is_shared: false,
        inclusions: "",
        exclusions: "",
        image_url:
          attraction.images && attraction.images.length > 0
            ? attraction.images[0]
            : "",
        tag: attraction.tag,
        opening_hours: attraction.opening_hours,
        average_duration_hours: attraction.average_duration_hours,
        latitude: attraction.latitude,
        longitude: attraction.longitude,
        category: attraction.category,
        best_time: attraction.best_time,
        sightseeing_id: attraction.id,
        warnings: [],
      });
    });
  }

  return {
    success: true,
    activities: generatedActivities,
    summary: {
      total_activities: generatedActivities.length,
      days: numDays,
      by_tag: {
        "Full-day": fullDayAttractions.length,
        "Half-day": halfDayAttractions.length,
        "Night-only": nightOnlyAttractions.length,
        "Quick stop": quickStopAttractions.length,
      },
    },
  };
}

// Main endpoint: Generate activities for entire itinerary
app.post(
  "/api/itinerary/generate-activities",
  requireAuth,
  async (req, res) => {
    try {
      const currentUser = req.user;
      const {
        travelDate,
        duration,
        destination,
        adults = 2,
        children = 0,
        existingActivities = [],
      } = req.body;

      if (!travelDate || !duration || !destination) {
        return res.status(400).json({
          message: "travelDate, duration, and destination are required.",
        });
      }

      // Use the helper function
      try {
        const result = await generateActivitiesInternal({
          travelDate,
          duration,
          destination,
          adults,
          children,
          existingActivities,
        });

        console.log(
          `[Itinerary AI] Generated ${result.activities.length} activities for ${result.summary.days} days by ${currentUser.name}`
        );

        return res.json(result);
      } catch (error) {
        console.error("Error generating activities:", error);
        return res
          .status(
            error.message?.includes("No destinations")
              ? 404
              : error.message?.includes("already been added")
              ? 400
              : 500
          )
          .json({
            message: error.message || "Failed to generate activities.",
          });
      }
    } catch (error) {
      console.error("Error generating activities:", error);
      res.status(500).json({
        message: error.message || "Failed to generate activities.",
      });
    }
  }
);

// OLD ENDPOINT HANDLER CODE REMOVED - Now using generateActivitiesInternal helper function
// The code below was the old implementation, kept for reference but not executed
/*
      const numDays = parseDurationToDays(duration);
      if (numDays === 0) {
        return res.status(400).json({
          message:
            "Invalid duration format. Expected format: 'X Days' or 'X Days / Y Nights'.",
        });
      }

      // Fetch destinations to match by name
      const { data: destinations, error: destError } = await supabase
        .from("destinations")
        .select("id, name");

      if (destError) throw destError;

      // Find matching destination IDs
      const matchingDestinations = destinations.filter(
        (d) =>
          d.name.toLowerCase().includes(destination.toLowerCase()) ||
          destination.toLowerCase().includes(d.name.toLowerCase())
      );

      if (matchingDestinations.length === 0) {
        return res.status(404).json({
          message: `No destinations found matching "${destination}".`,
        });
      }

      const destIds = matchingDestinations.map((d) => d.id);

      // Fetch all attractions for matching destinations
      const { data: sightseeing, error: sightError } = await supabase
        .from("sightseeing")
        .select("*")
        .in("destination_id", destIds);

      if (sightError) throw sightError;

      if (!sightseeing || sightseeing.length === 0) {
        return res.status(404).json({
          message: "No attractions available for this destination.",
        });
      }

      // Filter out already added attractions by ID and name similarity
      const addedSightseeingIds = new Set(
        existingActivities.map((a) => a.sightseeing_id).filter(Boolean)
      );
      const addedAttractionNames = existingActivities
        .map((a) => a.name)
        .filter(Boolean);

      let availableAttractions = sightseeing.filter((s) => {
        // Filter by ID
        if (addedSightseeingIds.has(s.id)) return false;

        // Filter by name similarity
        return !addedAttractionNames.some((name) =>
          areAttractionsSimilar(name, s.attraction_name)
        );
      });

      if (availableAttractions.length === 0) {
        return res.status(400).json({
          message: "All attractions have already been added to this itinerary.",
        });
      }

      // Track used attractions to prevent duplicates
      const usedAttractionNames = new Set();
      const usedAttractionIds = new Set();

      // Helper function to check if attraction is already used
      const isAttractionUsed = (attraction) => {
        if (usedAttractionIds.has(attraction.id)) return true;
        return Array.from(usedAttractionNames).some((name) =>
          areAttractionsSimilar(name, attraction.attraction_name)
        );
      };

      // Helper function to mark attraction as used
      const markAttractionUsed = (attraction) => {
        usedAttractionIds.add(attraction.id);
        usedAttractionNames.add(attraction.attraction_name);
      };

      // Helper function to check if attractions are within distance
      const areWithinDistance = (
        attraction1,
        attraction2,
        maxDistance = 12
      ) => {
        if (
          !attraction1.latitude ||
          !attraction1.longitude ||
          !attraction2.latitude ||
          !attraction2.longitude
        ) {
          return false; // Can't calculate distance, assume not nearby
        }
        const distance = calculateDistance(
          attraction1.latitude,
          attraction1.longitude,
          attraction2.latitude,
          attraction2.longitude
        );
        return distance <= maxDistance;
      };

      // Classify attractions
      const fullDayAttractions = availableAttractions.filter(
        (s) => s.tag === "Full-day" && !isAttractionUsed(s)
      );
      const nightOnlyAttractions = availableAttractions.filter(
        (s) => s.tag === "Night-only" && !isAttractionUsed(s)
      );
      const halfDayAttractions = availableAttractions.filter(
        (s) => s.tag === "Half-day" && !isAttractionUsed(s)
      );
      const quickStopAttractions = availableAttractions.filter(
        (s) => s.tag === "Quick stop" && !isAttractionUsed(s)
      );
      const unclassifiedAttractions = availableAttractions.filter(
        (s) => !s.tag && !isAttractionUsed(s)
      );

      // Distribute attractions across days
      const dayAssignments = {};
      for (let day = 1; day <= numDays; day++) {
        dayAssignments[day] = [];
      }

      // DAY 1 (Arrival Day): Only activities after 5 PM, 2-3 hours duration OR Night-only tours after 6 PM
      const arrivalDayCandidates = [
        ...nightOnlyAttractions.filter(
          (s) =>
            (s.average_duration_hours || 0) >= 2 &&
            (s.average_duration_hours || 0) <= 3
        ),
        ...halfDayAttractions.filter(
          (s) =>
            (s.average_duration_hours || 0) >= 2 &&
            (s.average_duration_hours || 0) <= 3
        ),
        ...quickStopAttractions.filter(
          (s) =>
            (s.average_duration_hours || 0) >= 2 &&
            (s.average_duration_hours || 0) <= 3
        ),
        ...unclassifiedAttractions.filter(
          (s) =>
            (s.average_duration_hours || 0) >= 2 &&
            (s.average_duration_hours || 0) <= 3
        ),
      ];

      // Prefer night-only for arrival day
      const arrivalNightOnly = arrivalDayCandidates.filter(
        (s) => s.tag === "Night-only"
      );
      if (
        arrivalNightOnly.length > 0 &&
        !isAttractionUsed(arrivalNightOnly[0])
      ) {
        dayAssignments[1].push(arrivalNightOnly[0]);
        markAttractionUsed(arrivalNightOnly[0]);
      } else if (arrivalDayCandidates.length > 0) {
        // Pick one 2-3 hour activity
        const candidate = arrivalDayCandidates.find(
          (s) => !isAttractionUsed(s)
        );
        if (candidate) {
          dayAssignments[1].push(candidate);
          markAttractionUsed(candidate);
        }
      }

      // DEPARTURE DAY (Last Day): Only light 2-3 hour activities before 12 PM
      const departureDayCandidates = [
        ...halfDayAttractions.filter(
          (s) =>
            (s.average_duration_hours || 0) >= 2 &&
            (s.average_duration_hours || 0) <= 3
        ),
        ...quickStopAttractions.filter(
          (s) =>
            (s.average_duration_hours || 0) >= 2 &&
            (s.average_duration_hours || 0) <= 3
        ),
        ...unclassifiedAttractions.filter(
          (s) =>
            (s.average_duration_hours || 0) >= 2 &&
            (s.average_duration_hours || 0) <= 3
        ),
      ];

      if (departureDayCandidates.length > 0) {
        const candidate = departureDayCandidates.find(
          (s) => !isAttractionUsed(s)
        );
        if (candidate) {
          dayAssignments[numDays].push(candidate);
          markAttractionUsed(candidate);
        }
      }

      // MIDDLE DAYS: Can mix - max 1 full-day OR max 2 nearby 2-3 hour attractions within 10-15km OR max 3 nearby 3-4 hour attractions within 10km
      const middleDays =
        numDays > 2 ? Array.from({ length: numDays - 2 }, (_, i) => i + 2) : [];

      middleDays.forEach((day) => {
        let dayActivities = dayAssignments[day];
        let dayHours = dayActivities.reduce(
          (sum, a) => sum + (a.average_duration_hours || 0),
          0
        );

        // Strategy 1: Try to add 1 full-day attraction
        if (dayHours === 0) {
          const fullDayCandidate = fullDayAttractions.find(
            (s) => !isAttractionUsed(s)
          );
          if (
            fullDayCandidate &&
            (fullDayCandidate.average_duration_hours || 0) <= 8
          ) {
            dayAssignments[day].push(fullDayCandidate);
            markAttractionUsed(fullDayCandidate);
            dayHours += fullDayCandidate.average_duration_hours || 0;
            dayActivities = dayAssignments[day]; // Update reference
          }
        }

        // Strategy 2: Add nearby 2-3 hour attractions (max 2, within 12km)
        if (dayHours < 8) {
          const twoToThreeHourAttractions = [
            ...halfDayAttractions.filter(
              (s) =>
                (s.average_duration_hours || 0) >= 2 &&
                (s.average_duration_hours || 0) <= 3
            ),
            ...unclassifiedAttractions.filter(
              (s) =>
                (s.average_duration_hours || 0) >= 2 &&
                (s.average_duration_hours || 0) <= 3
            ),
          ].filter((s) => !isAttractionUsed(s));

          let addedCount = 0;
          const lastAdded =
            dayActivities.length > 0
              ? dayActivities[dayActivities.length - 1]
              : null;

          for (const candidate of twoToThreeHourAttractions) {
            if (addedCount >= 2) break;
            if (dayHours + (candidate.average_duration_hours || 0) > 8)
              continue;

            // Check distance if we have a previous attraction
            if (lastAdded && !areWithinDistance(lastAdded, candidate, 12)) {
              continue; // Skip if too far
            }

            dayAssignments[day].push(candidate);
            markAttractionUsed(candidate);
            dayHours += candidate.average_duration_hours || 0;
            addedCount++;
            dayActivities = dayAssignments[day]; // Update reference
          }
        }

        // Strategy 3: Add nearby 3-4 hour attractions (max 3, within 10km)
        if (dayHours < 8 && dayAssignments[day].length < 3) {
          const threeToFourHourAttractions = [
            ...halfDayAttractions.filter(
              (s) =>
                (s.average_duration_hours || 0) >= 3 &&
                (s.average_duration_hours || 0) <= 4
            ),
            ...unclassifiedAttractions.filter(
              (s) =>
                (s.average_duration_hours || 0) >= 3 &&
                (s.average_duration_hours || 0) <= 4
            ),
          ].filter((s) => !isAttractionUsed(s));

          let addedCount = 0;
          dayActivities = dayAssignments[day]; // Update reference
          const lastAdded =
            dayActivities.length > 0
              ? dayActivities[dayActivities.length - 1]
              : null;

          for (const candidate of threeToFourHourAttractions) {
            if (addedCount >= 3 || dayAssignments[day].length >= 3) break;
            if (dayHours + (candidate.average_duration_hours || 0) > 8)
              continue;

            // Check distance - stricter for 3-4 hour attractions (10km)
            if (lastAdded && !areWithinDistance(lastAdded, candidate, 10)) {
              continue;
            }

            dayAssignments[day].push(candidate);
            markAttractionUsed(candidate);
            dayHours += candidate.average_duration_hours || 0;
            addedCount++;
            dayActivities = dayAssignments[day]; // Update reference
          }
        }

        // Add night-only attractions if there's room (after 6 PM)
        if (dayHours < 8) {
          const nightCandidate = nightOnlyAttractions.find(
            (s) =>
              !isAttractionUsed(s) &&
              (s.average_duration_hours || 0) <= 8 - dayHours
          );
          if (nightCandidate) {
            dayAssignments[day].push(nightCandidate);
            markAttractionUsed(nightCandidate);
          }
        }
      });

      // Generate activities with proper time slots
      const generatedActivities = [];

      for (let day = 1; day <= numDays; day++) {
        const dayAttractions = dayAssignments[day];

        dayAttractions.forEach((attraction, index) => {
          let startTime = "09:00";
          let endTime = "17:00";
          const durationHours = attraction.average_duration_hours || 2;
          const durationMinutes = durationHours * 60;

          // DAY 1 (Arrival Day): Activities after 5 PM
          if (day === 1) {
            if (attraction.tag === "Night-only") {
              // Night-only tours after 6 PM
              startTime = "18:00";
            } else {
              // Other activities after 5 PM
              startTime = "17:00";
            }
            const [startH, startM] = startTime.split(":").map(Number);
            const endMinutes = startH * 60 + startM + durationMinutes;
            const endH = Math.floor(endMinutes / 60);
            const endM = endMinutes % 60;
            endTime = `${endH.toString().padStart(2, "0")}:${endM
              .toString()
              .padStart(2, "0")}`;
          }
          // DEPARTURE DAY (Last Day): Activities before 12 PM (morning only)
          else if (day === numDays) {
            // Start early morning, ensure it ends before 12 PM
            startTime = "09:00";
            const [startH, startM] = startTime.split(":").map(Number);
            const endMinutes = startH * 60 + startM + durationMinutes;
            const endH = Math.floor(endMinutes / 60);
            const endM = endMinutes % 60;

            // Ensure it doesn't go past 12 PM
            if (endH >= 12) {
              // Adjust start time backwards
              const maxEndMinutes = 12 * 60; // 12:00 PM
              const adjustedStartMinutes = maxEndMinutes - durationMinutes;
              const adjustedStartH = Math.floor(adjustedStartMinutes / 60);
              const adjustedStartM = adjustedStartMinutes % 60;
              startTime = `${adjustedStartH
                .toString()
                .padStart(2, "0")}:${adjustedStartM
                .toString()
                .padStart(2, "0")}`;
              endTime = "12:00";
            } else {
              endTime = `${endH.toString().padStart(2, "0")}:${endM
                .toString()
                .padStart(2, "0")}`;
            }
          }
          // MIDDLE DAYS: Normal scheduling
          else {
            const timeSlots = generateTimeSlots(
              attraction.opening_hours,
              attraction.best_time,
              attraction.average_duration_hours
            );
            startTime = timeSlots[0]?.start || "09:00";
            endTime = timeSlots[0]?.end || "17:00";

            // For night-only attractions, ensure they start after 6:30 PM
            if (attraction.tag === "Night-only") {
              startTime = "18:30";
              const [startH, startM] = startTime.split(":").map(Number);
              const endMinutes = startH * 60 + startM + durationMinutes;
              const endH = Math.floor(endMinutes / 60);
              const endM = endMinutes % 60;
              endTime = `${endH.toString().padStart(2, "0")}:${endM
                .toString()
                .padStart(2, "0")}`;
            } else {
              // Adjust start time based on previous activities in the same day
              if (index > 0) {
                const prevActivity = generatedActivities.filter(
                  (a) => a.day_number === day
                )[index - 1];
                if (prevActivity && prevActivity.end_time) {
                  const [prevH, prevM] = prevActivity.end_time
                    .split(":")
                    .map(Number);
                  const nextHour = prevH + 1; // Add 1 hour break
                  startTime = `${nextHour.toString().padStart(2, "0")}:${prevM
                    .toString()
                    .padStart(2, "0")}`;
                }
              }

              // Calculate end time
              const [startH, startM] = startTime.split(":").map(Number);
              const endMinutes = startH * 60 + startM + durationMinutes;
              const endH = Math.floor(endMinutes / 60);
              const endM = endMinutes % 60;
              endTime = `${endH.toString().padStart(2, "0")}:${endM
                .toString()
                .padStart(2, "0")}`;
            }
          }

          generatedActivities.push({
            name: attraction.attraction_name,
            date: getDayDate(travelDate, day),
            day_number: day,
            start_time: startTime,
            end_time: endTime,
            duration: attraction.average_duration_hours
              ? `${attraction.average_duration_hours} hours`
              : "",
            is_shared: false,
            inclusions: "",
            exclusions: "",
            image_url:
              attraction.images && attraction.images.length > 0
                ? attraction.images[0]
                : "",
            tag: attraction.tag,
            opening_hours: attraction.opening_hours,
            average_duration_hours: attraction.average_duration_hours,
            latitude: attraction.latitude,
            longitude: attraction.longitude,
            category: attraction.category,
            best_time: attraction.best_time,
            sightseeing_id: attraction.id,
            warnings: [],
          });
        });
      }

      console.log(
        `[Itinerary AI] Generated ${generatedActivities.length} activities for ${numDays} days by ${currentUser.name}`
      );

      res.json({
        success: true,
        activities: generatedActivities,
        summary: {
          total_activities: generatedActivities.length,
          days: numDays,
          by_tag: {
            "Full-day": fullDayAttractions.length,
            "Half-day": halfDayAttractions.length,
            "Night-only": nightOnlyAttractions.length,
            "Quick stop": quickStopAttractions.length,
          },
        },
      });
*/

// Endpoint: Generate suggestions for a specific day
app.post(
  "/api/itinerary/generate-day-suggestions",
  requireAuth,
  async (req, res) => {
    try {
      const currentUser = req.user;
      const {
        travelDate,
        duration,
        destination,
        dayNumber,
        existingActivities = [],
        currentDayActivities = [],
      } = req.body;

      if (!travelDate || !duration || !destination || !dayNumber) {
        return res.status(400).json({
          message:
            "travelDate, duration, destination, and dayNumber are required.",
        });
      }

      const numDays = parseDurationToDays(duration);
      if (numDays === 0 || dayNumber < 1 || dayNumber > numDays) {
        return res.status(400).json({
          message: "Invalid day number or duration.",
        });
      }

      // Calculate current day hours
      const dayHours = currentDayActivities.reduce(
        (sum, a) => sum + (a.average_duration_hours || 0),
        0
      );
      const remainingHours = Math.max(0, 8 - dayHours);

      if (remainingHours <= 0) {
        return res.status(400).json({
          message: "Day is already full (8 hours max).",
        });
      }

      // Fetch destinations
      const { data: destinations, error: destError } = await supabase
        .from("destinations")
        .select("id, name");

      if (destError) throw destError;

      const matchingDestinations = destinations.filter(
        (d) =>
          d.name.toLowerCase().includes(destination.toLowerCase()) ||
          destination.toLowerCase().includes(d.name.toLowerCase())
      );

      if (matchingDestinations.length === 0) {
        return res.status(404).json({
          message: `No destinations found matching "${destination}".`,
        });
      }

      const destIds = matchingDestinations.map((d) => d.id);

      // Fetch attractions
      const { data: sightseeing, error: sightError } = await supabase
        .from("sightseeing")
        .select("*")
        .in("destination_id", destIds);

      if (sightError) throw sightError;

      // Filter out already added attractions by ID and name similarity
      const addedSightseeingIds = new Set(
        existingActivities.map((a) => a.sightseeing_id).filter(Boolean)
      );
      const addedAttractionNames = existingActivities
        .map((a) => a.name)
        .filter(Boolean);
      const currentDayAttractionNames = currentDayActivities
        .map((a) => a.name)
        .filter(Boolean);

      let availableAttractions = sightseeing.filter((s) => {
        // Filter by ID
        if (addedSightseeingIds.has(s.id)) return false;

        // Filter by name similarity (check both existing activities and current day)
        const allNames = [
          ...addedAttractionNames,
          ...currentDayAttractionNames,
        ];
        if (
          allNames.some((name) =>
            areAttractionsSimilar(name, s.attraction_name)
          )
        ) {
          return false;
        }

        return true;
      });

      // Apply day-specific constraints
      if (dayNumber === 1) {
        // Arrival day: Only activities after 5 PM, 2-3 hours OR Night-only after 6 PM
        availableAttractions = availableAttractions.filter((s) => {
          const hours = s.average_duration_hours || 0;
          return (hours >= 2 && hours <= 3) || s.tag === "Night-only";
        });
      } else if (dayNumber === numDays) {
        // Departure day: Only light 2-3 hour activities (morning only)
        availableAttractions = availableAttractions.filter((s) => {
          const hours = s.average_duration_hours || 0;
          return hours >= 2 && hours <= 3;
        });
      }

      // Check if day already has a long attraction (â‰¥5 hours)
      const hasLongAttraction = currentDayActivities.some(
        (a) => (a.average_duration_hours || 0) >= 5
      );
      if (hasLongAttraction) {
        availableAttractions = availableAttractions.filter(
          (s) => (s.average_duration_hours || 0) < 5
        );
      }

      // Filter by remaining hours
      availableAttractions = availableAttractions.filter(
        (s) => (s.average_duration_hours || 0) <= remainingHours
      );

      if (availableAttractions.length === 0) {
        return res.status(404).json({
          message: "No suitable attractions found for this day.",
        });
      }

      // Score attractions based on geo-clustering and best_time
      const scoredAttractions = availableAttractions.map((attraction) => {
        let score = 0;

        // Geo-clustering: prefer attractions close to existing ones
        const dayActivitiesWithCoords = currentDayActivities.filter(
          (a) => a.latitude && a.longitude
        );
        if (
          dayActivitiesWithCoords.length > 0 &&
          attraction.latitude &&
          attraction.longitude
        ) {
          const minDistance = Math.min(
            ...dayActivitiesWithCoords.map((a) =>
              calculateDistance(
                a.latitude,
                a.longitude,
                attraction.latitude,
                attraction.longitude
              )
            )
          );
          if (minDistance <= 12) {
            score += 10;
          } else {
            score -= 5;
          }
        } else if (currentDayActivities.length === 0) {
          score += 5;
        }

        // Best time matching
        if (currentDayActivities.length === 0) {
          if (attraction.best_time === "Morning") score += 5;
        } else {
          const usedTimes = currentDayActivities
            .map((a) => a.best_time)
            .filter(Boolean);
          if (!usedTimes.includes(attraction.best_time)) {
            score += 3;
          }
        }

        // Tag-based scoring
        if (attraction.tag === "Half-day" || attraction.tag === "Quick stop") {
          score += 2;
        }

        return { attraction, score };
      });

      // Sort by score and select attractions to fill the day
      const sortedAttractions = scoredAttractions.sort(
        (a, b) => b.score - a.score
      );

      let currentDayHours = dayHours;
      const attractionsToAdd = [];

      for (const { attraction } of sortedAttractions) {
        const attractionHours = attraction.average_duration_hours || 0;
        if (currentDayHours + attractionHours <= 8) {
          attractionsToAdd.push(attraction);
          currentDayHours += attractionHours;

          if (attractionsToAdd.length >= 3 || currentDayHours >= 7) {
            break;
          }
        }
      }

      // Generate activities with time slots
      const generatedActivities = attractionsToAdd.map((attraction, index) => {
        const timeSlots = generateTimeSlots(
          attraction.opening_hours,
          attraction.best_time,
          attraction.average_duration_hours
        );
        const selectedSlot = timeSlots[0] || { start: "09:00", end: "17:00" };

        // Adjust start time based on existing activities
        let startTime = selectedSlot.start;
        if (currentDayActivities.length > 0 && index === 0) {
          const lastActivity =
            currentDayActivities[currentDayActivities.length - 1];
          if (lastActivity.end_time) {
            const [lastHour, lastMin] = lastActivity.end_time
              .split(":")
              .map(Number);
            const nextHour = lastHour + 1;
            startTime = `${nextHour.toString().padStart(2, "0")}:${lastMin
              .toString()
              .padStart(2, "0")}`;
          }
        }

        // Calculate end time
        const [startH, startM] = startTime.split(":").map(Number);
        const durationMinutes = (attraction.average_duration_hours || 2) * 60;
        const endMinutes = startH * 60 + startM + durationMinutes;
        const endHour = Math.floor(endMinutes / 60);
        const endMin = endMinutes % 60;
        const endTime = `${endHour.toString().padStart(2, "0")}:${endMin
          .toString()
          .padStart(2, "0")}`;

        return {
          name: attraction.attraction_name,
          date: getDayDate(travelDate, dayNumber),
          day_number: dayNumber,
          start_time: startTime,
          end_time: endTime,
          duration: attraction.average_duration_hours
            ? `${attraction.average_duration_hours} hours`
            : "",
          is_shared: false,
          inclusions: "",
          exclusions: "",
          image_url:
            attraction.images && attraction.images.length > 0
              ? attraction.images[0]
              : "",
          tag: attraction.tag,
          opening_hours: attraction.opening_hours,
          average_duration_hours: attraction.average_duration_hours,
          latitude: attraction.latitude,
          longitude: attraction.longitude,
          category: attraction.category,
          best_time: attraction.best_time,
          sightseeing_id: attraction.id,
          warnings: [],
        };
      });

      console.log(
        `[Itinerary AI] Generated ${generatedActivities.length} suggestions for Day ${dayNumber} by ${currentUser.name}`
      );

      res.json({
        success: true,
        activities: generatedActivities,
        summary: {
          day: dayNumber,
          activities_added: generatedActivities.length,
          hours_added: generatedActivities.reduce(
            (sum, a) => sum + (a.average_duration_hours || 0),
            0
          ),
        },
      });
    } catch (error) {
      console.error("Error generating day suggestions:", error);
      res.status(500).json({
        message: error.message || "Failed to generate day suggestions.",
      });
    }
  }
);

// --- PDF CLEANUP API ENDPOINT ---
// Manual cleanup endpoint (for testing/admin use)
app.post("/api/admin/cleanup-pdfs", requireAuth, async (req, res) => {
  try {
    const currentUser = req.user;

    // Only Super Admin can trigger manual cleanup
    if (currentUser.role !== "Super Admin") {
      return res.status(403).json({
        message: "Forbidden: Super Admin access required.",
      });
    }

    const { dryRun = false } = req.body;

    logger.info("Manual PDF cleanup triggered", {
      userId: currentUser.id,
      userName: currentUser.name,
      dryRun,
    });

    const result = await cleanupOldPdfs({ dryRun });

    res.json({
      success: true,
      message: dryRun
        ? "Dry run completed. No files were deleted."
        : "PDF cleanup completed.",
      result,
    });
  } catch (error) {
    logger.error("Manual PDF cleanup failed", {
      error: error.message,
      stack: error.stack,
    });
    res.status(500).json({
      success: false,
      message: `PDF cleanup failed: ${error.message}`,
    });
  }
});

// --- SERVER START ---
app.listen(PORT, () => {
  console.log(`âœ… Secure API server listening on http://localhost:${PORT}`);
  listenForManualAssignments(); // Start listening for manual assignments.
  setupGlobalListeners(); // Start the global DB listeners (leads, assignments)

  // Start PDF cleanup scheduler
  scheduleDailyCleanup();
  console.log("âœ… PDF cleanup scheduler started");

  // TBO static data refresh disabled for academy/lead-management (tboClient removed)

  // Start WhatsApp token monitoring (checks every 12 hours)
  if (WHATSAPP_TOKEN) {
    startTokenMonitoring(WHATSAPP_TOKEN, 12);
  } else {
    console.warn(
      "[CRM] âš ï¸ WHATSAPP_TOKEN not set, token monitoring disabled"
    );
  }
});
