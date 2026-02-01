import express from "express";
import fetch from "node-fetch";
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";
import { normalizePhone } from "./phoneUtils.js";

// This file contains the WhatsApp CRM API endpoints for managing conversations,
// sending messages, templates, and message history.
// It is imported and mounted in the main server/index.js file.

const router = express.Router();

// -------------------------
// SUPABASE INIT
// -------------------------
const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY
);

// -------------------------
// WHATSAPP CONFIG
// -------------------------
const WHATSAPP_TOKEN = process.env.WHATSAPP_TOKEN;
const WHATSAPP_PHONE_NUMBER_ID = process.env.WHATSAPP_PHONE_NUMBER_ID;
const WHATSAPP_GRAPH_API_BASE = `https://graph.facebook.com/v20.0/${WHATSAPP_PHONE_NUMBER_ID}/messages`;

// Rate limiting: Track last message time per recipient to prevent spam
const messageRateLimiter = new Map(); // phone -> lastMessageTime
const MIN_MESSAGE_INTERVAL = 2000; // 2 seconds between messages to same recipient

// Helper to throttle messages
async function throttleMessage(recipientPhone) {
  const lastSent = messageRateLimiter.get(recipientPhone);
  if (lastSent) {
    const timeSinceLastMessage = Date.now() - lastSent;
    if (timeSinceLastMessage < MIN_MESSAGE_INTERVAL) {
      const waitTime = MIN_MESSAGE_INTERVAL - timeSinceLastMessage;
      console.log(
        `[WhatsApp CRM] [RateLimit] Throttling message to ${recipientPhone} - waiting ${waitTime}ms`
      );
      await new Promise((resolve) => setTimeout(resolve, waitTime));
    }
  }
  messageRateLimiter.set(recipientPhone, Date.now());
}

// -------------------------
// WHATSAPP SENDING FUNCTIONS (Exported for use in index.js)
// -------------------------

/**
 * Send a plain text WhatsApp message
 * @param {string} to - Phone number in E.164 format (e.g., +919841732011)
 * @param {string} text - Message text
 * @returns {Promise<Object|null>} - API response or null on error
 */
export async function sendCrmWhatsappText(to, text) {
  try {
    // Rate limiting: throttle messages to same recipient
    await throttleMessage(to);

    const payload = {
      messaging_product: "whatsapp",
      to,
      type: "text",
      text: { body: text },
    };
    console.log(`[WhatsApp CRM] 📤 Sending TEXT message to ${to}.`);
    const response = await fetch(WHATSAPP_GRAPH_API_BASE, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${WHATSAPP_TOKEN}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify(payload),
    });
    const result = await response.json();
    if (!response.ok) {
      const errorCode = result.error?.code;
      const errorTitle = result.error?.error_subtitle || result.error?.message;
      const errorType = result.error?.type;

      // Check for token expiration (error code 190)
      if (errorCode === 190 || errorType === "OAuthException") {
        console.error(
          `[WhatsApp CRM] 🔴 TOKEN EXPIRED: WhatsApp token has expired!`,
          result.error?.message || ""
        );
        console.error(
          `[WhatsApp CRM] ⚠️ Action required: Generate a new token and update WHATSAPP_TOKEN environment variable`
        );
      } else if (errorCode === 131047) {
        // Check for re-engagement error (131047)
        console.warn(
          `[WhatsApp CRM] ⚠️ Re-engagement required for ${to}. Error Code: ${errorCode}, Title: ${errorTitle}`
        );
        console.warn(
          `[WhatsApp CRM] 💡 Suggestion: Customer needs to send a message first, or use a template message if outside 24-hour window.`
        );
      } else {
        console.error(
          `[WhatsApp CRM] ❌ Message failed to send to ${to}. Reason from API:`,
          JSON.stringify(result, null, 2)
        );
      }
      return null;
    }
    console.log(
      `[WhatsApp CRM] ✅ Message sent successfully to ${to}. Message ID: ${result.messages?.[0]?.id}`
    );
    return result;
  } catch (err) {
    console.error(
      "[WhatsApp CRM] ❌ CRITICAL Error in sendCrmWhatsappText:",
      err.message
    );
    return null;
  }
}

/**
 * Send a WhatsApp message with reply buttons
 * @param {string} to - Phone number in E.164 format
 * @param {string} text - Message text
 * @param {Array} buttons - Array of button objects: [{ type: "reply", reply: { id: "id", title: "Title" } }]
 * @returns {Promise<Object|null>} - API response or null on error
 */
export async function sendCrmWhatsappReplyButtons(to, text, buttons) {
  try {
    await throttleMessage(to);

    const payload = {
      messaging_product: "whatsapp",
      to,
      type: "interactive",
      interactive: { type: "button", body: { text }, action: { buttons } },
    };
    console.log(`[WhatsApp CRM] 📤 Sending INTERACTIVE (BUTTONS) to ${to}.`);
    const response = await fetch(WHATSAPP_GRAPH_API_BASE, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${WHATSAPP_TOKEN}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify(payload),
    });
    const result = await response.json();
    if (!response.ok) {
      // Check for token expiration (error code 190)
      if (
        result.error?.code === 190 ||
        result.error?.type === "OAuthException"
      ) {
        console.error(
          `[WhatsApp CRM] 🔴 TOKEN EXPIRED: WhatsApp token has expired!`,
          result.error?.message || ""
        );
        console.error(
          `[WhatsApp CRM] ⚠️ Action required: Generate a new token and update WHATSAPP_TOKEN environment variable`
        );
      } else {
        console.error(
          `[WhatsApp CRM] ❌ Message failed to send to ${to}. Reason from API:`,
          JSON.stringify(result, null, 2)
        );
      }
      return null;
    }
    console.log(
      `[WhatsApp CRM] ✅ Message sent successfully to ${to}. Message ID: ${result.messages?.[0]?.id}`
    );
    return result;
  } catch (err) {
    console.error(
      "[WhatsApp CRM] ❌ CRITICAL Error in sendCrmWhatsappReplyButtons:",
      err.message
    );
    return null;
  }
}

/**
 * Send a WhatsApp message with CTA URL button (template with fallback)
 * @param {string} to - Phone number in E.164 format
 * @param {string} text - Message text
 * @param {string} buttonText - Button text
 * @param {string} url - URL to open
 * @param {string} agentName - Optional agent name for template
 * @returns {Promise<Object|null>} - API response or null on error
 */
export async function sendCrmWhatsappCtaUrl(
  to,
  text,
  buttonText,
  url,
  agentName = ""
) {
  try {
    await throttleMessage(to);

    // Try to send a Template message first (call_consultant). If template fails
    // (for example pending review), fall back to a standard text message with the link.
    try {
      const urlObj = new URL(url);
      const queryParams = urlObj.search.substring(1);

      const templatePayload = {
        messaging_product: "whatsapp",
        to: to,
        type: "template",
        template: {
          name: "call_consultant",
          language: { code: "en" },
          components: [
            {
              type: "body",
              parameters: [{ type: "text", text: agentName || buttonText }],
            },
            {
              type: "button",
              sub_type: "url",
              index: 0,
              parameters: [{ type: "text", text: queryParams }],
            },
          ],
        },
      };

      console.log(`[WhatsApp CRM] 📤 Attempting Template Message to ${to}`);
      const response = await fetch(WHATSAPP_GRAPH_API_BASE, {
        method: "POST",
        headers: {
          Authorization: `Bearer ${WHATSAPP_TOKEN}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify(templatePayload),
      });

      const result = await response.json();
      if (response.ok) {
        console.log(`[WhatsApp CRM] ✅ Template message sent successfully.`);
        return result;
      }

      console.warn(
        `[WhatsApp CRM] ⚠️ Template failed. Falling back to text. Reason: ${JSON.stringify(
          result
        )}`
      );
    } catch (templateErr) {
      console.warn(
        `[WhatsApp CRM] ⚠️ Template attempt errored (will fallback to text): ${templateErr.message}`
      );
    }

    // Text fallback
    const fullText = `${text}\n\n🔗 ${buttonText}: ${url}`;
    return await sendCrmWhatsappText(to, fullText);
  } catch (err) {
    console.error(
      "[WhatsApp CRM] ❌ CRITICAL Error in sendCrmWhatsappCtaUrl:",
      err.message
    );
    return null;
  }
}

/**
 * Send a WhatsApp template message
 * @param {string} to - Phone number in E.164 format
 * @param {string} templateName - Template name (e.g., "staff_lead_assigned")
 * @param {string} languageCode - Language code (default: "en")
 * @param {Array} components - Template components (body parameters, buttons, etc.)
 * @returns {Promise<Object|null>} - API response or null on error
 */
/**
 * Upload media file to WhatsApp Business API
 * @param {Buffer} fileBuffer - File buffer to upload
 * @param {string} mimeType - MIME type of the file (e.g., "application/pdf")
 * @param {string} fileName - Name of the file
 * @returns {Promise<string|null>} - Media ID or null on error
 */
export async function uploadWhatsappMedia(fileBuffer, mimeType, fileName) {
  try {
    // Use form-data package for Node.js
    const FormData = (await import("form-data")).default;
    const formData = new FormData();

    // Append file buffer
    formData.append("file", fileBuffer, {
      filename: fileName,
      contentType: mimeType,
    });
    formData.append("messaging_product", "whatsapp");
    formData.append("type", mimeType);

    const uploadUrl = `https://graph.facebook.com/v20.0/${WHATSAPP_PHONE_NUMBER_ID}/media`;

    console.log(
      `[WhatsApp CRM] 📤 Uploading media to WhatsApp: ${fileName} (${mimeType}, ${fileBuffer.length} bytes)`
    );

    const response = await fetch(uploadUrl, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${WHATSAPP_TOKEN}`,
        ...formData.getHeaders(), // Get headers for multipart/form-data
      },
      body: formData,
    });

    const result = await response.json();

    if (!response.ok) {
      console.error(
        `[WhatsApp CRM] ❌ Failed to upload media: ${
          result.error?.message || JSON.stringify(result)
        }`
      );
      return null;
    }

    const mediaId = result.id;
    console.log(
      `[WhatsApp CRM] ✅ Media uploaded successfully. Media ID: ${mediaId}`
    );
    return mediaId;
  } catch (error) {
    console.error(`[WhatsApp CRM] ❌ Error uploading media: ${error.message}`);
    return null;
  }
}

export async function sendCrmWhatsappTemplate(
  to,
  templateName,
  languageCode = "en",
  components = []
) {
  try {
    await throttleMessage(to);

    const payload = {
      messaging_product: "whatsapp",
      to,
      type: "template",
      template: {
        name: templateName,
        language: { code: languageCode },
        components: components,
      },
    };

    console.log(
      `[WhatsApp CRM] 📤 Sending template "${templateName}" to ${to}`
    );
    const response = await fetch(WHATSAPP_GRAPH_API_BASE, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${WHATSAPP_TOKEN}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify(payload),
    });

    const result = await response.json();
    if (!response.ok) {
      const errorCode = result.error?.code;
      const errorMessage = result.error?.message;
      const errorSubtitle = result.error?.error_subtitle;
      const errorType = result.error?.type;

      // Check for token expiration (error code 190)
      if (errorCode === 190 || errorType === "OAuthException") {
        console.error(
          `[WhatsApp CRM] 🔴 TOKEN EXPIRED: WhatsApp token has expired!`,
          errorMessage || ""
        );
        console.error(
          `[WhatsApp CRM] ⚠️ Action required: Generate a new token and update WHATSAPP_TOKEN environment variable`
        );
      } else {
        console.error(
          `[WhatsApp CRM] ❌ Template "${templateName}" failed to send to ${to}.`
        );
        console.error(`[WhatsApp CRM] Error Code: ${errorCode}`);
        console.error(`[WhatsApp CRM] Error Type: ${errorType}`);
        console.error(`[WhatsApp CRM] Error Message: ${errorMessage}`);
        console.error(`[WhatsApp CRM] Error Subtitle: ${errorSubtitle}`);
      }
      console.error(
        `[WhatsApp CRM] Full Error Response:`,
        JSON.stringify(result, null, 2)
      );

      // Common error codes and their meanings
      if (errorCode === 132000) {
        console.error(
          `[WhatsApp CRM] 💡 Template "${templateName}" is not approved or doesn't exist in Meta Business Manager.`
        );
        console.error(
          `[WhatsApp CRM] 💡 Please check: 1) Template name matches exactly (case-sensitive), 2) Template is approved, 3) Template language code is correct.`
        );
      } else if (errorCode === 131026) {
        console.error(
          `[WhatsApp CRM] 💡 Phone number ${to} is not registered on WhatsApp.`
        );
      } else if (errorCode === 131047) {
        console.error(
          `[WhatsApp CRM] 💡 Customer needs to send a message first (24-hour window expired).`
        );
      } else if (errorCode === 131051) {
        console.error(
          `[WhatsApp CRM] 💡 Template parameters don't match the template structure.`
        );
      }

      return null;
    }

    console.log(
      `[WhatsApp CRM] ✅ Template sent successfully. Message ID: ${result.messages?.[0]?.id}`
    );
    return result;
  } catch (err) {
    console.error(
      "[WhatsApp CRM] ❌ CRITICAL Error in sendCrmWhatsappTemplate:",
      err.message
    );
    return null;
  }
}

// -------------------------
// API ENDPOINTS
// -------------------------

// Health check endpoint to verify router is working
router.get("/api/whatsapp/health", (req, res) => {
  res
    .status(200)
    .json({ status: "ok", message: "WhatsApp CRM router is working" });
});

/**
 * GET /api/whatsapp/templates
 * Get list of available WhatsApp templates
 */
router.get("/api/whatsapp/templates", async (req, res) => {
  try {
    // Fetch templates from WhatsApp Business API
    const templatesUrl = `https://graph.facebook.com/v20.0/${WHATSAPP_PHONE_NUMBER_ID}/message_templates`;

    const response = await fetch(templatesUrl, {
      method: "GET",
      headers: {
        Authorization: `Bearer ${WHATSAPP_TOKEN}`,
      },
    });

    const result = await response.json();

    if (!response.ok) {
      // Check for token expiration (error code 190)
      if (
        result.error?.code === 190 ||
        result.error?.type === "OAuthException"
      ) {
        console.error(
          `[WhatsApp CRM] 🔴 TOKEN EXPIRED: WhatsApp token has expired!`,
          result.error?.message || ""
        );
        console.error(
          `[WhatsApp CRM] ⚠️ Action required: Generate a new token and update WHATSAPP_TOKEN environment variable`
        );
      }
      console.error("[WhatsApp CRM] Error fetching templates:", result);
      return res.status(500).json({
        message: "Failed to fetch templates",
        error: result.error?.message || "Unknown error",
      });
    }

    // Filter and format templates for easier debugging
    const templates = (result.data || []).map((template) => ({
      name: template.name,
      status: template.status,
      language: template.language,
      category: template.category,
      components: template.components?.map((comp) => ({
        type: comp.type,
        format: comp.format,
        text: comp.text,
        buttons: comp.buttons?.map((btn) => ({
          type: btn.type,
          text: btn.text,
        })),
      })),
    }));

    // Check for required templates
    const requiredTemplates = [
      "mts_summary",
      "staff_lead_assigned",
      "staff_task_assigned",
    ];
    const templateNames = templates.map((t) => t.name);
    const missingTemplates = requiredTemplates.filter(
      (name) => !templateNames.includes(name)
    );
    const approvedTemplates = templates
      .filter((t) => t.status === "APPROVED")
      .map((t) => t.name);
    const pendingTemplates = templates
      .filter((t) => t.status === "PENDING")
      .map((t) => t.name);
    const rejectedTemplates = templates
      .filter((t) => t.status === "REJECTED")
      .map((t) => t.name);

    res.status(200).json({
      templates,
      summary: {
        total: templates.length,
        approved: approvedTemplates.length,
        pending: pendingTemplates.length,
        rejected: rejectedTemplates.length,
        requiredTemplates: {
          found: requiredTemplates.filter((name) =>
            templateNames.includes(name)
          ),
          missing: missingTemplates,
        },
        approvedRequired: requiredTemplates.filter((name) =>
          approvedTemplates.includes(name)
        ),
        pendingRequired: requiredTemplates.filter((name) =>
          pendingTemplates.includes(name)
        ),
        rejectedRequired: requiredTemplates.filter((name) =>
          rejectedTemplates.includes(name)
        ),
      },
    });
  } catch (error) {
    console.error(
      "[WhatsApp CRM] Error in GET /api/whatsapp/templates:",
      error
    );
    res.status(500).json({
      message: "Internal server error",
      error: error.message,
    });
  }
});

/**
 * GET /api/whatsapp/check-phone-status
 * Check if a phone number is registered on WhatsApp
 * Query params: phone (required)
 */
router.get("/api/whatsapp/check-phone-status", async (req, res) => {
  try {
    const { phone } = req.query;

    if (!phone) {
      return res.status(400).json({
        message: "Phone number is required. Use ?phone=+919841732011",
      });
    }

    // Normalize phone number
    let sanitizedPhone = normalizePhone(phone, "IN");
    if (!sanitizedPhone) {
      // Fallback normalization
      const phoneStr = String(phone)
        .trim()
        .replace(/[\s\-\(\)]/g, "");
      if (phoneStr.startsWith("+91") || phoneStr.startsWith("919")) {
        sanitizedPhone = phoneStr.startsWith("+") ? phoneStr : `+${phoneStr}`;
      } else if (phoneStr.length === 10) {
        sanitizedPhone = `+91${phoneStr}`;
      } else {
        sanitizedPhone = phoneStr.startsWith("+") ? phoneStr : `+${phoneStr}`;
      }
    }

    console.log(`[WhatsApp CRM] Checking phone status for: ${sanitizedPhone}`);

    let waId = null;
    let registrationStatus = "unknown";
    let checkResult = null;

    try {
      const checkUrl = `https://graph.facebook.com/v20.0/${WHATSAPP_PHONE_NUMBER_ID}/contacts`;

      const checkResponse = await fetch(checkUrl, {
        method: "POST",
        headers: {
          Authorization: `Bearer ${WHATSAPP_TOKEN}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          blocking: "none",
          contacts: [sanitizedPhone],
        }),
      });

      checkResult = await checkResponse.json();
      console.log(
        `[WhatsApp CRM] API Response:`,
        JSON.stringify(checkResult, null, 2)
      );

      if (checkResult.contacts && checkResult.contacts.length > 0) {
        const contact = checkResult.contacts[0];
        waId = contact.wa_id;

        if (waId) {
          registrationStatus = "registered";
        } else {
          registrationStatus = "not_registered";
        }
      } else if (checkResult.error) {
        console.error(`[WhatsApp CRM] API Error:`, checkResult.error);

        if (checkResult.error.code === 100) {
          registrationStatus = "api_permission_error";
        } else {
          registrationStatus = "api_error";
        }
      }
    } catch (error) {
      console.error(`[WhatsApp CRM] Request failed:`, error.message);
      checkResult = { error: { message: error.message } };
      registrationStatus = "request_failed";
    }

    res.status(200).json({
      phone: sanitizedPhone,
      registrationStatus,
      waId,
      optOutStatus: "Check manually in WhatsApp Business Manager",
      details: {
        message: "Phone number status check completed. See notes below.",
        notes: [
          "Registration Status: 'registered' means the number is on WhatsApp, 'not_registered' means it's not.",
          "Opt-Out Status: Check manually in WhatsApp Business Manager dashboard.",
          "To check opt-out in dashboard:",
          "1. Go to https://business.facebook.com/",
          "2. Select your WhatsApp Business Account",
          "3. Go to Settings > Phone Numbers",
          "4. Click on your phone number",
          "5. Check 'Blocked Contacts' or 'Opted Out Contacts' section",
          "6. Search for the phone number there",
        ],
        apiResponse: checkResult,
      },
    });
  } catch (error) {
    console.error("[WhatsApp CRM] Error:", error);
    res.status(500).json({
      message: "Error checking phone status",
      error: error.message,
    });
  }
});

/**
 * GET /api/whatsapp/search-customers
 * Search customers by name or phone number
 * Query params: q (search query)
 */
router.get("/api/whatsapp/search-customers", async (req, res) => {
  try {
    const { q } = req.query;

    if (!q || q.trim().length < 2) {
      return res.status(200).json({ customers: [] });
    }

    const searchTerm = `%${q}%`;

    const { data: customers, error } = await supabase
      .from("customers")
      .select("id, first_name, last_name, phone, avatar_url")
      .not("phone", "is", null)
      .neq("phone", "")
      .or(
        `first_name.ilike.${searchTerm},last_name.ilike.${searchTerm},phone.ilike.${searchTerm}`
      )
      .limit(10);

    if (error) {
      console.error("[WhatsApp CRM] Error searching customers:", error);
      return res.status(500).json({
        message: "Failed to search customers",
        error: error.message,
      });
    }

    const suggestions = (customers || []).map((customer) => ({
      id: customer.id,
      name: `${customer.first_name} ${customer.last_name}`.trim() || "Unknown",
      phone: customer.phone,
      avatar: customer.avatar_url,
    }));

    res.status(200).json({ customers: suggestions });
  } catch (error) {
    console.error("[WhatsApp CRM] Error in search:", error);
    res.status(500).json({
      message: "Internal server error",
      error: error.message,
    });
  }
});

/**
 * POST /api/whatsapp/customers/:customerId/automation
 * Toggle automated WhatsApp flow for a customer
 * Body: { disabled: boolean }
 */
router.post(
  "/api/whatsapp/customers/:customerId/automation",
  async (req, res) => {
    try {
      const { customerId } = req.params;
      const { disabled } = req.body;

      if (typeof disabled !== "boolean") {
        return res.status(400).json({
          message: "disabled must be a boolean",
        });
      }

      // Update customer's automation setting
      const updateData = {
        whatsapp_automation_disabled: disabled,
        whatsapp_automation_disabled_until: disabled
          ? null
          : new Date().toISOString(),
      };

      const { error: updateError } = await supabase
        .from("customers")
        .update(updateData)
        .eq("id", parseInt(customerId));

      if (updateError) {
        throw updateError;
      }

      console.log(
        `[WhatsApp CRM] ${
          disabled ? "Disabled" : "Enabled"
        } automation for customer ${customerId}`
      );

      res.status(200).json({
        success: true,
        disabled,
        message: `Automation ${disabled ? "disabled" : "enabled"} successfully`,
      });
    } catch (error) {
      console.error(
        "[WhatsApp CRM] Error updating customer automation:",
        error
      );
      res.status(500).json({
        message: "Internal server error",
        error: error.message,
      });
    }
  }
);

/**
 * GET /api/whatsapp/customers/:customerId/automation
 * Get automation status for a customer
 */
router.get(
  "/api/whatsapp/customers/:customerId/automation",
  async (req, res) => {
    try {
      const { customerId } = req.params;

      const { data: customer, error } = await supabase
        .from("customers")
        .select(
          "whatsapp_automation_disabled, whatsapp_automation_disabled_until, last_staff_message_at"
        )
        .eq("id", parseInt(customerId))
        .single();

      if (error) {
        throw error;
      }

      // Check if automation should be auto-enabled (5 minutes after last staff message)
      let disabled = customer.whatsapp_automation_disabled || false;
      if (customer.last_staff_message_at) {
        const lastMessageTime = new Date(
          customer.last_staff_message_at
        ).getTime();
        const fiveMinutesAgo = Date.now() - 5 * 60 * 1000;
        if (lastMessageTime < fiveMinutesAgo) {
          // More than 5 minutes passed, auto-enable
          disabled = false;
          // Update in database
          await supabase
            .from("customers")
            .update({ whatsapp_automation_disabled: false })
            .eq("id", parseInt(customerId));
        }
      }

      res.status(200).json({
        disabled,
        disabledUntil: customer.whatsapp_automation_disabled_until,
        lastStaffMessageAt: customer.last_staff_message_at,
      });
    } catch (error) {
      console.error(
        "[WhatsApp CRM] Error fetching customer automation:",
        error
      );
      res.status(500).json({
        message: "Internal server error",
        error: error.message,
      });
    }
  }
);

export default router;
