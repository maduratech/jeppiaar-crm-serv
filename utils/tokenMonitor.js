import fetch from "node-fetch";

/**
 * WhatsApp Token Monitor
 * Monitors WhatsApp Business API token expiration using Graph API debug_token endpoint
 */

const GRAPH_API_DEBUG_TOKEN = "https://graph.facebook.com/debug_token";
const ALERT_DAYS_BEFORE_EXPIRY = 7; // Alert if token expires in 7 days or less

/**
 * Check token expiration status using Graph API debug_token endpoint
 * @param {string} accessToken - The WhatsApp access token to check
 * @returns {Promise<Object>} Token status with expiration info
 */
export async function checkTokenExpiration(accessToken) {
  try {
    if (!accessToken) {
      return {
        valid: false,
        error: "Token is missing",
        expired: true,
        expiresAt: null,
        daysUntilExpiry: null,
      };
    }

    // Use the token itself to check its status (Graph API allows this)
    const url = `${GRAPH_API_DEBUG_TOKEN}?input_token=${accessToken}&access_token=${accessToken}`;

    const response = await fetch(url);
    const result = await response.json();

    if (!response.ok || result.error) {
      // If we get an error checking the token, it might be expired
      if (result.error?.code === 190) {
        return {
          valid: false,
          error: "Token has expired",
          expired: true,
          expiresAt: null,
          daysUntilExpiry: null,
        };
      }
      return {
        valid: false,
        error: result.error?.message || "Failed to check token",
        expired: false,
        expiresAt: null,
        daysUntilExpiry: null,
      };
    }

    const data = result.data || {};
    const expiresAt = data.expires_at;
    const isValid = data.is_valid === true;

    // Calculate days until expiry
    let daysUntilExpiry = null;
    if (expiresAt) {
      const expiryDate = new Date(expiresAt * 1000); // expires_at is in seconds
      const now = new Date();
      const diffTime = expiryDate - now;
      daysUntilExpiry = Math.ceil(diffTime / (1000 * 60 * 60 * 24));
    } else if (isValid) {
      // Token doesn't expire (permanent token)
      daysUntilExpiry = Infinity;
    }

    return {
      valid: isValid,
      error: null,
      expired: !isValid || (expiresAt && expiresAt * 1000 < Date.now()),
      expiresAt: expiresAt ? new Date(expiresAt * 1000) : null,
      daysUntilExpiry,
      appId: data.app_id,
      userId: data.user_id,
      scopes: data.scopes || [],
      metadata: data,
    };
  } catch (error) {
    console.error("[Token Monitor] Error checking token:", error.message);
    return {
      valid: false,
      error: error.message,
      expired: false,
      expiresAt: null,
      daysUntilExpiry: null,
    };
  }
}

/**
 * Check token and log alert if expiring soon
 * @param {string} accessToken - The WhatsApp access token to check
 * @returns {Promise<Object>} Token status
 */
export async function checkTokenAndAlert(accessToken) {
  const status = await checkTokenExpiration(accessToken);

  if (status.expired) {
    console.error(
      "🔴 [Token Monitor] ⚠️ CRITICAL: WhatsApp token has EXPIRED!",
      status.expiresAt
        ? `Expired on: ${status.expiresAt.toLocaleString()}`
        : "Expired (unknown date)"
    );
    console.error(
      "[Token Monitor] ⚠️ Action required: Generate a new token and update WHATSAPP_TOKEN environment variable"
    );
    return status;
  }

  if (status.daysUntilExpiry === null || status.daysUntilExpiry === Infinity) {
    // Token doesn't expire or expiration unknown
    if (status.valid) {
      console.log(
        "✅ [Token Monitor] Token is valid (permanent or expiration unknown)"
      );
    }
    return status;
  }

  if (status.daysUntilExpiry <= 0) {
    console.error(
      `🔴 [Token Monitor] ⚠️ CRITICAL: Token expires TODAY or has expired! Days remaining: ${status.daysUntilExpiry}`
    );
    if (status.expiresAt) {
      console.error(
        `[Token Monitor] Expiration date: ${status.expiresAt.toLocaleString()}`
      );
    }
    console.error(
      "[Token Monitor] ⚠️ Action required: Generate a new token immediately!"
    );
  } else if (status.daysUntilExpiry <= ALERT_DAYS_BEFORE_EXPIRY) {
    console.warn(
      `🟡 [Token Monitor] ⚠️ WARNING: Token expires in ${status.daysUntilExpiry} day(s)!`,
      status.expiresAt ? `Expires on: ${status.expiresAt.toLocaleString()}` : ""
    );
    console.warn(
      `[Token Monitor] ⚠️ Action required: Generate a new token within the next ${status.daysUntilExpiry} day(s) and update WHATSAPP_TOKEN environment variable`
    );
  } else {
    console.log(
      `✅ [Token Monitor] Token is valid. Expires in ${status.daysUntilExpiry} day(s)`,
      status.expiresAt ? `(${status.expiresAt.toLocaleDateString()})` : ""
    );
  }

  return status;
}

/**
 * Start periodic token monitoring
 * @param {string} accessToken - The WhatsApp access token to monitor
 * @param {number} checkIntervalHours - How often to check (default: 12 hours)
 */
export function startTokenMonitoring(accessToken, checkIntervalHours = 12) {
  if (!accessToken) {
    console.warn("[Token Monitor] No token provided, skipping monitoring");
    return;
  }

  // Initial check
  console.log("[Token Monitor] Starting token monitoring...");
  checkTokenAndAlert(accessToken);

  // Periodic checks
  const intervalMs = checkIntervalHours * 60 * 60 * 1000;
  const intervalId = setInterval(() => {
    checkTokenAndAlert(accessToken);
  }, intervalMs);

  console.log(
    `[Token Monitor] Monitoring started. Checking every ${checkIntervalHours} hour(s)`
  );

  return intervalId;
}
