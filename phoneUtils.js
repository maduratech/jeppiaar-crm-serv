import { parsePhoneNumberFromString } from "libphonenumber-js";

/**
 * Normalize a phone number to E.164 where possible.
 * If parsing fails, attempts sensible fallbacks:
 *  - Strip non-digits and if length===10 assume India (+91)
 *  - If already starts with country code digits, prefix with +
 * Returns formatted E.164 string without spaces, or null if input invalid.
 */
export function normalizePhone(raw, defaultCountry = "IN") {
  if (!raw) return null;
  const str = String(raw).trim();

  // Try parse with libphonenumber-js
  try {
    const parsed = parsePhoneNumberFromString(str, defaultCountry);
    if (parsed && parsed.isValid && parsed.number) {
      return parsed.number; // E.164 format
    }
  } catch (err) {
    // continue to fallback
  }

  // Fallback: remove non-digits
  const digits = str.replace(/[^0-9]/g, "");
  if (!digits) return null;

  // If 10 digits, assume India
  if (digits.length === 10) {
    return `+91${digits}`;
  }

  // If starts with country code like 61, 91 etc and length reasonable, prefix +
  if (digits.length >= 11 && digits.length <= 15) {
    return `+${digits}`;
  }

  // Unable to normalize reliably
  return null;
}
