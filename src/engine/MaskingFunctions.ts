import * as crypto from 'crypto';

// Strategy type signature
export type MaskingStrategy = (value: any, salt: string) => any;

/**
 * Deterministic Masking for primary/foreign keys.
 * Uses HMAC-SHA256 with a salt to ensure that the same input key (e.g. user_id = 12)
 * always outputs the same masked key across all tables, preserving relational integrity
 * without exposing the raw IDs.
 */
export const deterministicMask: MaskingStrategy = (value: any, salt: string): any => {
  if (value === null || value === undefined) return value;

  const inputStr = String(value);
  const hash = crypto.createHmac('sha256', salt).update(inputStr).digest('hex');

  // If the input was originally a number, we can map it to a deterministic positive integer
  if (typeof value === 'number') {
    // Parse first 12 hex characters (48 bits) to stay safe within JavaScript's MAX_SAFE_INTEGER
    return parseInt(hash.substring(0, 12), 16) % 1000000000;
  }

  return hash;
};

/**
 * Scrambles name by generating a deterministic pseudo-random name of similar length.
 */
export const scrambleName: MaskingStrategy = (value: any, salt: string): any => {
  if (!value) return value;
  const str = String(value);

  // Deterministic shuffle using HMAC bytes to decide characters
  const hash = crypto.createHmac('sha256', salt).update(str).digest();
  const alphabets = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz';

  let result = '';
  for (let i = 0; i < str.length; i++) {
    const char = str[i];
    if (/\s/.test(char)) {
      result += ' '; // Preserve spaces
    } else {
      const index = hash[i % hash.length] % alphabets.length;
      result += alphabets[index];
    }
  }
  return result;
};

/**
 * Scrambles an email while keeping the domain name intact (e.g. test@example.com -> a8d9f@example.com)
 * so that email formatting constraints or domain-based routing logic still works.
 */
export const scrambleEmail: MaskingStrategy = (value: any, salt: string): any => {
  if (!value) return value;
  const str = String(value);
  const parts = str.split('@');
  if (parts.length !== 2) return scrambleName(value, salt); 

  const [localPart, domainPart] = parts;
  const hashedLocal = crypto.createHmac('sha256', salt).update(localPart).digest('hex').substring(0, 8);
  return `${hashedLocal}@${domainPart}`;
};

export const maskCreditCard: MaskingStrategy = (value: any): any => {
  if (!value) return value;
  const str = String(value);

  const digitsOnly = str.replace(/\D/g, '');
  if (digitsOnly.length < 4) return 'XXXX';

  const lastFour = digitsOnly.substring(digitsOnly.length - 4);

  if (str.includes('-')) {
    return `XXXX-XXXX-XXXX-${lastFour}`;
  }

  return 'X'.repeat(Math.max(0, str.length - 4)) + lastFour;
};

export const maskAadhaar: MaskingStrategy = (value: any): any => {
  if (value === null || value === undefined) {
    return value;
  }

  const str = String(value);
  const digitsOnly = str.replace(/\D/g, '');

  if (digitsOnly.length !== 12) return str;
  
  const lastFour = digitsOnly.slice(-4);

  if (str.includes(' ')) return `xxxx xxxx ${lastFour}`;
  return `xxxxxxxx${lastFour}`;
};

export const maskPan: MaskingStrategy = (value: any): any => {
  if (value == null) return value;

  const str = String(value).trim().toUpperCase();

  if (!/^[A-Z]{5}[0-9]{4}[A-Z]$/.test(str)) {
    return value;
  }

  return `XXXXXX${str.slice(-4)}`;
};

/**
 * Scrambles phone numbers while preserving formatting characters like '+' or '-'
 */
export const scramblePhone: MaskingStrategy = (value: any, salt: string): any => {
  if (!value) return value;
  const str = String(value);

  const hash = crypto.createHmac('sha256', salt).update(str).digest();
  let result = '';
  let digitIndex = 0;

  for (let i = 0; i < str.length; i++) {
    const char = str[i];
    if (/\d/.test(char)) {
      const randomDigit = hash[digitIndex % hash.length] % 10;
      result += randomDigit.toString();
      digitIndex++;
    } else {
      result += char; // Keep formatting like '+', '-', '(', ')'
    }
  }
  return result;
};

// Strategy registry dictionary lookup
const registry: Record<string, MaskingStrategy> = {
  deterministic: deterministicMask,
  scramble_name: scrambleName,
  scramble_email: scrambleEmail,
  scramble_phone: scramblePhone,
  mask_credit_card: maskCreditCard,
  mask_aadhaar: maskAadhaar,
  mask_pan: maskPan,
};

/**
 * Dynamic dispatcher function. Resolves the requested rule name at runtime.
 * Falls back to returning the original value (pass-through) if the rule name is not defined.
 */
export function applyMask(ruleName: string | undefined, value: any, salt: string): any {
  if (!ruleName) return value; // Pass-through

  const strategy = registry[ruleName];
  if (!strategy) {
    // If a user specified a rule we don't recognize, warn and pass-through to prevent data loss
    console.warn(`Warning: Unknown masking rule "${ruleName}". Passing value through unchanged.`);
    return value;
  }

  return strategy(value, salt);
}
