// ============================================================
// APICONNECTION.GS — CONFIG, Core API Caller, Cast Utilities
//
// SCALABILITY ADDITIONS (v2)
// ──────────────────────────
//   callAppFolioAPI()    — now retries up to 3× with exponential
//                          backoff on 429 / 5xx responses.
//
//   batchWithRetry()     — helper for loaders that split large
//                          arrays into chunks. Retries each chunk
//                          independently so one failure doesn't
//                          abort the entire import.
//
//   RATE_LIMIT_PAUSE_MS  — configurable sleep duration when a
//                          429 is received. Defaults to 5 seconds.
//
// NOTE: confirmAndRun()  lives in onOpen.gs — do NOT redefine here.
// NOTE: getApiHeaders()  lives in setupLogic.gs — do NOT redefine here.
// ============================================================


const CONFIG = {
  BASE_URL: 'https://api.appfolio.com/api/v0',

  // Lazy getter so BASE_URL substitution works at call time, not definition time
  get ENDPOINTS() {
    return {
      BANKS:             `${this.BASE_URL}/bank_accounts/bulk`,
      PROPERTIES:        `${this.BASE_URL}/properties/bulk`,
      OWNERS:            `${this.BASE_URL}/owners/bulk`,
      OWNER_GROUPS:      `${this.BASE_URL}/owner_groups`,
      VENDORS:           `${this.BASE_URL}/vendors/bulk`,
      UNITS_BULK:        `${this.BASE_URL}/units/bulk`,
      UNIT_TYPES:        `${this.BASE_URL}/unit_types/bulk`,
      TENANTS_BULK:      `${this.BASE_URL}/tenants/bulk`,
      GL_ACCOUNTS:        `${this.BASE_URL}/gl_accounts`,
      RECURRING_CHARGES:  `${this.BASE_URL}/recurring_charges`,
      LATE_FEE_POLICIES:  `${this.BASE_URL}/late_fee_policies/bulk`,
      JOBS:               `${this.BASE_URL}/jobs`
    };
  },

  // ── Column name constants ──────────────────────────────────
  STATUS_COL: 'API_Status',
  REF_ID_COL: 'ReferenceId',
  API_ID_COL: 'API_ID',

  // ── Rate limit / retry config ──────────────────────────────
  // Tune these if AppFolio adjusts their limits.
  MAX_RETRIES:         3,     // Max attempts per request before giving up
  RETRY_BASE_DELAY_MS: 2000,  // Starting backoff delay (doubles each retry)
  RATE_LIMIT_PAUSE_MS: 5000,  // Extra sleep when a 429 is received

  // ── Field allowlists ───────────────────────────────────────
  REQUIRED_BANKS:      ['AccountNumber','AccountName','AccountType','BankName','RoutingNumber','BankAddress1','BankCity'],
  ALLOWED_BANKS:       ['ReferenceId','AccountNumber','AccountName','AccountType','BankName','RoutingNumber',
                        'BankAddress1','BankCity','NextCheckNumber','BankAddress2','BankState','BankZip',
                        'CompanyAddress1','CompanyAddress2','CompanyCity','CompanyState','CompanyZip'],

  REQUIRED_PROPERTIES: ['Name','ReferenceId','PropertyType','Address1','City','State','Zip',
                        'OperatingCashBankAccountId','EscrowCashBankAccountId'],
  ALLOWED_PROPERTIES:  ['ReferenceId','Name','PropertyType','Address1','Address2','City','State','Zip',
                        'YearBuilt','Class','MaintenanceLimit','ReserveFunds','ManagementStartDate',
                        'ManagementEndDate','ManagementEndReason','AdminFee','NsfFee','MaintenanceNotes',
                        'UnitEntryPreAuthorized','OperatingCashBankAccountId','EscrowCashBankAccountId',
                        'ExpensesCashBankAccountId','InsuranceExpiration','CatsAllowed','DogsAllowed'],

  ALLOWED_OWNERS:      ['ReferenceId','FirstName','LastName','CompanyName','OwnerPaidByACH',
                        'BankAccountRoutingNumber','BankAccountNumber','SavingsAccount','TaxpayerName',
                        'TaxpayerId','Send1099','OwnerConsentedToReceiveElectronic1099',
                        'Sending1099Preference','Tags','UseAlternatePayee','AlternatePayeeName',
                        'AlternatePaymentAddress1','AlternatePaymentAddress2','AlternatePaymentCity',
                        'AlternatePaymentState','AlternatePaymentPostalCode','AlternatePaymentCountryCode',
                        'PhoneNumber','Label','AdditionalDetails','IsPrimary','EmailAddress'],

  REQUIRED_TENANTS:    ['PropertyName','UnitName','FirstName','LastName','MoveIn'],

  ALLOWED_VENDORS:     ['ReferenceId','IsCompany','UseCompanyNameAsTaxpayerName','TaxpayerName',
                        'TaxpayerId','CompanyName','Send1099','FirstName','LastName','Address1',
                        'Address2','City','State','Zip','CountryCode','CompanyURL',
                        'LiabilityInsuranceExpiration','AutoInsuranceExpiration','WorkersCompExpiration',
                        'ContractExpiration','UmbrellaInsuranceExpiration','CompliantStatus',
                        'NetVendorId','PaymentTerms','UsingEomTerms','DefaultAutoAcceptWorkOrders',
                        'Tags']
};


// ── Core API Caller ──────────────────────────────────────────

/**
 * Universal POST wrapper for all AppFolio API calls.
 *
 * RETRY BEHAVIOR
 * ──────────────
 * Retries up to CONFIG.MAX_RETRIES times on:
 *   429 — Rate limited:  sleeps RATE_LIMIT_PAUSE_MS + backoff, then retries
 *   5xx — Server error:  exponential backoff (2s → 4s → 8s), then retries
 *   Network error:       same exponential backoff
 *
 * 4xx errors (except 429) are NOT retried — they indicate a payload
 * problem that won't resolve by retrying (e.g. validation failure).
 *
 * On every successful or final-failure call, writes a structured
 * entry to the audit log via logResponse() using NAMED PARAMETERS.
 *
 * @param {string}       endpoint  — Full URL from CONFIG.ENDPOINTS
 * @param {Array|Object} payload   — Record(s) to send
 * @param {string}       logObject — Entity name for audit log (e.g. 'Banks')
 * @param {string}       [logAction='Load'] — Action label for audit log
 *
 * @returns {{ success: boolean, data: *, message: string, status: number }}
 */
function callAppFolioAPI(endpoint, payload, logObject, logAction, suppressLog) {
  const action = logAction || 'Load';
  const isBulk = endpoint.toLowerCase().includes('/bulk') ||
                 endpoint.toLowerCase().includes('bank_accounts');

  const finalPayload = isBulk
    ? { data: Array.isArray(payload) ? payload : [payload] }
    : payload;

  const options = {
    method:             'post',
    contentType:        'application/json',
    headers:            getApiHeaders(),
    payload:            JSON.stringify(finalPayload),
    muteHttpExceptions: true
  };

  let responseText = '';
  let statusCode   = 0;
  let result       = {};
  let lastError    = null;

  for (let attempt = 1; attempt <= CONFIG.MAX_RETRIES; attempt++) {
    try {
      const response = UrlFetchApp.fetch(endpoint, options);
      responseText   = response.getContentText();
      statusCode     = response.getResponseCode();

      try   { result = JSON.parse(responseText); }
      catch (_) { result = { message: responseText }; }

      if (statusCode === 429) {
        const sleep = CONFIG.RATE_LIMIT_PAUSE_MS + (attempt * CONFIG.RETRY_BASE_DELAY_MS);
        if (attempt < CONFIG.MAX_RETRIES) { Utilities.sleep(sleep); continue; }
        lastError = `Rate limit (429) after ${CONFIG.MAX_RETRIES} attempts`;
        break;
      }

      if (statusCode >= 500) {
        const sleep = CONFIG.RETRY_BASE_DELAY_MS * Math.pow(2, attempt - 1);
        if (attempt < CONFIG.MAX_RETRIES) { Utilities.sleep(sleep); continue; }
        lastError = `Server error (${statusCode}) after ${CONFIG.MAX_RETRIES} attempts`;
        break;
      }
      lastError = null;
      break;
    } catch (networkErr) {
      lastError = networkErr.message;
      const sleep = CONFIG.RETRY_BASE_DELAY_MS * Math.pow(2, attempt - 1);
      if (attempt < CONFIG.MAX_RETRIES) { Utilities.sleep(sleep); continue; }
      statusCode = 500;
      responseText = lastError;
      result = { message: lastError };
    }
  }

  // ── IMPORTANT: Generate Log ID here so we can return it ──
  const logId = Utilities.getUuid();

  // suppressLog = true when the caller will aggregate multiple chunks
  // into a single logResponse() call at the end (batching pattern).
  if (!suppressLog && typeof logResponse === 'function') {
    logResponse({
      logId:        logId, // Pass the generated ID
      action:       action,
      object:       logObject || '',
      request:      finalPayload,
      responseText: responseText,
      responseJson: result,
      statusCode:   statusCode
    });
  }

  const overallSuccess = !lastError && statusCode >= 200 && statusCode < 300;
  return {
    success: overallSuccess,
    data:    result.data || result,
    message: lastError || responseText || ('(empty response HTTP ' + statusCode + ')'),
    status:  statusCode,
    logId:   logId // Return the logId to the caller
  };
}


// ── Batch with Retry Helper ───────────────────────────────────

/**
 * Splits a large payload array into chunks and calls callAppFolioAPI()
 * for each chunk. Designed for loaders handling hundreds to thousands
 * of rows — lets each chunk fail or succeed independently.
 *
 * WHY THIS MATTERS FOR SCALE
 * ──────────────────────────
 * The old pattern (one giant array → one API call) means a single
 * bad record at row 847 can fail the entire 1000-row import.
 * batchWithRetry() fails only the chunk containing the bad record
 * and stamps those rows with errors, leaving the rest as Success.
 *
 * USAGE
 * ─────
 *   const results = batchWithRetry({
 *     endpoint:   CONFIG.ENDPOINTS.OWNERS,
 *     payloads:   ownerArray,       // full array of records
 *     rowNums:    readyRowIndices,  // parallel array of sheet row numbers
 *     logObject:  'Owners',
 *     chunkSize:  40,               // optional, default 40
 *     onChunkDone: ({ chunkResult, chunkRows }) => {
 *       // write status back to sheet for this chunk immediately
 *     }
 *   });
 *
 * @param {Object} opts
 *   @param {string}   opts.endpoint     — API endpoint URL
 *   @param {Array}    opts.payloads     — All records to send
 *   @param {Array}    opts.rowNums      — Parallel sheet row numbers
 *   @param {string}   opts.logObject    — Audit log entity name
 *   @param {number}   [opts.chunkSize]  — Records per batch (default 40)
 *   @param {Function} [opts.onChunkDone] — Callback after each chunk
 *
 * @returns {Array} Array of { chunkResult, chunkRows } per chunk
 */
function batchWithRetry({ endpoint, payloads, rowNums, logObject, chunkSize = 40, onChunkDone }) {
  if (!payloads || !payloads.length) return [];

  const results = [];

  for (let i = 0; i < payloads.length; i += chunkSize) {
    const chunkPayloads = payloads.slice(i, i + chunkSize);
    const chunkRows     = rowNums  ? rowNums.slice(i, i + chunkSize) : [];

    const chunkResult = callAppFolioAPI(endpoint, chunkPayloads, logObject);

    const entry = { chunkResult, chunkRows };
    results.push(entry);

    if (typeof onChunkDone === 'function') {
      onChunkDone(entry);
    }

    // Brief pause between chunks to be a good API citizen.
    // 300ms keeps a 1000-row import well within 6-minute execution limits.
    if (i + chunkSize < payloads.length) {
      Utilities.sleep(300);
    }

    SpreadsheetApp.flush(); // Ensure sheet writes from onChunkDone are visible
  }

  return results;
}


// ── Type Coercion ────────────────────────────────────────────

/**
 * Casts a raw sheet value to the type AppFolio expects for a given field.
 *
 * Returns null for blank/null/undefined — callers should omit null
 * fields from the payload rather than sending null explicitly.
 *
 * @param {string} key — API field name (used for type-list lookup)
 * @param {*}      val — Raw cell value from the sheet
 * @returns {string|number|boolean|null}
 */
function castPropertyType(key, val) {
  const NUMERIC = [
    'FlatAmount','Percentage','MaintenanceLimit','ReserveFunds',
    'AdminFee','NsfFee','YearBuilt','Minimum','Maximum',
    'BaseLateFeeAmount','MaxDailyLateFeesAmount','MarketRent',
    'SquareFeet','Bedrooms','Bathrooms','Deposit','ApplicationFee',
    'PaymentAmount'
  ];
  const BOOLEAN = [
    'UnitEntryPreAuthorized','Waive','WaiveWhenVacant','OwnerPaidByACH',
    'UseOnlinePayables','SavingsAccount','Send1099',
    'OwnerConsentedToReceiveElectronic1099','UseAlternatePayee',
    'IsPrimary','Hidden?'
  ];

  if (val === '' || val === null || val === undefined) return null;

  if (val instanceof Date) {
    return Utilities.formatDate(val, 'GMT', 'yyyy-MM-dd');
  }

  const s = String(val).trim();

  if (BOOLEAN.includes(key) || key.includes('Waive')) {
    return s.toLowerCase() === 'true' || s.toLowerCase() === 'yes' || val === true;
  }

  if (NUMERIC.includes(key)) {
    const n = parseFloat(s.replace(/[^\d.-]/g, ''));
    return isNaN(n) ? 0 : n;
  }

  return s;
}


// ── Conditional Formatting ───────────────────────────────────

/**
 * Colors the status column after every Prep or Load run.
 *
 * Color key:
 *   Blue  #cfe2f3 — Ready / Ready for Group Load
 *   Green #b6d7a8 — Success
 *   Red   #f4cccc — Error
 *   Amber #fff2cc — Pending
 *   null  (clear) — anything else
 *
 * Uses a single setBackgrounds() call on the whole range to avoid
 * per-row getRange() calls, which are very slow on large sheets.
 *
 * @param {Sheet}  sheet     — Google Sheet object
 * @param {Array}  headers   — Row-1 header strings (kept for API consistency)
 * @param {number} statusIdx — Zero-based column index of the status column
 */
function applyConditionalRules(sheet, headers, statusIdx) {
  const lastRow = sheet.getLastRow();
  if (lastRow <= 1) return;

  // Single getValues() call — much faster than per-row reads on large sheets
  const range  = sheet.getRange(2, statusIdx + 1, lastRow - 1, 1);
  const values = range.getValues();

  const colors = values.map(([val]) => {
    const s = String(val);
    if (s === 'Ready' || s === 'Ready for Group Load') return ['#cfe2f3'];
    if (s === 'Success')                               return ['#b6d7a8'];
    if (s.startsWith('Error'))                         return ['#f4cccc'];
    if (s.startsWith('Pending'))                       return ['#fff2cc'];
    return [null];
  });

  // Single setBackgrounds() call — O(1) API calls regardless of row count
  range.setBackgrounds(colors);
}


// Add this function to APIconnection.gs.
// Used by all load scripts when writing errors to API_Status cells.
//
// Handles every error shape AppFolio returns:
//   1. rowResult.errors[]       — per-record validation array  → joins .message fields
//   2. rowResult.message        — single string message        → uses directly
//   3. raw JSON string fallback — full response text           → parses and extracts
//   4. plain string fallback    — already readable             → uses directly

/**
 * Extracts a human-readable error string from any AppFolio
 * error response shape.
 *
 * @param {Object|null} rowResult      — Per-row result object (may be null)
 * @param {string}      fallbackMsg    — Raw message/responseText from callAppFolioAPI
 * @param {number}      [maxLen=120]   — Truncate to this length
 * @returns {string}
 */
function extractErrorMessage(rowResult, fallbackMsg, maxLen) {
  const limit = maxLen || 120;

  // ── 1. Per-record errors array ────────────────────────────
  if (rowResult && Array.isArray(rowResult.errors) && rowResult.errors.length) {
    const msg = rowResult.errors.map(e => e.message || e.attribute || '').filter(Boolean).join(', ');
    if (msg) return msg.substring(0, limit);
  }

  // ── 2. Single message on rowResult ───────────────────────
  if (rowResult && rowResult.message) {
    return String(rowResult.message).substring(0, limit);
  }

  // ── 3. Parse JSON fallback string ────────────────────────
  // callAppFolioAPI returns responseText as message on failure.
  // Try to find a readable message inside it.
  if (fallbackMsg) {
    try {
      const parsed = JSON.parse(fallbackMsg);

      // Top-level errors array: [{ errors: [{ message }] }]
      const items = parsed.errors || parsed.data || (Array.isArray(parsed) ? parsed : []);
      const messages = [];
      items.forEach(item => {
        if (Array.isArray(item.errors)) {
          item.errors.forEach(e => { if (e.message) messages.push(e.message); });
        } else if (item.message) {
          messages.push(item.message);
        }
      });

      if (messages.length) return messages.join(', ').substring(0, limit);

      // Top-level message field
      if (parsed.message) return String(parsed.message).substring(0, limit);

    } catch (_) {
      // Not JSON — use as-is
    }

    // ── 4. Plain string fallback ──────────────────────────
    return String(fallbackMsg).substring(0, limit);
  }

  return 'Unknown error';
}
