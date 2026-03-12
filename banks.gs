// ============================================================
// BANKS.GS — Bank Account Prep & Load
//
// SCALABILITY NOTE
// ────────────────
// executeBankLoad() now uses batchWithRetry() (from APIconnection.gs)
// instead of a single bulk API call. Results are written back to the
// sheet per-chunk so a failure at chunk 5 doesn't erase the
// successful writes from chunks 1–4.
//
// FIELD RULES (enforced in prepBanks)
// ─────────────────────────────────────
//   ReferenceId     — auto-generated if blank
//   AccountNumber   — required, digits only, 4–17 chars
//   AccountName     — required, non-blank
//   AccountType     — required, exact enum match
//   BankName        — required, non-blank
//   RoutingNumber   — required, exactly 9 digits
//   BankAddress1    — required, non-blank
//   BankCity        — required, non-blank
//   BankState       — optional, 2-letter US state if provided
//   BankZip         — optional, 5 or 9-digit ZIP if provided
//   NextCheckNumber — optional, positive integer > 0
//   CompanyCity/State/Zip — all three required if any one present
// ============================================================


var BANK_ACCOUNT_TYPES = [
  'Personal Checking',
  'Personal Savings',
  'Business Checking',
  'Business Saving',
  'Other'
];

var US_STATES = [
  'AL','AK','AZ','AR','CA','CO','CT','DE','FL','GA',
  'HI','ID','IL','IN','IA','KS','KY','LA','ME','MD',
  'MA','MI','MN','MS','MO','MT','NE','NV','NH','NJ',
  'NM','NY','NC','ND','OH','OK','OR','PA','RI','SC',
  'SD','TN','TX','UT','VT','VA','WA','WV','WI','WY',
  'DC','PR','VI','GU','MP','AS'
];


// ── Prep ─────────────────────────────────────────────────────

/**
 * Validates every non-Success row against the API spec and stamps
 * each row with "Ready" or a descriptive error message.
 *
 * All errors on a row are collected and shown together so the
 * user can fix everything in one pass.
 *
 * Performance note: reads data once, writes status cells in a
 * single pass. For very large sheets (1000+ rows) consider
 * batching the status writes via setValues() on a range rather
 * than per-cell setValue() calls.
 */
function prepBanks() {
  const ss    = SpreadsheetApp.getActive();
  const sheet = ss.getSheetByName('Banks');
  if (!sheet) {
    ss.toast('Banks sheet not found.', 'Prep Error');
    return;
  }

  const data      = sheet.getDataRange().getValues();
  const headers   = data[0].map(h => String(h).trim());
  const statusIdx = headers.indexOf(CONFIG.STATUS_COL);
  const refIdIdx  = headers.indexOf(CONFIG.REF_ID_COL);

  if (statusIdx === -1) {
    ss.toast('API_Status column not found in Banks sheet.', 'Prep Error');
    return;
  }

  const get = (row, col) => {
    const idx = headers.indexOf(col);
    return idx === -1 ? '' : String(data[row][idx] || '').trim();
  };

  const timestamp = new Date().getTime();

  // ── Collect all status writes into arrays for a single batch
  // setValues() call at the end instead of per-row setValue().
  // This is significantly faster for sheets with hundreds of rows.
  const statusWrites = []; // { rowNum, value }
  const refIdWrites  = []; // { rowNum, value }

  for (let i = 1; i < data.length; i++) {
    if (data[i][statusIdx] === 'Success') continue;
    if (data[i].filter(v => String(v).trim() !== '').length === 0) continue;

    const errors = [];

    // ── 1. Auto-generate ReferenceId if blank ────────────────
    if (refIdIdx !== -1 && !data[i][refIdIdx]) {
      const safeName = get(i, 'AccountName').replace(/\W/g, '_') || 'Bank';
      refIdWrites.push({ rowNum: i + 1, value: `${safeName}_${timestamp}_${i}` });
    }

    // ── 2. Required fields ───────────────────────────────────
    ['AccountNumber','AccountName','AccountType','BankName',
     'RoutingNumber','BankAddress1','BankCity'].forEach(f => {
      if (!get(i, f)) errors.push('Missing ' + f);
    });

    // ── 3. AccountType enum ──────────────────────────────────
    const acctType = get(i, 'AccountType');
    if (acctType && !BANK_ACCOUNT_TYPES.includes(acctType)) {
      errors.push(`AccountType "${acctType}" invalid. Must be: ${BANK_ACCOUNT_TYPES.join(', ')}`);
    }

    // ── 4. RoutingNumber — exactly 9 digits ──────────────────
    const routing = get(i, 'RoutingNumber').replace(/\s/g, '');
    if (routing && !/^\d{9}$/.test(routing)) {
      errors.push(`RoutingNumber must be exactly 9 digits (got ${routing.length} chars: "${routing}")`);
    }

    // ── 5. AccountNumber — digits only, 4–17 chars ───────────
    const acctNum = get(i, 'AccountNumber').replace(/\s/g, '');
    if (acctNum) {
      if (!/^\d+$/.test(acctNum)) {
        errors.push(`AccountNumber must contain digits only (got "${acctNum}")`);
      } else if (acctNum.length < 4 || acctNum.length > 17) {
        errors.push(`AccountNumber must be 4–17 digits (got ${acctNum.length})`);
      }
    }

    // ── 6. BankState — 2-letter code if provided ─────────────
    const bankState = get(i, 'BankState');
    if (bankState && !US_STATES.includes(bankState.toUpperCase())) {
      errors.push(`BankState "${bankState}" is not a valid 2-letter US state code`);
    }

    // ── 7. BankZip — 5 or 9-digit if provided ────────────────
    const bankZip = get(i, 'BankZip').replace(/\s/g, '');
    if (bankZip && !/^\d{5}(-?\d{4})?$/.test(bankZip)) {
      errors.push(`BankZip "${bankZip}" must be 5 digits or ZIP+4 format`);
    }

    // ── 8. NextCheckNumber — positive integer > 0 ────────────
    const nextCheck = get(i, 'NextCheckNumber');
    if (nextCheck !== '') {
      const parsed = Number(nextCheck);
      if (!Number.isInteger(parsed) || parsed <= 0) {
        errors.push(`NextCheckNumber must be a positive integer > 0 (got "${nextCheck}")`);
      }
    }

    // ── 9. Company address co-dependency ─────────────────────
    const compCity  = get(i, 'CompanyCity');
    const compState = get(i, 'CompanyState');
    const compZip   = get(i, 'CompanyZip');
    const compCount = [compCity, compState, compZip].filter(v => v !== '').length;

    if (compCount > 0 && compCount < 3) {
      const missing = [];
      if (!compCity)  missing.push('CompanyCity');
      if (!compState) missing.push('CompanyState');
      if (!compZip)   missing.push('CompanyZip');
      errors.push(
        'Company address incomplete — CompanyCity, CompanyState, and CompanyZip ' +
        'are all required together. Missing: ' + missing.join(', ')
      );
    }

    // ── 10. CompanyState — 2-letter code if provided ─────────
    if (compState && !US_STATES.includes(compState.toUpperCase())) {
      errors.push(`CompanyState "${compState}" is not a valid 2-letter US state code`);
    }

    // ── 11. CompanyZip — 5 or 9-digit if provided ────────────
    const compZipClean = compZip.replace(/\s/g, '');
    if (compZipClean && !/^\d{5}(-?\d{4})?$/.test(compZipClean)) {
      errors.push(`CompanyZip "${compZipClean}" must be 5 digits or ZIP+4 format`);
    }

    statusWrites.push({
      rowNum: i + 1,
      value:  errors.length > 0 ? 'Error: ' + errors.join(' | ') : 'Ready'
    });
  }

  // ── Batch write ReferenceIds ──────────────────────────────
  // Per-cell writes are very slow at scale. Write each ref ID
  // individually only because they have auto-generated unique values.
  // For status (same-column writes) we batch below.
  refIdWrites.forEach(w => {
    sheet.getRange(w.rowNum, refIdIdx + 1).setValue(w.value);
  });

  // ── Batch write status column ─────────────────────────────
  // Build a full column array and write in one setValues() call.
  if (statusWrites.length > 0) {
    // Find the min/max rows touched so we write a contiguous range.
    const minRow = Math.min(...statusWrites.map(w => w.rowNum));
    const maxRow = Math.max(...statusWrites.map(w => w.rowNum));
    const rangeHeight = maxRow - minRow + 1;

    // Read current values for the range (to preserve rows we didn't touch)
    const currentStatuses = sheet
      .getRange(minRow, statusIdx + 1, rangeHeight, 1)
      .getValues();

    // Overlay our writes onto the current values
    const writeMap = {};
    statusWrites.forEach(w => { writeMap[w.rowNum] = w.value; });

    for (let r = 0; r < rangeHeight; r++) {
      const absRow = minRow + r;
      if (writeMap[absRow] !== undefined) {
        currentStatuses[r][0] = writeMap[absRow];
      }
    }

    sheet.getRange(minRow, statusIdx + 1, rangeHeight, 1).setValues(currentStatuses);
  }

  applyConditionalRules(sheet, headers, statusIdx);
  ss.toast('Prep complete. Review any Error rows before loading.', 'Banks Prep');
}


// ── Execute Load ─────────────────────────────────────────────

/**
 * Sends all "Ready" rows to the AppFolio bulk endpoint.
 *
 * Uses batchWithRetry() to split the payload into chunks of 40.
 * Results are written back to the sheet immediately after each
 * chunk so progress is visible and partial failures are isolated.
 *
 * A 1000-row Banks sheet will run ~25 API calls.
 * At 300ms between chunks that's ~7.5 seconds of sleep total,
 * well within Google's 6-minute execution limit.
 */
function executeBankLoad() {
  const ss    = SpreadsheetApp.getActive();
  const sheet = ss.getSheetByName('Banks');
  if (!sheet) {
    ss.toast('Banks sheet not found.', 'Load Error');
    return;
  }

  const data    = sheet.getDataRange().getValues();
  const headers = data[0].map(h => String(h).trim());

  const statusIdx = headers.indexOf(CONFIG.STATUS_COL);
  const apiIdIdx  = headers.indexOf(CONFIG.API_ID_COL);

  // ── Collect Ready rows ───────────────────────────────────
  const allPayloads = [];
  const allRowNums  = [];

  for (let i = 1; i < data.length; i++) {
    if (data[i][statusIdx] !== 'Ready') continue;

    const record = {};
    headers.forEach((hName, colIdx) => {
      if (CONFIG.ALLOWED_BANKS.includes(hName) && data[i][colIdx] !== '') {
        record[hName] = castPropertyType(hName, data[i][colIdx]);
      }
    });

    allPayloads.push(record);
    allRowNums.push(i + 1);
  }

  if (!allPayloads.length) {
    ss.toast('No "Ready" rows found. Run Prep first.', 'Nothing to load');
    return;
  }

  // ── Batch load with per-chunk write-back ─────────────────
  // onChunkDone fires after each chunk so sheet is updated
  // incrementally — if the job is interrupted, completed chunks
  // are already written as Success.
  let totalSuccess = 0;
  let totalError   = 0;

  batchWithRetry({
    endpoint:   CONFIG.ENDPOINTS.BANKS,
    payloads:   allPayloads,
    rowNums:    allRowNums,
    logObject:  'Banks',
    chunkSize:  40,
    onChunkDone: ({ chunkResult, chunkRows }) => {
  chunkRows.forEach((rowNum, index) => {
    const rowResult = chunkResult.data && chunkResult.data[index]
      ? chunkResult.data[index]
      : null;

    if (chunkResult.success && rowResult && rowResult.successful !== false) {
      sheet.getRange(rowNum, statusIdx + 1).setValue('Success').setBackground('#b6d7a8');
      sheet.getRange(rowNum, apiIdIdx  + 1).setValue(rowResult.BankAccountId || 'Success');
      totalSuccess++;
    } else {
      // extractErrorMessage() parses any AF error shape down to a clean string
      const errMsg = extractErrorMessage(rowResult, chunkResult.message);
      sheet.getRange(rowNum, statusIdx + 1).setValue('Error: ' + errMsg).setBackground('#f4cccc');
      totalError++;
    }
  });
}
  });

  ss.toast(
    `${totalSuccess} succeeded, ${totalError} errors. Check API Log for details.`,
    'Banks Load Complete'
  );
}
