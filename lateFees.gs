// ============================================================
// LATE_FEES.GS — Prep, Load & Sync for Late Fee Policies
//
// Late fee policies are created at two levels:
//   Property  — applies to all charges on the property
//   Occupancy — applies to a specific occupancy (tenant) within a MFR property
//
// Both use the same /late_fee_policies/bulk endpoint.
// The difference is the ID field supplied:
//   Property-level:  PropertyId  (from Properties sheet API_ID column)
//   Occupancy-level: OccupancyId (from Tenants sheet API_ID column Note,
//                                 written by syncTenantJobStatuses)
//
// The endpoint returns a JobId (async) — rows are stamped
// "Pending: <jobId>" until the matching sync function resolves them.
//
//
// TEMPLATE COLUMNS (same on both Properties and Tenants sheets)
// ─────────────────────────────────────────────────────────────
//   Eligible Charges       → EligibleCharges      (required, enum)
//   Effective Date         → PolicyEffectiveOn     (required, future date)
//   Grace Period Type      → GracePeriodType       (required, enum)
//   Grace Days             → GracePeriodValue      (required, integer)
//   Late Fee Type          → FeeType               (required, enum)
//   Base Late Fee Amount   → FlatAmount|Percentage (conditional on FeeType)
//   Late Fee Per Day       → LateFeePerDay         (optional, decimal string)
//   Max Daily Late Fees Amount → MaxDailyLateFeesAmount (optional, decimal string)
//   Late Fee Grace Balance → LateFeeGraceBalance   (required, decimal string)
//   LateFee_Status         → tracks prep/load/sync progress
//
//
// FUNCTIONS
// ─────────
//   prepPropertyLateFees()            — validates Properties sheet LF rows
//   executePropertyLateFeeLoad()      — POSTs property-level late fee policies
//   syncPropertyLateFeeStatuses()     — polls /jobs, writes Success/Error to Properties sheet
//   prepOccupancyLateFees()           — validates Tenants sheet LF rows
//   executeOccupancyLateFeeLoad()     — POSTs occupancy-level late fee policies
//   syncOccupancyLateFeeStatuses()    — polls /jobs, writes Success/Error to Tenants sheet
// ============================================================


// ── Shared constants ──────────────────────────────────────────

const LF_STATUS_COL    = 'LateFee_Status';

const LF_COLS = {
  eligibleCharges: 'Eligible Charges',
  // No 'Effective Date' column — PolicyEffectiveOn defaults to 1st of next month at load time
  gracePeriodType: 'Grace Period Type',
  graceDays:       'Grace Days',
  feeType:         'Late Fee Type',
  baseAmount:      'Base Late Fee Amount',
  lateFeePerDay:   'Late Fee Per Day',
  maxDailyAmount:  'Max Daily Late Fees Amount',
  graceBalance:    'Late Fee Grace Balance'  // blank → defaults to "0"
};

const LF_ELIGIBLE_CHARGES  = ['Every Charge', 'All Recurring Charges', 'Only Recurring Rent'];
const LF_FEE_TYPES         = ['Flat', 'Percentage'];
const LF_GRACE_PERIOD_TYPES = ['Fixed Period', 'Fixed Day of Month'];


// ── Shared validator ─────────────────────────────────────────

/**
 * Validates a single row's late fee fields and returns an array of error strings.
 * Empty array = row is valid.
 *
 * @param {Array}  row     — one data row (sheet.getDataRange().getValues()[i])
 * @param {Object} h       — header-name → column-index map
 * @returns {string[]}
 */
function _validateLateFeeRow(row, h) {
  const errors = [];

  const eligibleCharges  = String(row[h[LF_COLS.eligibleCharges]]  || '').trim();
  const gracePeriodType  = String(row[h[LF_COLS.gracePeriodType]]  || '').trim();
  const feeType          = String(row[h[LF_COLS.feeType]]          || '').trim();
  const baseAmountRaw    = row[h[LF_COLS.baseAmount]];
  const graceBalance     = String(row[h[LF_COLS.graceBalance]]     || '').trim();
  const graceDaysRaw     = row[h[LF_COLS.graceDays]];

  // EligibleCharges
  if (!eligibleCharges) {
    errors.push('Eligible Charges is required');
  } else if (!LF_ELIGIBLE_CHARGES.includes(eligibleCharges)) {
    errors.push(`Eligible Charges must be one of: "${LF_ELIGIBLE_CHARGES.join('", "')}"`);
  }

  // PolicyEffectiveOn — not a column; defaults to 1st of next month at load time (no validation needed)

  // GracePeriodType
  if (!gracePeriodType) {
    errors.push('Grace Period Type is required');
  } else if (!LF_GRACE_PERIOD_TYPES.includes(gracePeriodType)) {
    errors.push('Grace Period Type must be "Fixed Period" or "Fixed Day of Month"');
  }

  // GracePeriodValue
  if (graceDaysRaw === '' || graceDaysRaw === null || graceDaysRaw === undefined) {
    errors.push('Grace Days is required');
  } else {
    const gd = parseInt(graceDaysRaw);
    if (isNaN(gd)) {
      errors.push('Grace Days must be an integer');
    } else if (gracePeriodType === 'Fixed Period' && (gd < 0 || gd > 365)) {
      errors.push('Grace Days must be 0–365 for Fixed Period');
    } else if (gracePeriodType === 'Fixed Day of Month' && (gd < 1 || gd > 28)) {
      errors.push('Grace Days must be 1–28 for Fixed Day of Month');
    }
  }

  // FeeType + Base Amount
  if (!feeType) {
    errors.push('Late Fee Type is required');
  } else if (!LF_FEE_TYPES.includes(feeType)) {
    errors.push('Late Fee Type must be "Flat" or "Percentage"');
  } else if (baseAmountRaw === '' || baseAmountRaw === null || baseAmountRaw === undefined) {
    errors.push(`Base Late Fee Amount is required when Late Fee Type is "${feeType}"`);
  } else if (isNaN(parseFloat(baseAmountRaw))) {
    errors.push('Base Late Fee Amount must be a number');
  }

  // LateFeeGraceBalance — blank defaults to "0"; validate only if a value is provided
  if (graceBalance && isNaN(parseFloat(graceBalance))) {
    errors.push('Late Fee Grace Balance must be a number');
  }

  return errors;
}


/**
 * Builds a late fee payload object from a validated row.
 * Caller must add the ID field (PropertyId or OccupancyId) before pushing.
 *
 * @param {Array}  row       — data row
 * @param {Object} h         — header map
 * @param {string} refId     — unique ReferenceId for this record
 * @returns {Object}
 */
function _buildLateFeePayload(row, h, refId) {
  const feeType       = String(row[h[LF_COLS.feeType]]       || '').trim();
  const baseAmountRaw = row[h[LF_COLS.baseAmount]];

  // PolicyEffectiveOn — 1st of next month (must be in the future per API)
  const now              = new Date();
  const firstOfNextMonth = new Date(now.getFullYear(), now.getMonth() + 1, 1);
  const effectiveDateStr = Utilities.formatDate(firstOfNextMonth, 'GMT', 'yyyy-MM-dd');

  // LateFeeGraceBalance — blank defaults to 0 (API expects a number, not a string)
  const graceBalanceRaw = row[h[LF_COLS.graceBalance]];
  const graceBalance    = (graceBalanceRaw === '' || graceBalanceRaw === null || graceBalanceRaw === undefined)
    ? 0
    : parseFloat(graceBalanceRaw);

  const record = {
    ReferenceId:         refId,
    EligibleCharges:     String(row[h[LF_COLS.eligibleCharges]]  || '').trim(),
    FeeType:             feeType,
    GracePeriodType:     String(row[h[LF_COLS.gracePeriodType]]  || '').trim(),
    GracePeriodValue:    parseInt(row[h[LF_COLS.graceDays]]),
    LateFeeGraceBalance: graceBalance,   // number
    PolicyEffectiveOn:   effectiveDateStr
  };

  // FeeType-conditional amount — API example shows numbers, not strings
  const baseAmt = parseFloat(baseAmountRaw) || 0;
  if (feeType === 'Flat')            record.FlatAmount  = baseAmt;
  else if (feeType === 'Percentage') record.Percentage  = baseAmt;

  // Optional numeric fields
  const perDay   = row[h[LF_COLS.lateFeePerDay]];
  const maxDaily = row[h[LF_COLS.maxDailyAmount]];
  if (perDay   !== '' && perDay   !== null && perDay   !== undefined && !isNaN(parseFloat(perDay)))   record.LateFeePerDay         = parseFloat(perDay);
  if (maxDaily !== '' && maxDaily !== null && maxDaily !== undefined && !isNaN(parseFloat(maxDaily))) record.MaxDailyLateFeesAmount = parseFloat(maxDaily);

  return record;
}


// ── Property-level Late Fees ──────────────────────────────────

/**
 * Validates the LateFee_Status column on the Properties sheet.
 *
 * Skips rows where LateFee_Status is 'Success' or all LF fields are blank
 * (indicating this property has no late fee policy to load).
 * Sets LateFee_Status to 'Ready' or 'Error: …'.
 */
function prepPropertyLateFees() {
  const ss    = SpreadsheetApp.getActive();
  const sheet = ss.getSheetByName('Properties');
  if (!sheet) { ss.toast('Properties sheet not found.', 'Late Fees Prep'); return; }

  const data    = sheet.getDataRange().getValues();
  const headers = data[0].map(h => String(h).trim());
  const h       = {};
  headers.forEach((t, i) => { h[t] = i; });

  if (h[LF_STATUS_COL] === undefined) {
    ss.toast(`"${LF_STATUS_COL}" column not found on Properties sheet.`, 'Late Fees Prep');
    return;
  }

  for (let i = 1; i < data.length; i++) {
    const rowNum = i + 1;
    if (String(data[i][h[LF_STATUS_COL]] || '').trim() === 'Success') continue;

    // If all LF fields are blank, clear status and skip (no LF for this row)
    const anyFilled = [LF_COLS.eligibleCharges, LF_COLS.gracePeriodType, LF_COLS.feeType, LF_COLS.baseAmount]
      .some(col => h[col] !== undefined && String(data[i][h[col]] || '').trim() !== '');
    if (!anyFilled) {
      sheet.getRange(rowNum, h[LF_STATUS_COL] + 1).setValue('').setBackground(null);
      continue;
    }

    const rowErrors = _validateLateFeeRow(data[i], h);

    // PropertyId must exist (needs to have been loaded first)
    const apiId = String(data[i][h[CONFIG.API_ID_COL]] || '').trim();
    if (!apiId) rowErrors.push('PropertyId (API_ID) missing — load Properties first');

    const statusCell = sheet.getRange(rowNum, h[LF_STATUS_COL] + 1);
    if (rowErrors.length > 0) {
      statusCell.setValue('Error:\n• ' + rowErrors.join('\n• ')).setBackground('#f4cccc');
    } else {
      statusCell.setValue('Ready').setBackground('#cfe2f3');
    }
  }

  applyConditionalRules(sheet, headers, h[LF_STATUS_COL]);
  ss.toast('Property Late Fees Prep Complete.', 'Late Fees Prep');
}


/**
 * Sends all "Ready" property late fee rows to the AppFolio
 * /late_fee_policies/bulk endpoint using the row's API_ID as PropertyId.
 *
 * The endpoint returns a JobId (async) — rows are stamped
 * "Pending: <jobId>" until syncPropertyLateFeeStatuses() resolves them.
 * The refId is stored in the status cell's Note for sync matching.
 */
function executePropertyLateFeeLoad() {
  const ss    = SpreadsheetApp.getActive();
  const sheet = ss.getSheetByName('Properties');
  if (!sheet) { ss.toast('Properties sheet not found.', 'Late Fees Load'); return; }

  const data    = sheet.getDataRange().getValues();
  const headers = data[0].map(h => String(h).trim());
  const h       = {};
  headers.forEach((t, i) => { h[t] = i; });

  const timestamp    = new Date().getTime();
  const payloadArray = [];
  const rowMapping   = [];   // parallel: [rowNum, ...]
  const refIdMapping = [];   // parallel: [refId, ...]

  for (let i = 1; i < data.length; i++) {
    if (String(data[i][h[LF_STATUS_COL]] || '').trim() !== 'Ready') continue;

    const propertyId = String(data[i][h[CONFIG.API_ID_COL]] || '').trim();
    if (!propertyId) continue;

    const refId       = `LateFee_Prop_${timestamp}_${i}`;
    const record      = _buildLateFeePayload(data[i], h, refId);
    record.PropertyId = propertyId;
    payloadArray.push(record);
    rowMapping.push(i + 1);
    refIdMapping.push(refId);
  }

  if (!payloadArray.length) {
    ss.toast('No "Ready" late fee rows found. Run Prep first.', 'Late Fees Load');
    return;
  }

  // suppressLog = true — we log once below after extracting JobId
  const result = callAppFolioAPI(CONFIG.ENDPOINTS.LATE_FEE_POLICIES, payloadArray, 'Prop Late Fees', 'Load', true);

  const jobId = (result.data && result.data.JobId)
    ? result.data.JobId
    : ((result.message && result.message.match(/"JobId"\s*:\s*"([^"]+)"/) || [])[1] || null);

  if ((result.success || jobId) && jobId) {
    // Stamp every submitted row as Pending; store refId in Note for sync matching
    rowMapping.forEach((rowNum, idx) => {
      const cell = sheet.getRange(rowNum, h[LF_STATUS_COL] + 1);
      cell.setValue('Pending: ' + jobId).setBackground('#fff2cc');
      cell.setNote(refIdMapping[idx]);
    });

    // Single audit log entry — job type
    const prebuiltResults = payloadArray.map((rec, idx) => ({
      referenceId: refIdMapping[idx],
      successful:  true,
      idType:      'job',
      returnedId:  jobId
    }));

    logResponse({
      action:          'Load',
      object:          'Prop Late Fees',
      recordCount:     payloadArray.length,
      request:         { data: payloadArray },
      responseText:    result.message || ('Submitted ' + payloadArray.length + ' property late fee(s)'),
      responseJson:    result.data || {},
      statusCode:      result.status || 200,
      prebuiltResults: prebuiltResults
    });

    ss.toast('Property Late Fee Load submitted. Run Sync to confirm statuses.', 'Late Fees Load');
  } else {
    // Hard error — stamp all rows with the failure message
    const errDetail = result.message || 'Unknown error';
    rowMapping.forEach(rowNum => {
      sheet.getRange(rowNum, h[LF_STATUS_COL] + 1)
        .setValue('Error: ' + errDetail).setBackground('#f4cccc');
    });
    ss.toast('Property Late Fee Load failed. See API Log for details.', 'Late Fees Load');
  }

  SpreadsheetApp.flush();
}


// ── Property Late Fee Sync ────────────────────────────────────

/**
 * Polls the AF Jobs endpoint for all "Pending: <jobId>" rows on the
 * Properties sheet (LateFee_Status column).
 *
 * On resolution:
 *   • Updates LateFee_Status to "Success" or "Error: <reason>"
 *   • Calls updateLogDataAfterSync() so the Log Viewer sidebar
 *     shows resolved results instead of the placeholder JobId
 *
 * RefIds are read from the Note on each status cell — written by
 * executePropertyLateFeeLoad() so sync can match results by ReferenceId.
 */
function syncPropertyLateFeeStatuses() {
  const ss    = SpreadsheetApp.getActive();
  const sheet = ss.getSheetByName('Properties');
  if (!sheet) { ss.toast('Properties sheet not found.', 'Late Fee Sync'); return; }

  const data    = sheet.getDataRange().getValues();
  const headers = data[0].map(h => String(h).trim());
  const lfIdx   = headers.indexOf(LF_STATUS_COL);
  if (lfIdx === -1) { ss.toast('"LateFee_Status" column not found.', 'Late Fee Sync'); return; }

  // Batch-read Notes (refIds) from the LateFee_Status column
  const statusNotes = sheet.getRange(1, lfIdx + 1, data.length, 1).getNotes();

  // Collect pending: { jobId → [{ rowNum, refId }] }
  const pendingJobs = {};
  for (let i = 1; i < data.length; i++) {
    const status = String(data[i][lfIdx] || '');
    if (!status.includes('Pending:')) continue;
    const jobId = status.split('Pending:')[1].trim();
    if (!pendingJobs[jobId]) pendingJobs[jobId] = [];
    pendingJobs[jobId].push({ rowNum: i + 1, refId: statusNotes[i][0] || '' });
  }

  const jobIds = Object.keys(pendingJobs);
  if (!jobIds.length) {
    ss.toast('No pending Property Late Fee jobs found.', 'Late Fee Sync');
    return;
  }

  jobIds.forEach(jobId => {
    const endpoint = `${CONFIG.BASE_URL}/jobs?filters[Id]=${jobId}`;
    const options  = { method: 'get', headers: getApiHeaders(), muteHttpExceptions: true };

    try {
      const resp   = UrlFetchApp.fetch(endpoint, options);
      const result = JSON.parse(resp.getContentText());
      const job    = (result.data && result.data.length > 0) ? result.data[0] : null;

      if (!job || job.Status !== 'finished' || !job.Result) return;

      // Build refId → result map from job results
      const resultMap = {};
      job.Result.forEach(item => {
        resultMap[String(item.ReferenceId || '').trim()] = {
          success: item.Successful || item.successful || false,
          error:   item.Error || item.error || item.message || ''
        };
      });

      // Write status for each pending row
      pendingJobs[jobId].forEach(({ rowNum, refId }) => {
        const statusCell = sheet.getRange(rowNum, lfIdx + 1);
        const match      = refId ? resultMap[refId] : null;

        if (match && match.success) {
          statusCell.setValue('Success').setBackground('#b6d7a8');
        } else if (match) {
          statusCell.setValue('Error: ' + (match.error || 'Unknown error')).setBackground('#f4cccc');
        } else {
          statusCell.setValue('Error: Result not found in job').setBackground('#f4cccc');
        }
      });

      // Update _Log Data for Log Viewer sidebar
      const resolvedPayload = job.Result.map(item => ({
        referenceId: String(item.ReferenceId || '').trim(),
        resourceId:  item.ResourceId  || '',
        successful:  item.Successful  || item.successful || false,
        error:       item.Error       || item.error      || ''
      }));
      updateLogDataAfterSync(jobId, resolvedPayload);

    } catch (e) {
      console.error('syncPropertyLateFeeStatuses — error on job ' + jobId + ': ' + e.message);
    }
  });

  SpreadsheetApp.flush();
  ss.toast('Property Late Fee Sync Complete.', 'Late Fee Sync');
}


// ── Occupancy-level Late Fees ─────────────────────────────────

/**
 * Validates the LateFee_Status column on the Tenants sheet.
 *
 * OccupancyId is read from the Note on the API_ID column — written by
 * syncTenantJobStatuses() after the Occupancies load + sync.
 *
 * Skips rows where LateFee_Status is 'Success' or all LF fields are blank.
 */
function prepOccupancyLateFees() {
  const ss    = SpreadsheetApp.getActive();
  const sheet = ss.getSheetByName('Tenants');
  if (!sheet) { ss.toast('Tenants sheet not found.', 'Late Fees Prep'); return; }

  const data    = sheet.getDataRange().getValues();
  const headers = data[0].map(h => String(h).trim());
  const h       = {};
  headers.forEach((t, i) => { h[t] = i; });

  if (h[LF_STATUS_COL] === undefined) {
    ss.toast(`"${LF_STATUS_COL}" column not found on Tenants sheet.`, 'Late Fees Prep');
    return;
  }

  // Batch-read OccupancyId Notes from API_ID column (written by syncTenantJobStatuses)
  const apiIdNotes = sheet.getRange(1, h[CONFIG.API_ID_COL] + 1, data.length, 1).getNotes();

  for (let i = 1; i < data.length; i++) {
    const rowNum = i + 1;
    if (String(data[i][h[LF_STATUS_COL]] || '').trim() === 'Success') continue;

    // Skip rows where all LF fields are blank
    const anyFilled = [LF_COLS.eligibleCharges, LF_COLS.gracePeriodType, LF_COLS.feeType, LF_COLS.baseAmount]
      .some(col => h[col] !== undefined && String(data[i][h[col]] || '').trim() !== '');
    if (!anyFilled) {
      sheet.getRange(rowNum, h[LF_STATUS_COL] + 1).setValue('').setBackground(null);
      continue;
    }

    const rowErrors = _validateLateFeeRow(data[i], h);

    // OccupancyId must be set (from Note on API_ID — requires prior Occupancies load + sync)
    const occupancyId = apiIdNotes[i][0];
    if (!occupancyId) {
      rowErrors.push('OccupancyId missing — run Sync on Occupancies first');
    }

    const statusCell = sheet.getRange(rowNum, h[LF_STATUS_COL] + 1);
    if (rowErrors.length > 0) {
      statusCell.setValue('Error:\n• ' + rowErrors.join('\n• ')).setBackground('#f4cccc');
    } else {
      statusCell.setValue('Ready').setBackground('#cfe2f3');
    }
  }

  applyConditionalRules(sheet, headers, h[LF_STATUS_COL]);
  ss.toast('Occupancy Late Fees Prep Complete.', 'Late Fees Prep');
}


/**
 * Sends all "Ready" occupancy late fee rows to the AppFolio
 * /late_fee_policies/bulk endpoint using the OccupancyId Note on API_ID.
 *
 * The endpoint returns a JobId (async) — rows are stamped
 * "Pending: <jobId>" until syncOccupancyLateFeeStatuses() resolves them.
 * The refId is stored in the status cell's Note for sync matching.
 */
function executeOccupancyLateFeeLoad() {
  const ss    = SpreadsheetApp.getActive();
  const sheet = ss.getSheetByName('Tenants');
  if (!sheet) { ss.toast('Tenants sheet not found.', 'Late Fees Load'); return; }

  const data    = sheet.getDataRange().getValues();
  const headers = data[0].map(h => String(h).trim());
  const h       = {};
  headers.forEach((t, i) => { h[t] = i; });

  // Batch-read OccupancyId Notes (one getRange call instead of one per row)
  const apiIdNotes = sheet.getRange(1, h[CONFIG.API_ID_COL] + 1, data.length, 1).getNotes();

  const timestamp    = new Date().getTime();
  const payloadArray = [];
  const rowMapping   = [];   // parallel: [rowNum, ...]
  const refIdMapping = [];   // parallel: [refId, ...]

  for (let i = 1; i < data.length; i++) {
    if (String(data[i][h[LF_STATUS_COL]] || '').trim() !== 'Ready') continue;

    const occupancyId = apiIdNotes[i][0];
    if (!occupancyId) continue;   // guard: skip rows whose OccupancyId was missed

    const refId        = `LateFee_Occ_${timestamp}_${i}`;
    const record       = _buildLateFeePayload(data[i], h, refId);
    record.OccupancyId = occupancyId;
    payloadArray.push(record);
    rowMapping.push(i + 1);
    refIdMapping.push(refId);
  }

  if (!payloadArray.length) {
    ss.toast('No "Ready" late fee rows found. Run Prep first.', 'Late Fees Load');
    return;
  }

  // suppressLog = true — we log once below after extracting JobId
  const result = callAppFolioAPI(CONFIG.ENDPOINTS.LATE_FEE_POLICIES, payloadArray, 'Occ Late Fees', 'Load', true);

  const jobId = (result.data && result.data.JobId)
    ? result.data.JobId
    : ((result.message && result.message.match(/"JobId"\s*:\s*"([^"]+)"/) || [])[1] || null);

  if ((result.success || jobId) && jobId) {
    // Stamp every submitted row as Pending; store refId in Note for sync matching
    rowMapping.forEach((rowNum, idx) => {
      const cell = sheet.getRange(rowNum, h[LF_STATUS_COL] + 1);
      cell.setValue('Pending: ' + jobId).setBackground('#fff2cc');
      cell.setNote(refIdMapping[idx]);
    });

    // Single audit log entry — job type
    const prebuiltResults = payloadArray.map((rec, idx) => ({
      referenceId: refIdMapping[idx],
      successful:  true,
      idType:      'job',
      returnedId:  jobId
    }));

    logResponse({
      action:          'Load',
      object:          'Occ Late Fees',
      recordCount:     payloadArray.length,
      request:         { data: payloadArray },
      responseText:    result.message || ('Submitted ' + payloadArray.length + ' occupancy late fee(s)'),
      responseJson:    result.data || {},
      statusCode:      result.status || 200,
      prebuiltResults: prebuiltResults
    });

    ss.toast('Occupancy Late Fee Load submitted. Run Sync to confirm statuses.', 'Late Fees Load');
  } else {
    // Hard error — stamp all rows with the failure message
    const errDetail = result.message || 'Unknown error';
    rowMapping.forEach(rowNum => {
      sheet.getRange(rowNum, h[LF_STATUS_COL] + 1)
        .setValue('Error: ' + errDetail).setBackground('#f4cccc');
    });
    ss.toast('Occupancy Late Fee Load failed. See API Log for details.', 'Late Fees Load');
  }

  SpreadsheetApp.flush();
}


// ── Occupancy Late Fee Sync ───────────────────────────────────

/**
 * Polls the AF Jobs endpoint for all "Pending: <jobId>" rows on the
 * Tenants sheet (LateFee_Status column).
 *
 * On resolution:
 *   • Updates LateFee_Status to "Success" or "Error: <reason>"
 *   • Calls updateLogDataAfterSync() so the Log Viewer sidebar
 *     shows resolved results instead of the placeholder JobId
 *
 * RefIds are read from the Note on each status cell — written by
 * executeOccupancyLateFeeLoad() so sync can match results by ReferenceId.
 */
function syncOccupancyLateFeeStatuses() {
  const ss    = SpreadsheetApp.getActive();
  const sheet = ss.getSheetByName('Tenants');
  if (!sheet) { ss.toast('Tenants sheet not found.', 'Late Fee Sync'); return; }

  const data    = sheet.getDataRange().getValues();
  const headers = data[0].map(h => String(h).trim());
  const lfIdx   = headers.indexOf(LF_STATUS_COL);
  if (lfIdx === -1) { ss.toast('"LateFee_Status" column not found.', 'Late Fee Sync'); return; }

  // Batch-read Notes (refIds) from the LateFee_Status column
  const statusNotes = sheet.getRange(1, lfIdx + 1, data.length, 1).getNotes();

  // Collect pending: { jobId → [{ rowNum, refId }] }
  const pendingJobs = {};
  for (let i = 1; i < data.length; i++) {
    const status = String(data[i][lfIdx] || '');
    if (!status.includes('Pending:')) continue;
    const jobId = status.split('Pending:')[1].trim();
    if (!pendingJobs[jobId]) pendingJobs[jobId] = [];
    pendingJobs[jobId].push({ rowNum: i + 1, refId: statusNotes[i][0] || '' });
  }

  const jobIds = Object.keys(pendingJobs);
  if (!jobIds.length) {
    ss.toast('No pending Occupancy Late Fee jobs found.', 'Late Fee Sync');
    return;
  }

  jobIds.forEach(jobId => {
    const endpoint = `${CONFIG.BASE_URL}/jobs?filters[Id]=${jobId}`;
    const options  = { method: 'get', headers: getApiHeaders(), muteHttpExceptions: true };

    try {
      const resp   = UrlFetchApp.fetch(endpoint, options);
      const result = JSON.parse(resp.getContentText());
      const job    = (result.data && result.data.length > 0) ? result.data[0] : null;

      if (!job || job.Status !== 'finished' || !job.Result) return;

      // Build refId → result map from job results
      const resultMap = {};
      job.Result.forEach(item => {
        resultMap[String(item.ReferenceId || '').trim()] = {
          success: item.Successful || item.successful || false,
          error:   item.Error || item.error || item.message || ''
        };
      });

      // Write status for each pending row
      pendingJobs[jobId].forEach(({ rowNum, refId }) => {
        const statusCell = sheet.getRange(rowNum, lfIdx + 1);
        const match      = refId ? resultMap[refId] : null;

        if (match && match.success) {
          statusCell.setValue('Success').setBackground('#b6d7a8');
        } else if (match) {
          statusCell.setValue('Error: ' + (match.error || 'Unknown error')).setBackground('#f4cccc');
        } else {
          statusCell.setValue('Error: Result not found in job').setBackground('#f4cccc');
        }
      });

      // Update _Log Data for Log Viewer sidebar
      const resolvedPayload = job.Result.map(item => ({
        referenceId: String(item.ReferenceId || '').trim(),
        resourceId:  item.ResourceId  || '',
        successful:  item.Successful  || item.successful || false,
        error:       item.Error       || item.error      || ''
      }));
      updateLogDataAfterSync(jobId, resolvedPayload);

    } catch (e) {
      console.error('syncOccupancyLateFeeStatuses — error on job ' + jobId + ': ' + e.message);
    }
  });

  SpreadsheetApp.flush();
  ss.toast('Occupancy Late Fee Sync Complete.', 'Late Fee Sync');
}
