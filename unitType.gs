// ============================================================
// UNITTYPES.GS — Prep, Load, and Sync for Unit Type bulk creation
//
//
// FUNCTIONS
// ─────────
//   prepUnitTypes()          — validates rows, maps PropertyId via Notes
//   runUnitTypeLoad()        — menu wrapper → confirmAndRun
//   executeUnitTypeLoad()    — bulk POST to /unit_types/bulk, writes JobId
//   syncUnitTypeJobStatuses() — polls /jobs, writes back UnitTypeId
// ============================================================


// ── Prep Unit Types ───────────────────────────────────────────

/**
 * Validates all non-Success rows on the Unit Types sheet.
 *
 * For each row:
 *   1. Looks up the Property's API_ID from the Properties sheet
 *      and stores it as a Note on the PropertyName cell.
 *   2. Validates that Name is present.
 *   3. Auto-generates a ReferenceId.
 *   4. Stamps Ready or Error.
 */
function prepUnitTypes() {
  const ss      = SpreadsheetApp.getActive();
  const utSheet = ss.getSheetByName('Unit Types');
  const propSheet = ss.getSheetByName('Properties');
  if (!utSheet || !propSheet) {
    SpreadsheetApp.getUi().alert("Ensure 'Unit Types' and 'Properties' sheets exist.");
    return;
  }

  const utData    = utSheet.getDataRange().getValues();
  const utHeaders = utData[0].map(h => String(h).trim());
  const h         = {};
  utHeaders.forEach((title, i) => { h[title] = i; });

  // ── Property lookup: Name → API_ID ────────────────────────
  const propData    = propSheet.getDataRange().getValues();
  const propHeaders = propData[0].map(h => String(h).trim());
  const pNameIdx    = propHeaders.indexOf('Name');
  const pApiIdx     = propHeaders.indexOf(CONFIG.API_ID_COL);

  const propLookup = {};
  propData.forEach((row, j) => {
    if (j === 0) return;
    const name = String(row[pNameIdx]).trim();
    if (name) propLookup[name] = row[pApiIdx];
  });

  const timestamp = new Date().getTime();

  for (let i = 1; i < utData.length; i++) {
    const rowNum = i + 1;
    if (utData[i][h[CONFIG.STATUS_COL]] === 'Success') continue;

    const errors   = [];
    const propName = String(utData[i][h['PropertyName']]).trim();
    const propId   = propLookup[propName];

    // ── Property mapping ──────────────────────────────────
    if (!propId) {
      errors.push('Property Name not found in Properties sheet');
    } else {
      // Store PropertyId as Note — read by executeUnitTypeLoad() at load time
      utSheet.getRange(rowNum, h['PropertyName'] + 1).setNote(propId);
    }

    // ── Name required ─────────────────────────────────────
    if (!String(utData[i][h['Name']]).trim()) errors.push('Unit Type Name is required');

    // ── Auto-generate ReferenceId — includes property name for Audit tab filters ─
    const safePropUT = propName.replace(/[^a-zA-Z0-9]/g, '_').replace(/_+/g, '_').replace(/^_|_$/g, '') || 'Property';
    utSheet.getRange(rowNum, h[CONFIG.REF_ID_COL] + 1).setValue(`${safePropUT}_UT_${timestamp}_${i}`);

    // ── Stamp status ──────────────────────────────────────
    const statusCell = utSheet.getRange(rowNum, h[CONFIG.STATUS_COL] + 1);
    if (errors.length > 0) {
      statusCell.setValue('Errors:\n• ' + errors.join('\n• '));
    } else {
      statusCell.setValue('Ready');
    }
  }

  applyConditionalRules(utSheet, utHeaders, h[CONFIG.STATUS_COL]);
  ss.toast('Unit Types Prepped.', 'Unit Types Prep');
}


// ── Execute Unit Type Load ────────────────────────────────────

function runUnitTypeLoad() {
  confirmAndRun(executeUnitTypeLoad, 'Bulk Create Unit Types');
}

/**
 * Bulk POST for unit types — all rows with Status = "Ready".
 *
 * Sends all ready rows in a single bulk payload to /unit_types/bulk.
 * The endpoint returns a JobId — actual UnitTypeIds come back
 * asynchronously. syncUnitTypeJobStatuses() resolves them.
 *
 */
function executeUnitTypeLoad() {
  const ss    = SpreadsheetApp.getActive();
  const sheet = ss.getSheetByName('Unit Types');
  const data  = sheet.getDataRange().getValues();
  const headers = data[0].map(h => String(h).trim());
  const h     = {};
  headers.forEach((title, i) => { h[title] = i; });

  // Read PropertyId UUIDs from cell Notes (set during prepUnitTypes())
  const propertyNotes = sheet.getRange(1, h['PropertyName'] + 1, data.length, 1).getNotes();

  const toNullIfEmpty = val =>
    (val === '' || val === null || val === undefined) ? null : String(val).trim();

  const payloadArray = [];
  const rowMap       = [];

  for (let i = 1; i < data.length; i++) {
    if (String(data[i][h[CONFIG.STATUS_COL]]).trim() !== 'Ready') continue;

    const propId = propertyNotes[i][0];
    if (!propId) continue;

    payloadArray.push({
      PropertyId:           propId,
      ReferenceId:          String(data[i][h[CONFIG.REF_ID_COL]]),
      Name:                 String(data[i][h['Name']]),
      SquareFeet:           castPropertyType('SquareFeet',    data[i][h['SquareFeet']]),
      Bedrooms:             castPropertyType('Bedrooms',      data[i][h['Bedrooms']]),
      Bathrooms:            castPropertyType('Bathrooms',     data[i][h['Bathrooms']]),
      CatsAllowed:          toNullIfEmpty(data[i][h['CatsAllowed']]),
      DogsAllowed:          toNullIfEmpty(data[i][h['DogsAllowed']]),
      Deposit:              castPropertyType('Deposit',       data[i][h['Deposit']]),
      MarketRent:           castPropertyType('MarketRent',    data[i][h['MarketRent']]),
      ApplicationFee:       castPropertyType('ApplicationFee', data[i][h['ApplicationFee']]),
      MarketingTitle:       toNullIfEmpty(data[i][h['MarketingTitle']]),
      MarketingDescription: toNullIfEmpty(data[i][h['MarketingDescription']]),
      YouTubeURL:           toNullIfEmpty(data[i][h['YouTubeURL']]),
      Amenities:            String(data[i][h['Amenities']] || '')
                              .split(',').map(s => s.trim()).filter(Boolean)
    });
    rowMap.push(i + 1);
  }

  if (payloadArray.length === 0) return;

  // 'Unit Types' matches ID_TYPE key → job-type (returns JobId, not direct ID)
  const result = callAppFolioAPI(CONFIG.ENDPOINTS.UNIT_TYPES, payloadArray, 'Unit Types');

  // Bulk unit types endpoint returns a JobId asynchronously —
  // check both result.data and the raw message string for it
  const jobId = (result.data && result.data.JobId)
    ? result.data.JobId
    : (result.message.match(/"JobId"\s*:\s*"([^"]+)"/) || [])[1]
    || 'Check_Logs';

  if (result.success || result.message.includes('JobId')) {
    rowMap.forEach(rowNum => {
      sheet.getRange(rowNum, h[CONFIG.STATUS_COL] + 1)
        .setValue(`Pending: ${jobId}`).setBackground('#fff2cc');
    });
  } else {
    rowMap.forEach(rowNum => {
      sheet.getRange(rowNum, h[CONFIG.STATUS_COL] + 1)
        .setValue('Error: ' + result.message).setBackground('#f4cccc');
    });
  }
}


// ── Sync Unit Type Job Statuses ───────────────────────────────

/**
 * Polls the /jobs endpoint for all rows with "Pending: {JobId}" status.
 *
 * Fetches each JobId, matches results back to rows by ReferenceId,
 * and writes the UnitTypeId to API_ID on success.
 *
 * Re-run this function if jobs are still showing as pending.
 */
// Replace the existing syncUnitTypeJobStatuses() function.
//
// FIX: Collects a resolvedMap during sync and calls
// updateLogDataAfterSync() at the end so _Log Data (sidebar viewer)
// gets updated with resolved ResourceIds instead of staying stuck
// on "⏳ Pending" after sync runs.
// Previously only the sheet tab was updated; the audit log was not.

function syncUnitTypeJobStatuses() {
  const ss    = SpreadsheetApp.getActive();
  const sheet = ss.getSheetByName('Unit Types');
  if (!sheet) return;

  const data      = sheet.getDataRange().getValues();
  const headers   = data[0].map(h => String(h).trim());
  const statusIdx = headers.indexOf(CONFIG.STATUS_COL);
  const apiIdIdx  = headers.indexOf(CONFIG.API_ID_COL);
  const refIdIdx  = headers.indexOf(CONFIG.REF_ID_COL);

  // ── Collect all Pending rows grouped by JobId ─────────────
  const pendingJobs = {};
  for (let i = 1; i < data.length; i++) {
    const status = String(data[i][statusIdx]);
    if (!status.includes('Pending:')) continue;
    const jobId = status.split('Pending:')[1].trim();
    if (!pendingJobs[jobId]) pendingJobs[jobId] = [];
    pendingJobs[jobId].push(i + 1);
  }

  const jobIds = Object.keys(pendingJobs);
  if (jobIds.length === 0) {
    ss.toast('No pending Unit Type jobs found.', 'Unit Types Sync');
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

      // Map ReferenceId → { success, id, error }
      const apiMap = {};
      job.Result.forEach(item => {
        const rId  = String(item.ReferenceId).trim();
        const id   = item.ResourceId || item.id || item.UnitTypeId || null;
        apiMap[rId] = { success: item.successful, id, error: item.error || item.message };
      });

      // resolvedItems for this job only — passed to updateLogDataAfterSync
      const resolvedItems = [];

      pendingJobs[jobId].forEach(rowNum => {
        const rowRefId = String(data[rowNum - 1][refIdIdx]).trim();
        const match    = apiMap[rowRefId];
        if (!match) return;

        if (match.success) {
          sheet.getRange(rowNum, apiIdIdx  + 1).setValue(match.id);
          sheet.getRange(rowNum, statusIdx + 1).setValue('Success').setBackground('#b6d7a8');
          resolvedItems.push({ referenceId: rowRefId, resourceId: match.id,  successful: true,  error: '' });
        } else {
          const errMsg = String(match.error || 'Unknown error');
          sheet.getRange(rowNum, statusIdx + 1)
            .setValue('Error: ' + errMsg).setBackground('#f4cccc');
          resolvedItems.push({ referenceId: rowRefId, resourceId: '', successful: false, error: errMsg });
        }
      });

      // ── Update _Log Data per-job so sidebar shows resolved IDs ─
      // Must pass the actual jobId (UUID) + array, not 'Unit Types' + object.
      if (resolvedItems.length) updateLogDataAfterSync(jobId, resolvedItems);

    } catch (e) {
      console.error('Error syncing Unit Type Job: ' + e.message);
    }
  });

  ss.toast('Unit Type Sync Complete.', 'Unit Types Sync');
}
