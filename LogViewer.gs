// ============================================================
// LOGVIEWER.GS — Audit Tab Data Providers + Sync Bridge
//
// FIXES (v2)
// ──────────
//   1. getLogEntries() — Environment now included in dropdown
//      label. Cross-references API Log col I (logId) → col C (env)
//      in a single pass so the lookup is O(n) not O(n²).
//
//   2. syncPendingJobs() — After resolving job results, now calls
//      _writeSyncResultsToSheet(object, resolvedMap) to update the
//      actual object sheet tab (API_Status, API_ID columns) in
//      addition to updating _Log Data. Previously only _Log Data
//      was written, leaving the sheet tab stuck on "Pending: <jobId>".
//
// ARCHITECTURE
// ────────────
//   _Log Data sheet  — source of truth for structured log entries
//                      (written by logSystem.gs logResponse())
//   API Log sheet    — presentational + env/user cross-reference
//   getLogEntries()  — returns dropdown list for Audit tab
//   getLogEntry(id)  — returns single entry detail for Audit viewer
//   syncPendingJobs()— polls /jobs for pending records, updates both
//                      _Log Data AND the object sheet tab
//   updateLogDataAfterSync() — called by syncXxx functions after job
//                              resolution to write ResourceIds back
//
// RESULT RECORD SHAPE (returned to sidebar)
// ──────────────────────────────────────────
//   {
//     referenceId:  string,
//     successful:   true | false | null,  // null = pending
//     returnedId:   string,               // AF ResourceId (if resolved)
//     jobId:        string,               // JobId (if pending)
//     errorMessage: string                // Error detail (if failed)
//   }
// ============================================================


// ── Get Log Entry List ────────────────────────────────────────

/**
 * Returns an array of log entry descriptors for the Audit tab dropdown.
 * Reads from _Log Data for structured data, cross-references API Log
 * (col I = logId) to pull Environment (col C) per entry.
 *
 * FIX: Environment is now included in each dropdown label so the
 * user can distinguish IMPORT vs LIVE entries at a glance.
 * Label format: "IMPORT | Owners | Load | 05/27/25 2:34 PM"
 *
 * Returns up to 100 most recent entries (newest first).
 *
 * @returns {Array<{ logId, label }>}
 */
function getLogEntries() {
  const ss        = SpreadsheetApp.getActive();
  const dataSheet = ss.getSheetByName('_Log Data');
  if (!dataSheet) return [];

  const rows    = dataSheet.getDataRange().getValues();
  const headers = rows[0].map(h => String(h).trim());

  const idIdx  = headers.indexOf('Log ID');
  const tsIdx  = headers.indexOf('Timestamp');
  const objIdx = headers.indexOf('Object');
  const actIdx = headers.indexOf('Action');

  if (idIdx === -1) return [];

  // ── Build logId → env map from API Log in one pass ───────
  // API Log: col B = user (idx 1), col C = env (idx 2), col I = logId (idx 8)
  // This is O(n) rather than calling _getEnvAndUserFromLog() per row
  // which would be O(n²) on large logs.
  const envMap = {};
  const logSheet = ss.getSheetByName('API Log');
  if (logSheet) {
    const logRows = logSheet.getDataRange().getValues();
    for (let i = 1; i < logRows.length; i++) {
      const lId = String(logRows[i][8] || '').trim();   // col I
      const env = String(logRows[i][2] || '').trim();   // col C
      if (lId) envMap[lId] = env || 'IMPORT';
    }
  }

  // ── Collect entries newest-first ─────────────────────────
  // Row 2 in _Log Data = most recent due to insertRowAfter(1) pattern.
  const entries = [];
  for (let i = 1; i < rows.length && entries.length < 100; i++) {
    const logId  = String(rows[i][idIdx]  || '').trim();
    const ts     = String(rows[i][tsIdx]  || '').trim();
    const obj    = String(rows[i][objIdx] || '').trim();
    const action = String(rows[i][actIdx] || '').trim();
    if (!logId) continue;

    // Pull env from the pre-built map; fall back to 'IMPORT' if not found
    const env = envMap[logId] || 'IMPORT';

    entries.push({
      logId,
      // "IMPORT | Owners | Load | 05/27/25 2:34 PM"
      label: [env, obj, action, ts].filter(Boolean).join(' | ')
    });
  }

  return entries;
}


// ── Get Single Log Entry ──────────────────────────────────────

/**
 * Returns the full detail for one log entry, keyed by Log ID.
 * Called by UnifiedSidebar.html Audit tab when a dropdown entry
 * is selected.
 *
 * @param {string} logId — UUID from _Log Data col A
 * @returns {Object|null}
 */
function getLogEntry(logId) {
  if (!logId) return null;

  const ss        = SpreadsheetApp.getActive();
  const dataSheet = ss.getSheetByName('_Log Data');
  if (!dataSheet) return null;

  const rows    = dataSheet.getDataRange().getValues();
  const headers = rows[0].map(h => String(h).trim());

  const h = {
    id:       headers.indexOf('Log ID'),
    ts:       headers.indexOf('Timestamp'),
    obj:      headers.indexOf('Object'),
    action:   headers.indexOf('Action'),
    payload:  headers.indexOf('Payload (JSON)'),
    response: headers.indexOf('Response (Raw)'),
    results:  headers.indexOf('Results (JSON)'),
    errors:   headers.indexOf('Errors (JSON)')
  };

  let row = null;
  for (let i = 1; i < rows.length; i++) {
    if (String(rows[i][h.id]).trim() === logId) { row = rows[i]; break; }
  }
  if (!row) return null;

  let results = [];
  let errors  = [];
  try { results = JSON.parse(String(row[h.results] || '[]')); } catch (_) {}
  try { errors  = JSON.parse(String(row[h.errors]  || '[]')); } catch (_) {}

  const resultRecords = _buildResultRecords(results, errors);
  const envUser       = _getEnvAndUserFromLog(logId);

  return {
    logId,
    timestamp: String(row[h.ts]     || ''),
    object:    String(row[h.obj]    || ''),
    action:    String(row[h.action] || ''),
    payload:   String(row[h.payload]  || '{}'),
    response:  String(row[h.response] || '{}'),
    env:       envUser.env  || 'IMPORT',
    user:      envUser.user || '',
    results:   resultRecords
  };
}


// ── Sync Pending Jobs (Audit tab button) ──────────────────────

/**
 * Called from the Audit tab's "Sync Pending Jobs" button.
 *
 * For Occupancy log entries, delegates entirely to syncTenantJobStatuses()
 * (the Load-tab Sync function). Occupancy resolution is two-phase:
 *   Phase 1: poll /jobs → TenantId
 *   Phase 2: GET /tenants → OccupancyId
 * syncPendingJobs can only perform Phase 1, so delegating ensures
 * OccupancyId is written — identical to the Load-tab Sync button.
 * A sheet-reconcile fallback handles the edge case where a previous
 * sync updated the Tenants sheet but missed the _Log Data update.
 *
 * For all other object types, the original flow applies:
 *   1. Read pending result records for the given logId.
 *   2. Extract unique JobIds.
 *   3. Poll /jobs endpoint for each.
 *   4. Write resolved ResourceIds to _Log Data (updateLogDataAfterSync).
 *   5. Write API_Status + API_ID back to the object sheet tab.
 *
 * @param {string} logId — UUID of the log entry to sync
 */
function syncPendingJobs(logId) {
  if (!logId) return { resolved: 0, message: 'No log entry selected.' };

  const entry = getLogEntry(logId);
  if (!entry) return { resolved: 0, message: 'Log entry not found.' };

  // ── Occupancies: delegate to syncTenantJobStatuses() ───────────────────
  // Occupancy loads require a two-phase resolution:
  //   Phase 1: poll /jobs → TenantId
  //   Phase 2: GET /tenants?filters[Id]=… → OccupancyId
  // This function only has access to /jobs results (Phase 1), so delegating
  // to syncTenantJobStatuses() ensures OccupancyId is also written —
  // matching exactly what the Load-tab Sync button does.
  if (entry.object === 'Occupancies') {
    const pendingBefore = (entry.results || []).filter(r => r.successful === null && r.jobId);
    if (!pendingBefore.length) {
      return { resolved: 0, alreadyResolved: true, message: 'All entries are already resolved — nothing to sync.' };
    }

    // Run the full tenant sync. Errors are silently caught — syncTenantJobStatuses()
    // always calls _updateAuditLogByRefIds in its finally block, so _Log Data is
    // updated even if the function itself throws (e.g. during the completion toast).
    try { syncTenantJobStatuses(); } catch (e) {
      console.warn('syncPendingJobs (Occupancies) — syncTenantJobStatuses: ' + e.message);
    }

    // ── Fallback: reconcile _Log Data from the Tenants sheet ──────────────
    // If _Log Data still has pending records after the sync (e.g. a previous
    // sync updated the sheet but missed the audit log update), read the Tenants
    // sheet directly to find rows that are already "Success" and update _Log Data.
    const postSyncEntry   = getLogEntry(logId);
    const pendingPostSync = (postSyncEntry ? postSyncEntry.results || [] : [])
                              .filter(r => r.successful === null && r.jobId);
    if (pendingPostSync.length > 0) {
      _reconcileOccupancyAuditFromSheet(pendingPostSync);
    }

    // Report final resolution count
    const finalEntry   = getLogEntry(logId);
    const finalPending = (finalEntry ? finalEntry.results || [] : [])
                           .filter(r => r.successful === null && r.jobId);
    const resolved     = pendingBefore.length - finalPending.length;
    if (finalPending.length === 0) {
      return { resolved, message: 'Occupancy sync completed — ' + resolved + ' record(s) resolved.' };
    }
    if (resolved > 0) {
      return { resolved, message: resolved + ' record(s) resolved. ' + finalPending.length + ' still pending — re-run Sync to retry.' };
    }
    return {
      resolved: 0,
      message: 'No pending Tenant rows found on the Tenants sheet. ' +
               'Jobs may still be processing — re-run Sync in a moment, ' +
               'or use the Load tab Sync button.'
    };
  }

  // Find all pending records (successful === null with a jobId)
  const pending = (entry.results || []).filter(r => r.successful === null && r.jobId);
  if (!pending.length) {
    return { resolved: 0, alreadyResolved: true, message: 'All entries are already resolved — nothing to sync.' };
  }

  // Collect unique Job IDs
  const uniqueJobIds = [...new Set(pending.map(r => r.jobId).filter(Boolean))];

  // resolvedMap: referenceId → { success, resourceId, error }
  const resolvedMap = {};

  // Poll in batches of 50 (AppFolio supports comma-separated Id filters).
  // This avoids hitting rate limits when there are many pending jobs
  // (e.g. 50+ rows that were submitted per-row each generating their own jobId).
  const jobChunkSize = 50;
  for (let i = 0; i < uniqueJobIds.length; i += jobChunkSize) {
    const chunk    = uniqueJobIds.slice(i, i + jobChunkSize);
    const idFilter = chunk.join(',');
    const endpoint = `${CONFIG.BASE_URL}/jobs?filters[Id]=${idFilter}&page[size]=${jobChunkSize}`;
    const options  = { method: 'get', headers: getApiHeaders(), muteHttpExceptions: true };

    try {
      const resp    = UrlFetchApp.fetch(endpoint, options);
      const resText = resp.getContentText();

      if (resText.includes('Retry later')) {
        console.warn('syncPendingJobs — rate limit hit, sleeping 5s');
        Utilities.sleep(5000);
        i -= jobChunkSize;  // retry this chunk
        continue;
      }

      const result = JSON.parse(resText);
      const jobs   = result.data || [];

      jobs.forEach(job => {
        if (job.Status !== 'finished' || !job.Result) return;
        job.Result.forEach(item => {
          const refId = String(item.ReferenceId || '').trim();
          if (!refId) return;
          resolvedMap[refId] = {
            success:    !!(item.Successful || item.successful),
            resourceId: item.ResourceId || '',
            error:      item.Error || item.error || item.message || ''
          };
        });
      });

      // Brief inter-chunk pause to stay within AppFolio rate limits
      if (i + jobChunkSize < uniqueJobIds.length) Utilities.sleep(300);
    } catch (e) {
      console.warn('syncPendingJobs — error polling job chunk: ' + e.message);
    }
  }

  if (!Object.keys(resolvedMap).length) {
    return {
      resolved: 0,
      message: 'AppFolio jobs are still processing or have already expired. ' +
               'Run the sheet Sync button first, then reload this entry.'
    };
  }

  const resolvedItems = Object.entries(resolvedMap).map(([refId, r]) => ({
    referenceId: refId,
    resourceId:  r.resourceId,
    successful:  r.success,
    error:       r.error
  }));

  // ── Step 4: Update _Log Data structured store ─────────────
  // Multi-batch loads produce one _Log Data row but with records
  // pointing to different jobIds (one per chunk). Must loop over
  // all unique jobIds so every record gets resolved, not just the
  // records from the first chunk.
  uniqueJobIds.forEach(jId => updateLogDataAfterSync(jId, resolvedItems));

  // ── Step 5: Write results back to the object sheet tab ────
  // FIX: This was missing. Without it the Owners/Vendors/etc.
  // sheet tab stays on "Pending: <jobId>" even after jobs resolve.
  _writeSyncResultsToSheet(entry.object, resolvedMap);

  const resolved = resolvedItems.filter(r => r.successful).length;
  const failed   = resolvedItems.length - resolved;
  const msg = failed > 0
    ? 'Resolved ' + resolvedItems.length + ' job(s): ' + resolved + ' succeeded, ' + failed + ' failed.'
    : 'Resolved ' + resolved + ' job(s) successfully.';
  return { resolved: resolvedItems.length, message: msg };
}


// ── Write Sync Results to Object Sheet Tab ────────────────────

/**
 * Writes resolved job results (API_Status + API_ID) directly to
 * the named object sheet tab.
 *
 * Mirrors what syncOwnerJobStatuses() does in owners.gs but is
 * driven from the resolved data already fetched by syncPendingJobs(),
 * so no additional API call is needed.
 *
 * Handles any object sheet that follows the standard column convention:
 *   ReferenceId  — used to match rows
 *   API_Status   — set to 'Success' (green) or 'Error: …' (red)
 *   API_ID       — set to the AF ResourceId on success
 *
 * @param {string} object       — e.g. 'Owners', 'Vendors', 'Unit Types'
 * @param {Object} resolvedMap  — { referenceId: { success, resourceId, error } }
 * @private
 */
function _writeSyncResultsToSheet(object, resolvedMap) {
  if (!object || !resolvedMap || !Object.keys(resolvedMap).length) return;

  // ── Late fee objects: use LateFee_Status column + Note-based refId matching ──
  // Late fees store their ReferenceId in the Note on the LateFee_Status cell
  // (written by executeProperty/OccupancyLateFeeLoad). There is no ReferenceId
  // column or API_ID column on these sheets for late fee rows.
  if (object === 'Prop Late Fees' || object === 'Occ Late Fees') {
    const ss        = SpreadsheetApp.getActive();
    const sheetName = object === 'Prop Late Fees' ? 'Properties' : 'Tenants';
    const sheet     = ss.getSheetByName(sheetName);
    if (!sheet) return;

    const data    = sheet.getDataRange().getValues();
    const headers = data[0].map(h => String(h).trim());
    const lfIdx   = headers.indexOf('LateFee_Status');
    if (lfIdx === -1) return;

    // Batch-read Notes (stored refIds) from the LateFee_Status column
    const notes = sheet.getRange(1, lfIdx + 1, data.length, 1).getNotes();

    for (let i = 1; i < data.length; i++) {
      const current = String(data[i][lfIdx] || '').trim();
      if (!current.includes('Pending:')) continue;

      const refId = notes[i][0] || '';
      const match = refId ? resolvedMap[refId] : null;
      if (!match) continue;

      const cell = sheet.getRange(i + 1, lfIdx + 1);
      if (match.success) {
        cell.setValue('Success').setBackground('#b6d7a8');
      } else {
        cell.setValue('Error: ' + (match.error || 'Unknown error')).setBackground('#f4cccc');
      }
    }
    return;
  }

  const ss = SpreadsheetApp.getActive();

  // Some log object names differ from their sheet tab names.
  // e.g. occupancies are logged as 'Occupancies' but the sheet is 'Tenants'.
  const SHEET_NAME_MAP = {
    'Occupancies': 'Tenants',
    'Tenants':     'Tenants'
  };
  const sheetName = SHEET_NAME_MAP[object] || object;
  const sheet = ss.getSheetByName(sheetName);
  if (!sheet) {
    console.warn('_writeSyncResultsToSheet: sheet "' + sheetName + '" not found — skipping tab update.');
    return;
  }

  const data    = sheet.getDataRange().getValues();
  const headers = data[0].map(h => String(h).trim());

  const statusIdx = headers.indexOf('API_Status');
  const apiIdIdx  = headers.indexOf('API_ID');
  const refIdIdx  = headers.indexOf('ReferenceId');

  // Sheet must have all three columns to proceed
  if (statusIdx === -1 || apiIdIdx === -1 || refIdIdx === -1) {
    console.warn(
      '_writeSyncResultsToSheet: sheet "' + object + '" is missing one of: ' +
      'API_Status, API_ID, ReferenceId — skipping tab update.'
    );
    return;
  }

  for (let i = 1; i < data.length; i++) {
    const rowRefId = String(data[i][refIdIdx] || '').trim();
    const current  = String(data[i][statusIdx] || '').trim();

    // Only update rows that are still in a Pending state
    if (!current.includes('Pending')) continue;

    const match = resolvedMap[rowRefId];
    if (!match) continue;

    if (match.success && match.resourceId) {
      // Success: write AF ID + green status
      sheet.getRange(i + 1, apiIdIdx  + 1).setValue(match.resourceId);
      sheet.getRange(i + 1, statusIdx + 1)
        .setValue('Success')
        .setBackground('#b6d7a8');   // light green — consistent with syncOwnerJobStatuses()
    } else {
      // Failure: write error reason + red status
      sheet.getRange(i + 1, statusIdx + 1)
        .setValue('Error: ' + (match.error || 'Unknown error'))
        .setBackground('#f4cccc');   // light red
    }
  }
}


// ── Update Log Data After Sync ────────────────────────────────

/**
 * Updates the Results (JSON) column in _Log Data after a job resolves.
 *
 * Called by:
 *   - syncOwnerJobStatuses()    in owners.gs
 *   - syncVendorJobStatuses()   in vendors.gs
 *   - syncTenantJobStatuses()   in tenants.gs
 *   - syncUnitTypeJobStatuses() in unitTypes.gs
 *   - syncPendingJobs()         above (audit-tab triggered)
 *
 * @param {string} jobId          — The JobId to resolve
 * @param {Array}  resolvedItems  — [{ referenceId, resourceId, successful, error }]
 */
function updateLogDataAfterSync(jobId, resolvedItems) {
  if (!jobId || !resolvedItems || !resolvedItems.length) return;

  const ss        = SpreadsheetApp.getActive();
  const dataSheet = ss.getSheetByName('_Log Data');
  if (!dataSheet) return;

  const rows    = dataSheet.getDataRange().getValues();
  const headers = rows[0].map(h => String(h).trim());
  const resIdx  = headers.indexOf('Results (JSON)');
  if (resIdx === -1) return;

  // Build refId → resolved data map for fast lookup
  const resolvedMap = {};
  resolvedItems.forEach(item => {
    resolvedMap[String(item.referenceId).trim()] = item;
  });

  for (let i = 1; i < rows.length; i++) {
    const rawResults = String(rows[i][resIdx] || '[]');
    if (!rawResults.includes(jobId)) continue;

    let results;
    try { results = JSON.parse(rawResults); } catch (_) { continue; }

    let changed = false;
    const updated = results.map(r => {
      // Skip the _truncated sentinel — preserve it as-is
      if (r._truncated) return r;

      if (r.returnedId === jobId && r.idType === 'job') {
        const resolved = resolvedMap[String(r.referenceId).trim()];
        if (resolved) {
          changed = true;
          return {
            referenceId:  r.referenceId,
            successful:   resolved.successful,
            idType:       'direct',
            returnedId:   resolved.resourceId || '',
            errorMessage: resolved.error || ''
          };
        }
      }
      return r;
    });

    if (changed) {
      // Use _safeCellArray so the write-back also stays under 50 000 chars
      dataSheet.getRange(i + 1, resIdx + 1).setValue(_safeCellArray(updated));
    }
  }
}


// ── Private Helpers ───────────────────────────────────────────

/**
 * Converts stored results + errors arrays into the record shape
 * the sidebar expects.
 * @private
 */
function _buildResultRecords(results, errors) {
  const records = [];

  // Check for truncation sentinel added by _safeCellArray() in logSystem.gs.
  // When the results JSON was too large to fit in one cell, the stored array
  // ends with {"_truncated":true,"omitted":N}. Filter it out of the main
  // result set and add a visible warning record instead so the user knows
  // some records weren't stored (they can still sync via the sheet tab).
  const truncationEntry = results.find(r => r._truncated);
  const cleanResults    = results.filter(r => !r._truncated);
  if (truncationEntry) {
    records.push({
      referenceId:  '⚠️ Log truncated',
      successful:   false,
      returnedId:   '',
      jobId:        '',
      errorMessage: truncationEntry.omitted + ' record(s) were not stored in the Audit log because '
                    + 'the payload exceeded the 50 000-character cell limit. '
                    + 'Use the sheet tab Sync button to resolve all pending jobs.'
    });
  }

  cleanResults.forEach(r => {
    const refId = String(r.referenceId || '').trim();

    if (r.successful === false) {
      const errMsg = r.errorMessage
        || (Array.isArray(r.errors) ? r.errors.map(e => `${e.field}: ${e.message}`).join(' | ') : 'Unknown error');
      records.push({
        referenceId:  refId,
        successful:   false,
        returnedId:   '',
        jobId:        '',
        errorMessage: errMsg
      });
    } else if (r.idType === 'job' && !r.returnedId) {
      records.push({
        referenceId:  refId,
        successful:   null,
        returnedId:   '',
        jobId:        r.returnedId || '',
        errorMessage: ''
      });
    } else if (r.idType === 'job' && r.returnedId && r.returnedId.length < 40) {
      // UUID is 36 chars — still a JobId, not yet a ResourceId
      records.push({
        referenceId:  refId,
        successful:   null,
        returnedId:   '',
        jobId:        r.returnedId,
        errorMessage: ''
      });
    } else {
      records.push({
        referenceId:  refId,
        successful:   true,
        returnedId:   r.returnedId || '',
        jobId:        '',
        errorMessage: ''
      });
    }
  });

  // Append error records not already in results (e.g. hard HTTP errors)
  const resultRefs = new Set(records.map(r => r.referenceId));
  errors.forEach(e => {
    if (!resultRefs.has(String(e.ref || '').trim())) {
      records.push({
        referenceId:  String(e.ref || 'Unknown'),
        successful:   false,
        returnedId:   '',
        jobId:        '',
        errorMessage: `${e.field ? e.field + ': ' : ''}${e.message || 'Unknown error'}`
      });
    }
  });

  return records;
}

/**
 * Reconciles _Log Data for Occupancy entries when _Log Data still shows
 * pending records but the Tenants sheet already has those rows as "Success".
 *
 * This edge case occurs when a previous syncTenantJobStatuses() run updated
 * the Tenants sheet but the _updateAuditLogByRefIds() call in its finally
 * block failed silently (e.g. quota error). Reads the Tenants sheet directly
 * and calls _updateAuditLogByRefIds() to sync _Log Data to match.
 *
 * @param {Array} stillPendingRecords — result records (from _buildResultRecords)
 *                                      where successful === null
 * @private
 */
function _reconcileOccupancyAuditFromSheet(stillPendingRecords) {
  if (!stillPendingRecords || !stillPendingRecords.length) return;

  const ss    = SpreadsheetApp.getActive();
  const sheet = ss.getSheetByName('Tenants');
  if (!sheet) return;

  const data    = sheet.getDataRange().getValues();
  const headers = data[0].map(h => String(h).trim());
  const refIdx    = headers.indexOf('ReferenceId');
  const statusIdx = headers.indexOf('API_Status');
  const apiIdIdx  = headers.indexOf('API_ID');
  if (refIdx === -1 || statusIdx === -1 || apiIdIdx === -1) return;

  // Index Tenants sheet rows that are already "Success" with a TenantId
  const sheetSuccessMap = {};
  for (let i = 1; i < data.length; i++) {
    const refId  = String(data[i][refIdx]    || '').trim();
    const status = String(data[i][statusIdx] || '').trim();
    const apiId  = String(data[i][apiIdIdx]  || '').trim();
    if (refId && status === 'Success' && apiId) {
      sheetSuccessMap[refId] = apiId;
    }
  }

  // Build resolvedByRef only for the records that are still pending in
  // _Log Data AND already "Success" on the sheet
  const resolvedByRef = new Map();
  stillPendingRecords.forEach(r => {
    const refId = String(r.referenceId || '').trim();
    if (sheetSuccessMap[refId]) {
      resolvedByRef.set(refId, {
        resourceId: sheetSuccessMap[refId],
        successful: true,
        error:      ''
      });
    }
  });

  if (resolvedByRef.size > 0) {
    console.log(
      '_reconcileOccupancyAuditFromSheet: updating ' + resolvedByRef.size +
      ' record(s) in _Log Data from Tenants sheet'
    );
    _updateAuditLogByRefIds(resolvedByRef);
  }
}


/**
 * Cross-references the API Log sheet to get env and user for a logId.
 * API Log col I = logId; env = col C, user = col B.
 * @private
 */
function _getEnvAndUserFromLog(logId) {
  const ss       = SpreadsheetApp.getActive();
  const logSheet = ss.getSheetByName('API Log');
  if (!logSheet) return { env: 'IMPORT', user: '' };

  const rows = logSheet.getDataRange().getValues();
  for (let i = 1; i < rows.length; i++) {
    if (String(rows[i][8] || '').trim() === logId) {
      return {
        env:  String(rows[i][2] || 'IMPORT').trim(),
        user: String(rows[i][1] || '').trim()
      };
    }
  }
  return { env: 'IMPORT', user: '' };
}
