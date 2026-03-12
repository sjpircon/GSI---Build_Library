// ============================================================
// TENANTS.GS — Prep, Load, and Sync for Tenants + Recurring Charges
//
// ARCHITECTURE
// ────────────
// Recurring Charges live as inline column groups on the Tenants
// sheet, not a separate sheet. Each RC slot follows this pattern:
//
//   Addtl Recurring Charge GL #N
//   Addtl Recurring Charge Start Date #N
//   Addtl Recurring Charge End Date #N
//   Addtl Recurring Charge Amount #N
//   Addtl Recurring Charge Description #N
//   Addtl Recurring Charge Frequency #N
//
// Slots are discovered dynamically at runtime — adding new
// numbered columns requires no code changes.
//
// FLOW
// ────
//   1. prepTenants()           — validates tenant rows, resolves UnitIds
//   2. executeTenantLoad()     — bulk POST /tenants/bulk in chunks of 40
//   3. syncTenantJobStatuses() — polls /jobs, writes OccupancyId + TenantId
//   4. prepRecurringCharges()  — resolves OccupancyId + GlAccountIds per slot,
//                                stores GlAccountId as Note on GL cell,
//                                defaults StartDate to MoveIn if blank
//   5. executeRecurringChargeLoad() — POSTs each ready RC slot, stamps
//                                     Note on GL cell with Success/Error
//
// FUNCTIONS
// ─────────
//   prepTenants()                — validates rows, resolves UnitId via Notes
//   runTenantLoad()              — menu wrapper → confirmAndRun
//   executeTenantLoad()          — bulk POST in chunks of 40
//   syncTenantJobStatuses()      — polls /jobs, writes TenantId + OccupancyId
//   buildTenantObjects()         — builds occupant payload array (name-guarded)
//   _hasResolvableName()         — checks occupant has at least one name field
//   _parseAfErrorMessage()       — parses AppFolio 4xx JSON → readable string
//   _buildRcSlots()              — discovers inline RC slot column indices
//   mapSingleTenant()            — maps one tenant to API payload shape
//   castDate()                   — formats date values to 'yyyy-MM-dd'
//   prepRecurringCharges()       — validates inline RC slots, stores Notes
//   executeRecurringChargeLoad() — POSTs each ready RC slot individually
// ============================================================


// ── Occupancy Operation Lock ──────────────────────────────────
//
// DESIGN
// ──────
// Both executeTenantLoad() and syncTenantJobStatuses() share a
// single LockService.getScriptLock(). This is the ONLY truly
// atomic lock in GAS — PropertiesService reads/writes are NOT
// atomic, so a PropertiesService-only mutex has a race condition
// when two executions start within the same few milliseconds.
//
// HOW IT WORKS
// ────────────
//   1. Each function calls LockService.getScriptLock().waitLock(N).
//      GAS guarantees only ONE caller proceeds; all others block
//      until the lock is released or the timeout expires.
//   2. Once the lock is acquired, a PropertiesService record is
//      written (op name + timestamp). This is ONLY used to build
//      a helpful "X has been running for Ys" toast for the caller
//      that eventually times out — not for the lock itself.
//   3. In the finally block the lock is released and the record
//      cleared. LockService also auto-releases on any GAS crash
//      (OOM, hard timeout) — so stale locks are impossible.
//   4. forceReleaseOccupancyLock() clears a leftover PropertiesService
//      record in the rare case the finally block was skipped while
//      LockService itself auto-released (GAS hard-kill edge case).
//
// SCOPE: executeTenantLoad() and syncTenantJobStatuses().

const _MUTEX_KEY = 'OCCUPANCY_OP_RUNNING';

/**
 * Builds a user-facing message describing the currently-running
 * occupancy operation, using the PropertiesService record written
 * by the holder of the script lock.
 * @private
 */
function _occupancyLockMessage() {
  try {
    const raw = PropertiesService.getScriptProperties().getProperty(_MUTEX_KEY);
    if (!raw) return 'Another occupancy operation is running.';
    const { op, startedAt } = JSON.parse(raw);
    const ageSec = Math.round((Date.now() - startedAt) / 1000);
    return `"${op}" has been running for ${ageSec}s.`;
  } catch (_) {
    return 'Another occupancy operation is running.';
  }
}

/** @private */
function _setOccupancyLockRecord(opName) {
  try {
    PropertiesService.getScriptProperties().setProperty(
      _MUTEX_KEY, JSON.stringify({ op: opName, startedAt: Date.now() })
    );
  } catch (_) {}
}

/** @private */
function _releaseOccupancyMutex() {
  try { PropertiesService.getScriptProperties().deleteProperty(_MUTEX_KEY); } catch (_) {}
}

/**
 * Menu action — clears a leftover PropertiesService record after a
 * hard GAS crash where LockService auto-released but the finally
 * block didn't run.
 * Bound to  Onboarding API → 🔓 Release Occupancy Lock.
 */
function forceReleaseOccupancyLock() {
  _releaseOccupancyMutex();
  SpreadsheetApp.getActive().toast(
    'Occupancy lock record cleared. You can now run Load or Sync.',
    '✅ Lock Released'
  );
}

/**
 * Acquires the script lock for occupancy operations.
 * Returns the lock object on success, null if timed out.
 * Call _releaseOccupancyMutex() + lock.releaseLock() in finally.
 * @private
 */
function _acquireOccupancyLock(opName) {
  const lock = LockService.getScriptLock();
  try {
    lock.waitLock(10000); // wait up to 10s for any in-progress operation
  } catch (e) {
    // Lock not acquired — another operation is still running
    const msg = _occupancyLockMessage();
    SpreadsheetApp.getActive().toast(
      msg + ' Wait for it to finish, or use  Onboarding API → 🔓 Release Occupancy Lock  if it appears stuck.',
      '⚠️ Occupancy Busy', 25
    );
    return null;
  }
  // Lock acquired — record what's running for helpful messaging
  _setOccupancyLockRecord(opName);
  return lock;
}


// ── Prep Tenants ─────────────────────────────────────────────

/**
 * Validates all non-Success rows on the Tenants sheet.
 *
 * For each row:
 *   1. Detects scientific notation in phone columns and flags it.
 *   2. Validates email format (must contain '@').
 *   3. Resolves UnitId from the Units sheet via composite key
 *      "PropertyName|UnitName" and stores it as a Note on UnitName.
 *   4. Auto-generates a ReferenceId prefixed with PropertyName.
 *   5. Stamps Ready or Error.
 *   6. Forces phone/email columns to text format.
 *
 * NAME VALIDATION
 *   Accepts (FirstName + LastName) OR CommercialName — commercial
 *   tenants with no individual names are valid. Aligns with the
 *   AppFolio API fix adding CompanyName support for tenants.
 */
function prepTenants() {
  const ss          = SpreadsheetApp.getActive();
  const tenantSheet = ss.getSheetByName('Tenants');
  const unitSheet   = ss.getSheetByName('Units');
  if (!tenantSheet || !unitSheet) return;

  const tenantData    = tenantSheet.getDataRange().getValues();
  const tenantHeaders = tenantData[0].map(h => String(h).trim());
  const t             = {};
  tenantHeaders.forEach((title, i) => { t[title] = i; });

  const phoneCols = [];
  const emailCols = [];
  tenantHeaders.forEach((h, i) => {
    if (h.toLowerCase().includes('phone')) phoneCols.push(i);
    if (h.toLowerCase().includes('email')) emailCols.push(i);
  });

  // ── Unit lookup: "PropertyName|UnitName" → UnitId ─────────
  const unitData    = unitSheet.getDataRange().getValues();
  const unitHeaders = unitData[0].map(h => String(h).trim());
  const uPropIdx    = unitHeaders.indexOf('PropertyName');
  const uNameIdx    = unitHeaders.indexOf('Name');
  const uApiIdx     = unitHeaders.indexOf(CONFIG.API_ID_COL);

  const unitLookup = {};
  unitData.forEach((row, j) => {
    if (j === 0) return;
    const key = `${String(row[uPropIdx]).trim()}|${String(row[uNameIdx]).trim()}`;
    if (row[uApiIdx]) unitLookup[key] = row[uApiIdx];
  });

  const timestamp = new Date().getTime();

  for (let i = 1; i < tenantData.length; i++) {
    const rowNum = i + 1;
    if (tenantData[i][t[CONFIG.STATUS_COL]] === 'Success') continue;

    const rowErrors = [];

    // ── Phone: detect scientific notation ─────────────────
    phoneCols.forEach(colIdx => {
      const cellRaw = tenantData[i][colIdx];
      const cellVal = String(cellRaw);
      const isScientific = (typeof cellRaw === 'number' && cellRaw > 100000000000)
                        || cellVal.toUpperCase().includes('E+');
      if (isScientific) {
        tenantSheet.getRange(rowNum, colIdx + 1).setBackground('#f4cccc');
        rowErrors.push(`Scientific notation in ${tenantHeaders[colIdx]} — format column as Plain Text`);
      }
    });

    // ── Email: basic format check ──────────────────────────
    emailCols.forEach(colIdx => {
      const cellVal = String(tenantData[i][colIdx]).trim();
      if (cellVal !== '' && !cellVal.includes('@')) {
        tenantSheet.getRange(rowNum, colIdx + 1).setBackground('#fce5cd');
        rowErrors.push(`Invalid email in ${tenantHeaders[colIdx]}`);
      }
    });

    // ── Name check ────────────────────────────────────────
    // Valid if: (FirstName + LastName) OR CommercialName present.
    const hasName = (
      String(tenantData[i][t['FirstName']] || '').trim() &&
      String(tenantData[i][t['LastName']]  || '').trim()
    ) || String(tenantData[i][t['CommercialName']] || '').trim();

    if (!hasName) {
      rowErrors.push('Name required: provide FirstName + LastName, or CommercialName');
    }

    // ── MoveIn: required, must be a valid date ────────────
    const moveInRaw = tenantData[i][t['MoveIn']];
    if (moveInRaw === '' || moveInRaw === null || moveInRaw === undefined) {
      rowErrors.push('MoveIn date is required');
    } else {
      const moveInDate = (moveInRaw instanceof Date) ? moveInRaw : new Date(moveInRaw);
      if (isNaN(moveInDate.getTime())) rowErrors.push('MoveIn is not a valid date');
    }

    // ── Resolve UnitId ────────────────────────────────────
    const propName = String(tenantData[i][t['PropertyName']]).trim();
    const unitName = String(tenantData[i][t['UnitName']]).trim();
    const unitId   = unitLookup[`${propName}|${unitName}`];

    if (unitId) {
      tenantSheet.getRange(rowNum, t['UnitName'] + 1).setNote(unitId);
    } else {
      tenantSheet.getRange(rowNum, t['UnitName'] + 1)
        .setBackground('#f4cccc').setNote('Unit API ID missing');
      rowErrors.push(`Unit '${unitName}' not found on '${propName}' — ensure Units are loaded first`);
    }

    // ── Auto-generate ReferenceId ─────────────────────────
    const safeProp = propName.replace(/[^a-zA-Z0-9]/g, '_').replace(/_+/g, '_').replace(/^_|_$/g, '') || 'Property';
    tenantSheet.getRange(rowNum, t[CONFIG.REF_ID_COL] + 1)
      .setValue(`${safeProp}_Group_${timestamp}_${i}`);

    // ── Stamp status ──────────────────────────────────────
    const statusCell = tenantSheet.getRange(rowNum, t[CONFIG.STATUS_COL] + 1);
    if (rowErrors.length > 0) {
      statusCell.setValue('Errors:\n• ' + rowErrors.join('\n• '));
    } else {
      statusCell.setValue('Ready');
    }
  }

  [...phoneCols, ...emailCols].forEach(idx => {
    tenantSheet.getRange(2, idx + 1, tenantSheet.getLastRow()).setNumberFormat('@');
  });

  applyConditionalRules(tenantSheet, tenantHeaders, t[CONFIG.STATUS_COL]);
  ss.toast('Tenants Prepped.', 'Tenants Prep');
}


// ── Execute Tenant Load ───────────────────────────────────────

function runTenantLoad() {
  confirmAndRun(executeTenantLoad, 'Bulk Tenant Load');
}

/**
 * Sends Ready rows to /tenants/bulk in batches of 40.
 *
 * Batching is safe because syncTenantJobStatuses() resolves
 * results by ReferenceId, not position. Reduces API calls ~40×.
 *
 * NAME GUARD: buildTenantObjects() drops nameless occupants so a
 * single blank-name row can't reject an entire chunk.
 *
 * DEADLOCK PREVENTION (Bug 12): LockService prevents two concurrent
 * executions from writing to the same rows simultaneously.
 *
 * BATCHING (Bug 7): All chunks are logged as a single Audit entry
 * per script run. Each occupant's pending placeholder points to
 * its chunk's JobId so sync still works correctly.
 *
 * Auto-stops at 5m40s — re-run picks up remaining Ready rows.
 */
function executeTenantLoad() {
  // ── Atomic mutual exclusion ───────────────────────────────
  // Shared with syncTenantJobStatuses() — only one can run at a time.
  const lock = _acquireOccupancyLock('Occupancy Load');
  if (!lock) return; // another operation is running — toast already shown

  try {
    const startTime = new Date().getTime();
    const ss        = SpreadsheetApp.getActive();
    const sheet     = ss.getSheetByName('Tenants');
    if (!sheet) return;

    const data    = sheet.getDataRange().getValues();
    const headers = data[0].map(h => String(h).trim());
    const t       = {};
    headers.forEach((title, i) => { t[title] = i; });

    // ── 1. Collect all Ready rows ───────────────────────────
    const allPayloads = [];
    const allRowNums  = [];

    for (let i = 1; i < data.length; i++) {
      if (String(data[i][t[CONFIG.STATUS_COL]]).trim() !== 'Ready') continue;

      const rowNum     = i + 1;
      const groupRefId = String(data[i][t[CONFIG.REF_ID_COL]]);
      const unitId     = sheet.getRange(rowNum, t['UnitName'] + 1).getNote();

      const occupantPayloads = buildTenantObjects(data[i], t, unitId, groupRefId, headers, sheet, rowNum);
      if (!occupantPayloads.length) continue; // nameless primary — row already stamped Skipped

      allPayloads.push(occupantPayloads);
      allRowNums.push(rowNum);
    }

    if (!allPayloads.length) {
      return ss.toast('No Ready rows found. Run Prep first.', 'Tenants Load');
    }

    // ── 2. One API call per row ──────────────────────────────
    // Each spreadsheet row is submitted as its own /tenants/bulk call,
    // producing an independent job on AppFolio's backend. This eliminates
    // batch-level deadlocks: each job contains only one occupancy group,
    // so there are no concurrent rows within a single transaction that
    // Postgres can deadlock against.
    //
    // Trade-off: more API calls, but each is tiny and the 340s guard
    // handles time limits gracefully. 300ms inter-row sleep keeps us
    // well within AppFolio's rate limits.

    const INTER_ROW_SLEEP = 300; // ms between rows

    // Accumulate for single audit log entry at the end.
    // ONE entry per spreadsheet row (groupRefId → jobId), not per occupant,
    // so the audit log count always matches the sheet row count.
    const allLogRecords     = [];  // one payload array per row
    const allPendingResults = [];  // one result entry per row

    for (let i = 0; i < allPayloads.length; i++) {
      if (new Date().getTime() - startTime > 340000) {
        ss.toast('Time limit approaching. Re-run to continue remaining rows.', '⚠️ PAUSED', 20);
        break;
      }

      const rowPayloads = allPayloads[i];          // occupant objects for this row
      const rowNum      = allRowNums[i];
      const groupRefId  = String(data[rowNum - 1][t[CONFIG.REF_ID_COL]] || '').trim();
      const flatPayload = rowPayloads;              // already flat for a single row

      // Per-row retry — handles transient HTTP failures (no-JobId response)
      let result;
      let jobId = null;
      const MAX_RETRIES = 3;
      for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
        result = callAppFolioAPI(CONFIG.ENDPOINTS.TENANTS_BULK, flatPayload, 'Occupancies', 'Load', true);

        jobId = (result.data && result.data.JobId)
          ? result.data.JobId
          : ((result.message && result.message.match(/"JobId"\s*:\s*"([^"]+)"/) || [])[1] || null);

        if (result.success && jobId) break;

        const noJobId     = result.success && !jobId;
        const isRetriable = _isDeadlockError(result.message) || noJobId;
        if (!isRetriable || attempt === MAX_RETRIES) break;

        const waitMs = attempt * 2000;
        console.warn(
          'Row ' + rowNum + ' (attempt ' + attempt + '): no JobId — retrying in ' + waitMs + 'ms' +
          ' (HTTP ' + result.status + ', msg: ' + String(result.message || '').slice(0, 120) + ')'
        );
        Utilities.sleep(waitMs);
      }

      const statusCell = sheet.getRange(rowNum, t[CONFIG.STATUS_COL] + 1);
      if (result.success && jobId) {
        statusCell.setValue('Pending: ' + jobId).setBackground('#fff2cc');
        // One audit entry per row — referenceId is the groupRefId so sync
        // matches it correctly via the sheet's REF_ID column.
        allPendingResults.push({
          referenceId: groupRefId,
          successful:  true,
          idType:      'job',
          returnedId:  jobId
        });
        allLogRecords.push(flatPayload);
      } else {
        const errDetail = _parseAfErrorMessage(result.message) || ('HTTP ' + result.status);
        statusCell.setValue('Error: ' + errDetail).setBackground('#f4cccc');
      }

      SpreadsheetApp.flush();
      if (i < allPayloads.length - 1) Utilities.sleep(INTER_ROW_SLEEP);
    }

    // ── 3. Single audit log entry for the entire run ────────
    if (allLogRecords.length) {
      logResponse({
        action:          'Load',
        object:          'Occupancies',
        recordCount:     allLogRecords.length,
        request:         { data: allLogRecords },
        responseText:    'Submitted ' + allLogRecords.length + ' row(s) — one job per row',
        responseJson:    {},
        statusCode:      200,
        prebuiltResults: allPendingResults
      });
    }

    ss.toast('Tenant Load submitted. Run Sync to confirm statuses.', 'Tenants Load');

  } finally {
    lock.releaseLock();
    _releaseOccupancyMutex();
  }
}


// ── Sync Job Statuses ─────────────────────────────────────────

/**
 * Polls the AF Jobs endpoint for all "Pending: <jobId>" rows.
 * On resolution:
 *   • Writes TenantId (ResourceId) to API_ID column
 *   • Writes OccupancyId to OccupancyId column value + Note
 *     (Note is read by prepRecurringCharges() to resolve IDs)
 *   • Updates API_Status to "Success" or "Error: <reason>"
 */
function syncTenantJobStatuses() {
  // ── Atomic mutual exclusion ───────────────────────────────
  // Shared with executeTenantLoad() — only one can run at a time.
  const lock = _acquireOccupancyLock('Occupancy Sync');
  if (!lock) return;

  // Declared outside try so the finally block can always call
  // _updateAuditLogByRefIds — even when Phase 1 hits the 280s time limit
  // and returns early (which would skip any code inside the try block that
  // comes after the do-while loop).
  let allResolvedByRef = new Map();

  try {
    const startTime = new Date().getTime();
    const ss        = SpreadsheetApp.getActive();
    const sheet     = ss.getSheetByName('Tenants');
    if (!sheet) return;

    const data    = sheet.getDataRange().getValues();
    const headers = data[0].map(h => String(h).trim());
    const t       = {};
    headers.forEach((title, i) => { t[title] = i; });

    // ── Build initial pendingMap ──────────────────────────────
    // Collect { jobId → [{ rowIdx, refId }] } from "Pending:" rows.
    let pendingMap = new Map();
    for (let i = 1; i < data.length; i++) {
      const status = String(data[i][t[CONFIG.STATUS_COL]]);
      if (!status.includes('Pending:')) continue;
      const jobId = status.split('Pending:')[1].trim();
      const refId = String(data[i][t[CONFIG.REF_ID_COL]]).trim();
      if (!pendingMap.has(jobId)) pendingMap.set(jobId, []);
      pendingMap.get(jobId).push({ rowIdx: i + 1, refId });
    }

    if (!pendingMap.size) {
      ss.toast('No pending Tenant jobs found.', 'Tenant Sync');
      return;
    }

    // resolvedRows: TenantId → { rowIdx } — fed into Phase 2
    const resolvedRows = new Map();

    // allResolvedByRef: refId → { resourceId, successful, error }
    // Accumulated across all retry passes so _updateAuditLogByRefIds()
    // can update _Log Data once, after the entire retry loop finishes.
    // Using refId (not jobId) handles the retry case where new jobIds
    // are generated by _resubmitDeadlockedRows() and are NOT stored
    // in _Log Data (which still has the original load's jobIds).
    // NOTE: declared outside the try block (above) so the finally block
    // can always call _updateAuditLogByRefIds regardless of early returns.

    // ── Phase 1: Poll jobs with deadlock auto-retry ───────────
    //
    // Per-record deadlocks happen inside AppFolio's background job
    // processing (Postgres kills one transaction; the record is never
    // created). These are transient — re-submitting after a brief
    // delay almost always succeeds.
    //
    // The retry loop:
    //   1. Poll all jobs in pendingMap → write Success / Error per record.
    //      Deadlock failures are collected in retryQueue instead of being
    //      stamped as errors immediately.
    //   2. If retryQueue is empty → all resolved, exit loop.
    //   3. If retry limit reached → stamp retryQueue as final errors, exit.
    //   4. Otherwise: sleep (escalating), re-submit retryQueue rows via
    //      _resubmitDeadlockedRows(), wait for new jobs to start, repeat.
    //
    // MAX_DEADLOCK_RETRIES = 5 means up to 5 automatic re-submission
    // passes before a row is permanently marked as an error.

    // MAX_RETRY_PASSES = 5 — limits total re-poll passes for both deadlock
    // auto-retries AND still-in-progress re-polls. Each pass waits 3s → 6s →
    // 9s → 12s → 15s (total ≤ 45s) before giving up on unresolved jobs.
    const MAX_RETRY_PASSES = 5;
    let   retryPass        = 0;
    let   retryQueue       = [];  // [{ rowIdx, refId }] — deadlocked rows to re-submit

    do {
      if (new Date().getTime() - startTime > 280000) {
        ss.toast('Time limit approaching — re-run Sync to continue.', '⚠️ PAUSED', 20);
        return;
      }

      retryQueue = [];
      const finishedJobIds = new Set();  // jobs that returned as "finished" this pass
      const jobChunkSize   = 50;
      const allJobIds      = [...pendingMap.keys()];

      for (let i = 0; i < allJobIds.length; i += jobChunkSize) {
        const chunk    = allJobIds.slice(i, i + jobChunkSize);
        const idFilter = chunk.join(',');
        const endpoint = `${CONFIG.BASE_URL}/jobs?filters[Id]=${idFilter}&page[size]=${jobChunkSize}`;
        const options  = { method: 'get', headers: getApiHeaders(), muteHttpExceptions: true };

        try {
          const resp    = UrlFetchApp.fetch(endpoint, options);
          const resText = resp.getContentText();

          if (resText.includes('Retry later')) {
            console.warn('Rate limit hit during job fetch — sleeping 5s');
            Utilities.sleep(5000);
            i -= jobChunkSize;
            continue;
          }

          const result = JSON.parse(resText);
          const jobs   = result.data || [];

          jobs.forEach(job => {
            if (job.Status !== 'finished') return;
            if (!pendingMap.has(job.Id)) return;

            finishedJobIds.add(job.Id);  // mark this job as resolved on AppFolio's side

            const rows       = pendingMap.get(job.Id);
            const jobResults = job.Result || [];

            rows.forEach(row => {
              const match = jobResults.find(r => String(r.ReferenceId).trim() === row.refId);
              if (match && match.successful && match.ResourceId) {
                // Write TenantId to API_ID — queue for Phase 2 OccupancyId fetch
                sheet.getRange(row.rowIdx, t[CONFIG.API_ID_COL] + 1).setValue(match.ResourceId);
                resolvedRows.set(match.ResourceId, { rowIdx: row.rowIdx });
                // Track for audit log update (see _updateAuditLogByRefIds in finally)
                allResolvedByRef.set(row.refId, { resourceId: match.ResourceId, successful: true, error: '' });
              } else if (match && !match.successful) {
                // ── Diagnostic: log the raw AppFolio result so the true
                // error is always visible in Apps Script → Executions log.
                console.warn(
                  'Job ' + job.Id + ' record failed — raw match: ' +
                  JSON.stringify(match).slice(0, 300)
                );

                const err = _extractJobRecordError(match.errors);

                // Retry when AppFolio explicitly says "deadlock / lock timeout",
                // OR when it returns no error details at all — a null/empty errors
                // field on a failed record is AppFolio's silent deadlock pattern.
                const noDetails   = !match.errors ||
                                    (Array.isArray(match.errors) && !match.errors.length);
                const isRetriable = _isDeadlockError(err) || noDetails;

                if (isRetriable) {
                  // Collect for auto-retry — do NOT stamp the cell yet
                  retryQueue.push({ rowIdx: row.rowIdx, refId: row.refId });
                } else {
                  // Genuine (non-deadlock) error with a real message — stamp permanently
                  sheet.getRange(row.rowIdx, t[CONFIG.STATUS_COL] + 1)
                    .setValue('Error: ' + err).setBackground('#f4cccc');
                  // Track for audit log update
                  allResolvedByRef.set(row.refId, { resourceId: '', successful: false, error: err });
                }
              }
              // No match: job finished but result missing for this ref —
              // leave as "Pending:" so re-run can catch it.
            });
          });

          SpreadsheetApp.flush();
          Utilities.sleep(500);
        } catch (e) {
          console.error('Job fetch error: ' + e.message);
        }
      }

      // ── Jobs still in-progress on AppFolio's side ────────────────────────
      // Any job that didn't appear as "finished" in this pass hasn't been
      // processed by AppFolio yet. Keep them in pendingMap and re-poll
      // rather than exiting and forcing the user to manually click Sync again.
      const stillPendingJobIds = allJobIds.filter(id => !finishedJobIds.has(id));

      // ── Exit: all jobs finished and no deadlocks ──────────────────────────
      if (retryQueue.length === 0 && stillPendingJobIds.length === 0) break;

      // ── Exit: retry limit reached ─────────────────────────────────────────
      if (retryPass >= MAX_RETRY_PASSES) {
        if (retryQueue.length > 0) {
          console.warn(
            'Retry limit (' + MAX_RETRY_PASSES + ') reached — ' +
            retryQueue.length + ' row(s) stamped as final errors'
          );
          retryQueue.forEach(r => {
            sheet.getRange(r.rowIdx, t[CONFIG.STATUS_COL] + 1)
              .setValue('Error: deadlock (auto-retry limit reached)')
              .setBackground('#f4cccc');
            allResolvedByRef.set(r.refId, { resourceId: '', successful: false, error: 'deadlock (auto-retry limit reached)' });
          });
        }
        if (stillPendingJobIds.length > 0) {
          console.warn(
            stillPendingJobIds.length + ' job(s) still in-progress after retry limit — leaving as Pending for re-run'
          );
        }
        SpreadsheetApp.flush();
        break;
      }

      // ── Schedule next pass ────────────────────────────────────────────────
      retryPass++;
      const waitMs = retryPass * 3000;  // 3s → 6s → 9s → 12s → 15s

      if (retryQueue.length > 0) {
        // ── Deadlock path: resubmit deadlocked rows with fresh JobIds ───────
        console.warn(
          'Deadlock auto-retry ' + retryPass + '/' + MAX_RETRY_PASSES +
          ' — ' + retryQueue.length + ' row(s), waiting ' + waitMs + 'ms' +
          (stillPendingJobIds.length > 0 ? ', ' + stillPendingJobIds.length + ' still in-progress' : '')
        );
        ss.toast(
          retryQueue.length + ' deadlocked row(s) — auto-retrying (' +
          retryPass + '/' + MAX_RETRY_PASSES + ')…',
          'Tenant Sync', 8
        );

        // Snapshot still-in-progress entries BEFORE pendingMap is replaced,
        // then merge them back so they get re-polled next pass alongside the
        // newly submitted deadlock jobs.
        const stillPendingEntries = new Map();
        stillPendingJobIds.forEach(jobId => {
          if (pendingMap.has(jobId)) stillPendingEntries.set(jobId, pendingMap.get(jobId));
        });

        Utilities.sleep(waitMs);  // Let AppFolio DB settle before re-submitting

        pendingMap = _resubmitDeadlockedRows(retryQueue, data, headers, t, sheet);
        stillPendingEntries.forEach((rows, jobId) => pendingMap.set(jobId, rows));

        if (!pendingMap.size) {
          console.warn('Re-submission produced no new jobs — aborting retry loop');
          break;
        }

        Utilities.sleep(4000);  // Give AppFolio time to begin processing new jobs

      } else {
        // ── In-progress path: wait and re-poll (no re-submission needed) ────
        console.log(
          'Pass ' + retryPass + '/' + MAX_RETRY_PASSES + ': ' +
          stillPendingJobIds.length + ' job(s) still processing — waiting ' + waitMs + 'ms'
        );
        ss.toast(
          stillPendingJobIds.length + ' job(s) still processing — please wait…',
          'Tenant Sync', Math.ceil(waitMs / 1000) + 2
        );
        Utilities.sleep(waitMs);
        // pendingMap unchanged — same jobIds will be polled next pass
      }

    } while (true);

    // ── Phase 2: GET /tenants by TenantId → OccupancyId ─────
    // Job results contain TenantId (ResourceId) but NOT OccupancyId.
    // Fetch tenant records directly to extract it before stamping Success.
    const allTenantIds    = [...resolvedRows.keys()];
    const tenantChunkSize = 50;

    for (let i = 0; i < allTenantIds.length; i += tenantChunkSize) {
      if (new Date().getTime() - startTime > 330000) {
        ss.toast('Time limit approaching during OccupancyId fetch. Re-run Sync to continue.', '⚠️ PAUSED', 20);
        return;
      }

      const chunk    = allTenantIds.slice(i, i + tenantChunkSize);
      const idFilter = chunk.join(',');
      const endpoint = `${CONFIG.BASE_URL}/tenants?filters[Id]=${idFilter}&page[size]=${tenantChunkSize}`;
      const options  = { method: 'get', headers: getApiHeaders(), muteHttpExceptions: true };

      try {
        const resp    = UrlFetchApp.fetch(endpoint, options);
        const resText = resp.getContentText();

        if (resText.includes('Retry later')) {
          console.warn('Rate limit hit during tenant GET — sleeping 5s');
          Utilities.sleep(5000);
          i -= tenantChunkSize;
          continue;
        }

        const result  = JSON.parse(resText);
        const tenants = result.data || [];

        tenants.forEach(tenant => {
          const tenantId    = String(tenant.Id || '').trim();
          const occupancyId = String(tenant.OccupancyId || '').trim();
          const rowMeta     = resolvedRows.get(tenantId);
          if (!rowMeta) return;

          const { rowIdx } = rowMeta;
          if (occupancyId) {
            // Write OccupancyId as Note on API_ID — read path for prepRecurringCharges()
            sheet.getRange(rowIdx, t[CONFIG.API_ID_COL] + 1).setNote(occupancyId);
            if (t['OccupancyId'] !== undefined) {
              const occCell = sheet.getRange(rowIdx, t['OccupancyId'] + 1);
              occCell.setValue(occupancyId).setNote(occupancyId);
            }
          }
          // Stamp Success only after OccupancyId is confirmed written
          sheet.getRange(rowIdx, t[CONFIG.STATUS_COL] + 1)
            .setValue('Success').setBackground('#b6d7a8');
        });

        SpreadsheetApp.flush();
        Utilities.sleep(500);
      } catch (e) {
        console.error('Tenant GET error: ' + e.message);
      }
    }

    const suffix = retryPass > 0
      ? ' (' + retryPass + ' retry pass' + (retryPass !== 1 ? 'es' : '') + ')'
      : '';
    ss.toast('Sync Complete' + suffix + '.', 'Tenant Sync');

  } finally {
    // ── Update _Log Data so audit log reflects correct status ─────
    // Called in finally so it ALWAYS runs — even when Phase 1 returns
    // early due to the 280s time limit. allResolvedByRef was declared
    // outside the try block specifically so it's accessible here.
    //
    // syncTenantJobStatuses() runs as a sheet menu action, so _Log Data
    // is never updated automatically when jobs resolve. Without this call
    // the audit log shows "Pending: jobId" indefinitely and the sidebar
    // "Sync" button appears to do nothing (it polls the already-expired
    // job and finds nothing).
    //
    // Uses refId-based matching so it works even after deadlock auto-retries
    // that created new jobIds not stored in the original _Log Data entry.
    try { _updateAuditLogByRefIds(allResolvedByRef); } catch (e) {
      console.warn('_updateAuditLogByRefIds failed (non-fatal): ' + e.message);
    }

    lock.releaseLock();        // release atomic LockService lock
    _releaseOccupancyMutex();  // clear PropertiesService messaging record
  }
}


/**
 * Re-submits rows that deadlocked in the previous sync poll pass.
 *
 * Builds fresh occupant payloads from the original sheet data, sends
 * them to the /tenants/bulk endpoint in chunks of 25, and returns a
 * new pendingMap containing the fresh JobIds so the sync retry loop
 * can poll them on the next iteration.
 *
 * Rows whose POST fails outright (not just a deadlock in the job) are
 * stamped 'Error: …' immediately; they are excluded from the returned
 * pendingMap and will not be retried again.
 *
 * @param {Array<{rowIdx:number, refId:string}>} rows
 * @param {Array}  data    — full sheet data array (read at sync start)
 * @param {Array}  headers — header strings
 * @param {Object} t       — header → column-index map
 * @param {Sheet}  sheet   — Tenants sheet object
 * @returns {Map}  new pendingMap: jobId → [{ rowIdx, refId }]
 */
function _resubmitDeadlockedRows(rows, data, headers, t, sheet) {
  const newPendingMap = new Map();
  if (!rows.length) return newPendingMap;

  // Build payloads for each deadlocked row
  const payloads = [];
  const rowNums  = [];

  rows.forEach(function(r) {
    const rowData    = data[r.rowIdx - 1];
    const groupRefId = String(rowData[t[CONFIG.REF_ID_COL]] || '').trim();
    const unitId     = sheet.getRange(r.rowIdx, t['UnitName'] + 1).getNote();
    const occupants  = buildTenantObjects(rowData, t, unitId, groupRefId, headers, sheet, r.rowIdx);
    if (!occupants.length) return; // nameless primary — skip (already marked Skipped)
    payloads.push(occupants);
    rowNums.push(r.rowIdx);
  });

  if (!payloads.length) return newPendingMap;

  // Re-submit one row at a time — same pattern as the initial load.
  for (let i = 0; i < payloads.length; i++) {
    const flatPayload = payloads[i];
    const rowNum      = rowNums[i];
    const refId       = String(data[rowNum - 1][t[CONFIG.REF_ID_COL]] || '').trim();

    const result = callAppFolioAPI(
      CONFIG.ENDPOINTS.TENANTS_BULK, flatPayload, 'Occupancies', 'Retry', true
    );

    const jobId = (result.data && result.data.JobId)
      ? result.data.JobId
      : ((result.message && result.message.match(/"JobId"\s*:\s*"([^"]+)"/) || [])[1] || null);

    const statusCell = sheet.getRange(rowNum, t[CONFIG.STATUS_COL] + 1);
    if (result.success && jobId) {
      statusCell.setValue('Pending: ' + jobId).setBackground('#fff2cc');
      if (!newPendingMap.has(jobId)) newPendingMap.set(jobId, []);
      newPendingMap.get(jobId).push({ rowIdx: rowNum, refId: refId });
    } else {
      const errDetail = _parseAfErrorMessage(result.message) || ('HTTP ' + result.status);
      statusCell.setValue('Error: ' + errDetail).setBackground('#f4cccc');
    }

    SpreadsheetApp.flush();
    if (i < payloads.length - 1) Utilities.sleep(300);
  }

  return newPendingMap;
}


/**
 * Updates _Log Data result records whose referenceId appears in resolvedByRef.
 *
 * Called by syncTenantJobStatuses() after all jobs resolve (including any
 * deadlock auto-retry passes) so the audit log sidebar reflects correct
 * statuses without requiring the user to click the sidebar "Sync" button.
 *
 * WHY refId-based (not jobId-based):
 *   updateLogDataAfterSync() matches by jobId, which works perfectly for the
 *   normal case. But after a deadlock retry, _resubmitDeadlockedRows() creates
 *   NEW jobIds that are NOT stored in _Log Data (which still has the original
 *   load's jobIds). Using referenceId as the key works in all cases — original
 *   load, and any number of deadlock retries — because referenceId never changes.
 *
 * @param {Map<string, {resourceId:string, successful:boolean, error:string}>} resolvedByRef
 * @private
 */
function _updateAuditLogByRefIds(resolvedByRef) {
  if (!resolvedByRef || !resolvedByRef.size) return;

  const ss        = SpreadsheetApp.getActive();
  const dataSheet = ss.getSheetByName('_Log Data');
  if (!dataSheet) return;

  const rows    = dataSheet.getDataRange().getValues();
  const headers = rows[0].map(h => String(h).trim());
  const resIdx  = headers.indexOf('Results (JSON)');
  if (resIdx === -1) return;

  for (let i = 1; i < rows.length; i++) {
    const rawResults = String(rows[i][resIdx] || '[]');
    // Quick filter: skip rows with no pending-job-type results
    if (!rawResults.includes('"job"')) continue;

    let results;
    try { results = JSON.parse(rawResults); } catch (_) { continue; }

    let changed = false;
    const updated = results.map(function(r) {
      if (r._truncated)      return r;  // preserve truncation sentinel
      if (r.idType !== 'job') return r;  // already resolved to a direct ResourceId
      const resolved = resolvedByRef.get(String(r.referenceId).trim());
      if (!resolved) return r;           // not part of this sync batch

      changed = true;
      return {
        referenceId:  r.referenceId,
        successful:   resolved.successful,
        idType:       'direct',
        returnedId:   resolved.resourceId || '',
        errorMessage: resolved.error || ''
      };
    });

    if (changed) {
      // _safeCellArray handles the 50K char limit the same way logSystem.gs does
      dataSheet.getRange(i + 1, resIdx + 1).setValue(_safeCellArray(updated));
    }
  }
}


// ── Payload Builders ──────────────────────────────────────────

/**
 * Builds an array of occupant objects for one sheet row.
 * One row = one group (primary tenant + optional roommates).
 *
 * NAME GUARD: any occupant resolving to empty names after
 * CommercialName fallback is dropped rather than sent.
 * A nameless primary returns [] — the row is skipped entirely.
 * A nameless roommate is dropped individually.
 */
function buildTenantObjects(row, t, unitId, groupRefId, headers, sheet, rowNum) {
  const occupants = [];

  // ── Primary tenant ────────────────────────────────────────
  const primary = mapSingleTenant(row, t, unitId, groupRefId, true, 0);
  if (_hasResolvableName(primary)) {
    occupants.push(primary);
  } else {
    if (sheet && rowNum) {
      sheet.getRange(rowNum, t[CONFIG.STATUS_COL] + 1)
        .setValue('Skipped: Primary tenant has no resolvable name (FirstName + LastName, or CommercialName required)')
        .setBackground('#fff2cc');
    }
    return [];
  }

  // ── Roommates ─────────────────────────────────────────────
  headers.forEach((header, idx) => {
    if (!header.startsWith('RoommateFirst')) return;
    const rMatch = header.match(/\d+$/);
    const rNum   = rMatch ? rMatch[0] : null;
    if (!rNum || !row[idx] || String(row[idx]).trim() === '') return;

    const roommate = mapSingleTenant(row, t, unitId, groupRefId, false, rNum);
    if (_hasResolvableName(roommate)) {
      occupants.push(roommate);
    } else {
      if (sheet && rowNum) {
        sheet.getRange(rowNum, idx + 1)
          .setNote(`Roommate #${rNum} skipped — no resolvable name. Will include once AppFolio adds CompanyName support.`)
          .setBackground('#fff2cc');
      }
      console.warn(`Row ${rowNum}: Roommate #${rNum} dropped — empty name (groupRefId: ${groupRefId})`);
    }
  });

  return occupants;
}

/** @private */
function _hasResolvableName(occupantObj) {
  return String(occupantObj.FirstName || '').trim() !== ''
      || String(occupantObj.LastName  || '').trim() !== '';
}

/**
 * Extracts a human-readable error string from a job result record's errors
 * field. AppFolio returns errors in several shapes across different endpoints
 * and API versions — this handles all known formats:
 *
 *   string:             "deadlock detected on lock..."
 *   string[]:           ["field required", "invalid value"]
 *   {message:string}[]: [{message:"deadlock..."}, ...]
 *   object:             {message:"..."} or any other shape
 *   null / undefined:   no details available
 *
 * The old code was `Array.isArray(e) ? e.join(', ') : 'Unknown Error'`
 * which produced "Unknown Error" for every non-array shape, hiding the
 * real AppFolio error text (including deadlock messages) from the sheet.
 *
 * @param  {*} errors — the raw `errors` field from a job result record
 * @returns {string}
 * @private
 */
function _extractJobRecordError(errors) {
  if (!errors && errors !== 0) return 'Unknown error (no details returned by AppFolio)';
  if (typeof errors === 'string') return errors;
  if (Array.isArray(errors)) {
    if (!errors.length) return 'Unknown error (empty errors array)';
    return errors.map(function(e) {
      if (typeof e === 'string') return e;
      if (e && typeof e === 'object') return e.message || e.description || e.error || JSON.stringify(e);
      return String(e);
    }).join('; ');
  }
  if (typeof errors === 'object') {
    return errors.message || errors.description || errors.error || JSON.stringify(errors);
  }
  return String(errors);
}


/**
 * Returns true if an API error message is a transient AppFolio database
 * deadlock that is safe to retry.
 *
 * Known patterns:
 *  - "deadlock"               — Postgres deadlock surfaced directly
 *  - "lock timeout"           — Postgres lock-wait timeout
 *  - "concurrent modification"— optimistic-concurrency conflict
 *  - "unknown error"          — AppFolio's own generic message returned
 *                               when a background job record deadlocks;
 *                               they do not expose the Postgres detail
 *                               to API consumers, so this vague string
 *                               is their silent deadlock indicator.
 * @private
 */
function _isDeadlockError(message) {
  if (!message) return false;
  const lower = String(message).toLowerCase();
  return lower.includes('deadlock')
      || lower.includes('lock timeout')
      || lower.includes('concurrent modification')
      || lower === 'unknown error';
}


/**
 * Parses an AppFolio 4xx error JSON string into readable per-record lines.
 * Falls back to raw string (un-truncated) if parsing fails.
 * @private
 */
function _parseAfErrorMessage(rawMessage) {
  if (!rawMessage) return '';  // empty so callers' || fallback fires
  try {
    const parsed = JSON.parse(rawMessage);
    const items  = parsed.errors || parsed.data || [];
    if (!items.length) return rawMessage;
    const lines = [];
    items.forEach(item => {
      const ref  = item.ReferenceId || item.referenceId || '';
      const errs = Array.isArray(item.errors) ? item.errors : [];
      errs.forEach(e => {
        const attr = e.attribute || e.field || '';
        const msg  = e.message   || '';
        lines.push(ref ? `[${ref}] ${attr}: ${msg}` : `${attr}: ${msg}`);
      });
    });
    return lines.length ? lines.join('\n') : rawMessage;
  } catch (_) {
    return rawMessage;
  }
}

/**
 * Discovers inline RC slot column indices from the header row.
 *
 * Scans for columns matching "Addtl Recurring Charge GL #N" and
 * builds a slot descriptor for each numbered group found.
 *
 * @param {string[]} headers — Full header row from Tenants sheet
 * @param {Object}   t       — Header → column index map
 * @returns {Array<{n, glIdx, startIdx, endIdx, amtIdx, descIdx, freqIdx}>}
 * @private
 */
function _buildRcSlots(headers, t) {
  const slots = [];
  headers.forEach((h, idx) => {
    const m = h.match(/^Addtl Recurring Charge GL #(\d+)$/i);
    if (!m) return;
    const n = m[1];
    slots.push({
      n,
      glIdx:    idx,
      startIdx: t[`Addtl Recurring Charge Start Date #${n}`],
      endIdx:   t[`Addtl Recurring Charge End Date #${n}`],
      amtIdx:   t[`Addtl Recurring Charge Amount #${n}`],
      descIdx:  t[`Addtl Recurring Charge Description #${n}`],
      freqIdx:  t[`Addtl Recurring Charge Frequency #${n}`]
    });
  });
  return slots;
}

/**
 * Maps a single tenant (primary or roommate) to the API payload shape.
 */
function mapSingleTenant(row, t, unitId, groupRefId, isPrimary, rNum) {
  const getValue  = key => (t[key] !== undefined) ? row[t[key]] : null;
  const isPending = getValue('PendingMoveIn')
    ? String(getValue('PendingMoveIn')).toLowerCase() === 'yes'
    : false;

  // Name resolution: CommercialName → FirstName fallback for commercial tenants
  let firstName        = isPrimary ? getValue('FirstName') : getValue(`RoommateFirst${rNum}`);
  let lastName         = isPrimary ? getValue('LastName')  : getValue(`RoommateLast${rNum}`);
  const commercialName = getValue('CommercialName');
  if (!String(firstName || '').trim() && !String(lastName || '').trim() && commercialName) {
    firstName = String(commercialName).trim();
    lastName  = '';
  }

  // Move-in/out: roommates fall back to primary dates if their own are blank
  const primaryMoveIn   = getValue('MoveIn');
  const roommateMoveIn  = isPrimary ? null : getValue(`RoommateMoveIn${rNum}`);
  const moveInDate      = isPrimary ? primaryMoveIn : (roommateMoveIn || primaryMoveIn);

  const primaryMoveOut  = getValue('MoveOut');
  const roommateMoveOut = isPrimary ? null : getValue(`RoommateMoveOut${rNum}`);
  const moveOutDate     = isPrimary ? primaryMoveOut : (roommateMoveOut || primaryMoveOut);

  // ── Emails ────────────────────────────────────────────────
  const emails = [];
  if (isPrimary) {
    for (let j = 1; j <= 5; j++) {
      const email = getValue(`EmailAddress${j}`);
      if (email) emails.push({ EmailAddress: String(email), Type: j === 1 ? 'Primary' : 'Other' });
    }
  } else {
    const email = getValue(`RoommateEmail${rNum}`);
    if (email) emails.push({ EmailAddress: String(email), Type: 'Primary' });
  }

  // ── Phones ────────────────────────────────────────────────
  const phones = [];
  if (isPrimary) {
    for (let j = 1; j <= 5; j++) {
      const phoneVal = getValue(`PhoneNumber${j}`);
      if (phoneVal) {
        String(phoneVal).split(',').forEach(num => {
          phones.push({ Number: num.trim(), Label: getValue(`Label #${j}`) || 'Mobile', IsPrimary: false });
        });
      }
    }
  } else {
    const rmPhone = getValue(`RoommatePhone${rNum}`);
    if (rmPhone) {
      String(rmPhone).split(',').forEach(num => {
        phones.push({ Number: num.trim(), Label: 'Mobile', IsPrimary: false });
      });
    }
  }
  if (phones.length > 0) phones[0].IsPrimary = true;

  // ── Addresses (primary only) ──────────────────────────────
  const addresses = [];
  if (isPrimary && getValue('Address1')) {
    addresses.push({
      Address1:   String(getValue('Address1') || ''),
      Address2:   String(getValue('Address2') || ''),
      City:       String(getValue('City')     || ''),
      State:      String(getValue('State')    || ''),
      PostalCode: String(getValue('Zip')      || ''),
      Type:       'Previous',
      IsPrimary:  true
    });
  }

  // ── Tags (primary only) ───────────────────────────────────
  const tags = [];
  if (isPrimary) {
    const rawTags = getValue('Tags');
    if (rawTags) String(rawTags).split(',').map(s => s.trim()).filter(Boolean).forEach(tag => tags.push(tag));
  }

  // PendingMoveOut flag (primary tenant only)
  const isPendingMoveOut = isPrimary
    ? String(getValue('PendingMoveOut') || '').toLowerCase() === 'yes'
    : false;

  const tenantObj = {
    ReferenceId:   isPrimary ? groupRefId : `${groupRefId}_RM_${rNum}`,
    FirstName:     String(firstName || '').trim(),
    LastName:      String(lastName  || '').trim(),
    MoveInOn:      castDate(moveInDate),
    PrimaryTenant: isPrimary,
    TenantType:    isPrimary
      ? 'Financially Responsible'
      : (getValue(`RoommateType${rNum}`) || 'Financially Responsible'),
    Tags: tags
  };

  // Only include pending flags when true — omit false to keep payload clean
  if (isPending)        tenantObj.PendingMoveIn  = true;
  if (isPendingMoveOut) tenantObj.PendingMoveOut = true;

  if (emails.length    > 0) tenantObj.Emails       = emails;
  if (phones.length    > 0) tenantObj.PhoneNumbers = phones;
  if (addresses.length > 0) tenantObj.Addresses    = addresses;

  if (moveOutDate)               tenantObj.MoveOutOn      = castDate(moveOutDate);
  if (getValue('LeaseFrom'))     tenantObj.LeaseStartDate = castDate(getValue('LeaseFrom'));
  if (getValue('LeaseTo'))       tenantObj.LeaseEndDate   = castDate(getValue('LeaseTo'));
  if (getValue('LeaseSignedOn')) tenantObj.LeaseSignedAt  = castDate(getValue('LeaseSignedOn'));

  if (isPending) tenantObj.PendingMoveInGroupReferenceId = groupRefId;
  if (unitId)    tenantObj.UnitId = unitId;

  return tenantObj;
}

/**
 * Formats any date value to 'yyyy-MM-dd'. Returns null for blank/invalid.
 */
function castDate(val) {
  if (!val || val === '') return null;
  try {
    const d = new Date(val);
    if (isNaN(d.getTime())) return null;
    return Utilities.formatDate(d, 'GMT', 'yyyy-MM-dd');
  } catch (e) { return null; }
}


function prepRecurringCharges() {
  const ss    = SpreadsheetApp.getActive();
  const sheet = ss.getSheetByName('Tenants');
  if (!sheet) return;

  const data    = sheet.getDataRange().getValues();
  const headers = data[0].map(h => String(h).trim());
  const t       = {};
  headers.forEach((title, i) => { t[title] = i; });

  const slots       = _buildRcSlots(headers, t);
  const rcStatusIdx = t['Charge_Load_Status'];
  const apiIdIdx    = t[CONFIG.API_ID_COL];
  const statusIdx   = t[CONFIG.STATUS_COL];

  if (!slots.length)              { ss.toast('No RC slot columns found.',          'RC Prep'); return; }
  if (rcStatusIdx === undefined)  { ss.toast('Charge_Load_Status column missing.', 'RC Prep'); return; }

  // ── GL Account lookup: Number → UUID (Note on GL Accounts Number cell) ─
  const glAccountMap = {};
  const glSheet = ss.getSheetByName('GL Accounts');
  if (glSheet) {
    const glData    = glSheet.getDataRange().getValues();
    const glHeaders = glData[0].map(h => String(h).trim());
    const glNumIdx  = glHeaders.indexOf('Number');
    if (glNumIdx >= 0) {
      const glNotes = glSheet.getRange(1, glNumIdx + 1, glData.length, 1).getNotes();
      glData.forEach((row, j) => {
        if (j === 0) return;
        const num = String(row[glNumIdx] || '').trim();
        const id  = glNotes[j][0];
        if (num && id) glAccountMap[num] = id;
      });
    }
  }

  // ── Batch-read OccupancyId Notes from API_ID column ───────
  const apiIdNotes = sheet.getRange(1, apiIdIdx + 1, data.length, 1).getNotes();

  for (let i = 1; i < data.length; i++) {
    const rowNum = i + 1;

    // Only process rows where tenant is fully synced
    if (String(data[i][statusIdx] || '').trim() !== 'Success') continue;

    // Skip rows where all RCs are already done
    if (String(data[i][rcStatusIdx] || '').trim() === 'Success') continue;

    const occupancyId = apiIdNotes[i][0];
    if (!occupancyId) {
      sheet.getRange(rowNum, rcStatusIdx + 1)
        .setValue('Error: OccupancyId Note missing on API_ID — re-run Tenant Sync')
        .setBackground('#f4cccc');
      continue;
    }

    let rowHasSlot  = false;
    let rowHasError = false;

    slots.forEach(slot => {
      const glNum = String(data[i][slot.glIdx] || '').trim();
      if (!glNum) return;  // slot unused on this row

      rowHasSlot = true;
      const glCell = sheet.getRange(rowNum, slot.glIdx + 1);

      // Already successfully loaded — leave it alone
      if (glCell.getBackground() === '#b6d7a8') return;

      // ── Resolve GL UUID → write as Note on GL cell (same pattern as old version) ─
      // Cell may contain "4100: Rent Income" — extract just the number before the colon
      const glKey    = glNum.split(':')[0].trim();
      const glAcctId = glAccountMap[glKey];
      if (glAcctId) {
        glCell.setNote(glAcctId).setBackground('');
      } else {
        // Don't touch Note — preserve any existing UUID. Flag red only.
        glCell.setBackground('#f4cccc');
        rowHasError = true;
        return;  // can't validate further without a valid GL
      }

      // ── Required: Amount ───────────────────────────────────
      const amt     = data[i][slot.amtIdx];
      const amtCell = slot.amtIdx !== undefined ? sheet.getRange(rowNum, slot.amtIdx + 1) : null;
      if (!amt && amt !== 0) {
        if (amtCell) amtCell.setBackground('#f4cccc');
        rowHasError = true;
      } else {
        if (amtCell) amtCell.setBackground('');
      }

      // ── Default Frequency → write 'Monthly' if blank ──────
      if (slot.freqIdx !== undefined && !String(data[i][slot.freqIdx] || '').trim()) {
        sheet.getRange(rowNum, slot.freqIdx + 1).setValue('Monthly');
      }

      // StartDate: NOT written to sheet. Defaults to 1st of next month in payload.
    });

    // ── Stamp row-level Charge_Load_Status ────────────────
    const rcCell = sheet.getRange(rowNum, rcStatusIdx + 1);
    if (!rowHasSlot) {
      rcCell.setValue('No RC slots').setBackground('#efefef');
    } else if (rowHasError) {
      rcCell.setValue('Error: fix highlighted slots').setBackground('#f4cccc');
    } else {
      rcCell.setValue('Ready').setBackground('');
    }
  }

  applyConditionalRules(sheet, headers, rcStatusIdx);
  ss.toast('Recurring Charges Prepped.', 'RC Prep');
}


function executeRecurringChargeLoad() {
  const ss    = SpreadsheetApp.getActive();
  const sheet = ss.getSheetByName('Tenants');
  if (!sheet) return;

  const data    = sheet.getDataRange().getValues();
  const headers = data[0].map(h => String(h).trim());
  const t       = {};
  headers.forEach((title, i) => { t[title] = i; });

  const slots       = _buildRcSlots(headers, t);
  const rcStatusIdx = t['Charge_Load_Status'];
  const apiIdIdx    = t[CONFIG.API_ID_COL];

  if (!slots.length) { ss.toast('No RC slot columns found.', 'RC Load'); return; }

  // ── First of next month — StartDate default ────────────────
  const now            = new Date();
  const nextMonthFirst = Utilities.formatDate(
    new Date(now.getFullYear(), now.getMonth() + 1, 1), 'GMT', 'yyyy-MM-dd'
  );

  // ── Batch-read OccupancyId Notes from API_ID column ───────
  const apiIdNotes = sheet.getRange(1, apiIdIdx + 1, data.length, 1).getNotes();

  for (let i = 1; i < data.length; i++) {
    const rowNum = i + 1;
    if (String(data[i][rcStatusIdx] || '').trim() !== 'Ready') continue;

    // Read OccupancyId from Note on API_ID (written by syncTenantJobStatuses)
    const occupancyId = apiIdNotes[i][0];
    if (!occupancyId) {
      sheet.getRange(rowNum, rcStatusIdx + 1)
        .setValue('Error: OccupancyId Note missing on API_ID — re-run Tenant Sync')
        .setBackground('#f4cccc');
      continue;
    }

    let slotSuccess = 0;
    let slotError   = 0;
    let slotTotal   = 0;

    slots.forEach(slot => {
      const glNum = String(data[i][slot.glIdx] || '').trim();
      if (!glNum) return;

      slotTotal++;
      const glCell   = sheet.getRange(rowNum, slot.glIdx + 1);
      const glAcctId = glCell.getNote();  // UUID written during prep

      // Already loaded — green background means done
      if (glCell.getBackground() === '#b6d7a8') { slotSuccess++; return; }

      // No UUID note — wasn't prepped properly
      if (!glAcctId) {
        glCell.setBackground('#f4cccc');
        slotError++;
        return;
      }

      // ── Build payload (mirrors old working version structure) ─
      const startRaw = slot.startIdx !== undefined ? data[i][slot.startIdx] : null;
      const endRaw   = slot.endIdx   !== undefined ? data[i][slot.endIdx]   : null;
      const freq     = slot.freqIdx  !== undefined ? String(data[i][slot.freqIdx] || '').trim() : '';
      const desc     = slot.descIdx  !== undefined ? String(data[i][slot.descIdx] || '').trim() : '';
      const amt      = slot.amtIdx   !== undefined ? data[i][slot.amtIdx] : null;

      const payload = {
        OccupancyId: occupancyId,
        GlAccountId: glAcctId,
        Amount:      castPropertyType('FlatAmount', amt),
        Frequency:   freq || 'Monthly',
        StartDate:   castDate(startRaw) || nextMonthFirst
      };

      if (desc)   payload.Description = desc;
      if (endRaw) payload.EndDate     = castDate(endRaw);

      const result = callAppFolioAPI(CONFIG.ENDPOINTS.RECURRING_CHARGES, payload, 'RecurringCharges');

      if (result.success && result.data) {
        // Green background = done. Note (UUID) preserved — never overwritten.
        glCell.setBackground('#b6d7a8');
        slotSuccess++;
      } else {
        // Red background = failed. Note (UUID) preserved for retry.
        glCell.setBackground('#f4cccc');
        slotError++;
      }

      SpreadsheetApp.flush();
    });

    // ── Roll up Charge_Load_Status ─────────────────────────
    const rcCell = sheet.getRange(rowNum, rcStatusIdx + 1);
    if (slotTotal === 0) {
      // nothing to do — leave status as-is
    } else if (slotError === 0 && slotSuccess === slotTotal) {
      rcCell.setValue('Success').setBackground('#b6d7a8');
    } else if (slotSuccess === 0) {
      rcCell.setValue(`Error: ${slotError}/${slotTotal} slots failed`).setBackground('#f4cccc');
    } else {
      rcCell.setValue(`Partial: ${slotSuccess} ok, ${slotError} failed`).setBackground('#fff2cc');
    }
  }

  ss.toast('Recurring Charge Load Complete.', 'RC Load');
}
