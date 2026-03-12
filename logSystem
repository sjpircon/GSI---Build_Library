// ============================================================
// LOGSYSTEM.GS — Two-Layer Audit Engine + Structured Data Store  (v5)
//
// SHEET ARCHITECTURE
// ──────────────────
//   "API Log"     — user-facing, presentational only. Clean rows,
//                   status chips, one-line summary. No raw JSON.
//
//   "_Log Data"   — hidden structured store. One row per log entry,
//                   keyed by Log ID (UUID). Feeds the sidebar viewer.
//                   Columns: Log ID | Timestamp | Object | Action |
//                            Payload JSON | Response JSON |
//                            Results JSON | Errors JSON
//
//   "_Debug Log"  — raw HTTP detail. Payload + full response text.
//                   Auto-purges entries older than 30 days.
//
// ENTRY POINT
// ───────────
//   logResponse({ action, object, recordCount, request,
//                 responseText, responseJson, statusCode, rowErrors })
//
//   !! ALWAYS use named parameters (object literal) !!
//   !! Positional args will silently fail — object arrives as   !!
//   !! undefined and the dropdown shows "(unknown)" in the log. !!
//
//   Correct:
//     logResponse({
//       action:       'Load',
//       object:       'Owners',
//       recordCount:  ownerArray.length,
//       request:      { data: ownerArray },
//       responseText: rawText,
//       responseJson: parsedJson,
//       statusCode:   200
//     });
//
//   Wrong (positional — breaks silently):
//     logResponse('Load', { data: ownerArray }, 'Success msg', parsedJson);
//
// OBJECT → ID TYPE MAP
// ────────────────────
//   Job ID (async, needs sync):  Owners, Unit Types, Occupancies, Vendors
//   Direct ID (sync):            Banks, Properties, Owner Groups, Units,
//                                Rec. Charges
// ============================================================


// ── Brand Tokens — mirrors CSS :root in checklist HTML ───────
const AF = {
  sky45:     '#0F97FF',
  sky75:     '#08428D',
  sky15:     '#CDE9F9',
  street15:  '#EDF0F4',
  street30:  '#D0D7E2',
  street90:  '#1E2430',
  successBg: '#E6F4EA', successTxt: '#0A654B',
  warnBg:    '#FFF8F0', warnTxt:    '#A05E00',
  errorBg:   '#FDECEA', errorTxt:   '#B71C1C',
  importBg:  '#FFF8F0', importTxt:  '#A05E00',
  liveBg:    '#08428D', liveTxt:    '#FFFFFF',
  white:     '#FFFFFF',
};


// ── Object → ID Type + Field Name Map ────────────────────────
// 'job'    = async, returns a Job ID that resolves later via sync script
// 'direct' = sync,  returns the AF-assigned ID immediately
//
// idField: the exact key in the API response that holds the returned ID.
const ID_TYPE = {
  'Banks':          { type: 'direct', idField: 'BankAccountId' },
  'Owners':         { type: 'job',    idField: 'JobId'         },
  'Properties':     { type: 'direct', idField: 'PropertyId'    },
  'Owner Groups':   { type: 'direct', idField: 'Id'            },
  'Unit Types':     { type: 'job',    idField: 'JobId'         },
  'Units':          { type: 'direct', idField: 'UnitId'        },
  'Occupancies':    { type: 'job',    idField: 'JobId'         },
  'Tenants':        { type: 'job',    idField: 'JobId'         },
  'Prop Late Fees': { type: 'job',    idField: 'JobId'         },
  'Occ Late Fees':  { type: 'job',    idField: 'JobId'         },
  'Rec. Charges': { type: 'direct', idField: 'Id'            },
  'Vendors':      { type: 'job',    idField: 'JobId'         },
};


// ── Entry Point ──────────────────────────────────────────────

/**
 * Single entry point called by all API load functions.
 *
 * @param {Object} opts                    ← MUST be an object literal
 *   @param {string}  opts.action          - 'Prep' | 'Load' | 'Sync'
 *   @param {string}  opts.object          - 'Banks' | 'Owners' | etc.
 *   @param {number}  [opts.recordCount]   - Total rows processed
 *   @param {*}       [opts.request]       - Payload sent to API
 *   @param {string}  [opts.responseText]  - Raw API response string
 *   @param {*}       [opts.responseJson]  - Parsed API response object
 *   @param {number}  [opts.statusCode]    - HTTP status code
 *   @param {Array}   [opts.rowErrors]     - Array of { row, message }
 */
function logResponse(opts) {

  // ── GUARD: detect positional arg misuse ──────────────────
  // If opts is not a plain object (e.g. it's a string like 'Load'),
  // the caller is using the OLD positional signature. Log a loud
  // error and return — a silent write with blank Object/Action is
  // worse than no write at all.
  if (!opts || typeof opts !== 'object' || Array.isArray(opts)) {
    console.error(
      'logResponse() called with positional arguments — this is not supported.\n' +
      'Use named parameters: logResponse({ action, object, recordCount, ... })\n' +
      'First argument received: ' + JSON.stringify(opts)
    );
    return;
  }

  // ── 1. Unpack ────────────────────────────────────────────
  const action      = String(opts.action      || 'Load');
  const object      = String(opts.object      || '');
  const recordCount = opts.recordCount != null ? Number(opts.recordCount) : null;
  const request     = opts.request     || null;
  const respText    = String(opts.responseText || '');
  const respJson    = opts.responseJson || null;
  const statusCode  = Number(opts.statusCode  || 0);
  const rawErrors   = Array.isArray(opts.rowErrors) ? opts.rowErrors : [];

  // ── GUARD: warn if object is blank ───────────────────────
  // object drives the dropdown label and ID type lookup. If it
  // arrives blank the sidebar shows "(unknown)" for every entry.
  // This fires when a load script omits opts.object entirely.
  if (!object) {
    console.warn(
      'logResponse() called with a blank object — dropdown will show "(unknown)".\n' +
      'Check that the calling function passes opts.object, e.g. object: "Owners".'
    );
  }

  // ── 2. Identity / environment ────────────────────────────
  let userEmail = '';
  try { userEmail = Session.getActiveUser().getEmail() || ''; } catch (e) {}
  const env = PropertiesService.getUserProperties().getProperty('AF_ACTIVE_SET') || 'IMPORT';

  // ── 3. Parse bulk response ───────────────────────────────
  const bulkData = _extractBulkData(respJson);

  // ── 4. Normalise errors ──────────────────────────────────
  const errors = _normaliseErrors(rawErrors, bulkData);

  // ── 5. Extract results ───────────────────────────────────
  // Pass request + respJson so _extractResults can build pending
  // placeholder rows for job-type objects (Owners, Unit Types, etc.)
  // where bulkData is empty at load time — only a JobId is returned.
  // opts.prebuiltResults bypasses extraction for batched loads that
  // accumulate results across multiple chunks and log once at the end.
  const results = opts.prebuiltResults || _extractResults(bulkData, object, request, respJson);

  // ── 6. Status classification ─────────────────────────────
  const successCount = results.filter(r => r.successful).length || null;
  const isHardError  = statusCode >= 400
    || (statusCode === 0 && respText.toLowerCase().includes('error') && !bulkData.length);

  let chipLabel, chipBg, chipTxt;

  if (isHardError && !errors.length && successCount === null) {
    chipLabel = 'Error';
    chipBg    = AF.errorBg;
    chipTxt   = AF.errorTxt;
  } else if (
    errors.length > 0
    || (successCount !== null && recordCount !== null && successCount < recordCount)
  ) {
    const ok    = successCount !== null ? successCount : Math.max(0, (recordCount || 0) - errors.length);
    const total = recordCount  || (ok + errors.length);
    chipLabel   = `Partial  ${ok} / ${total}`;
    chipBg      = AF.warnBg;
    chipTxt     = AF.warnTxt;
  } else {
    const n   = successCount !== null ? successCount : recordCount;
    chipLabel = n != null ? `Success  ${n} / ${n}` : 'Success';
    chipBg    = AF.successBg;
    chipTxt   = AF.successTxt;
  }

  // ── 7. Summary ───────────────────────────────────────────
  const summary = _buildSummary({ chipLabel, object, recordCount, errors, statusCode });

  // ── 8. Shared identifiers ────────────────────────────────
  const logId = opts.logId || Utilities.getUuid();
  const now   = new Date();
  const tzFmt = s => Utilities.formatDate(now, Session.getScriptTimeZone(), s);
  const ss    = SpreadsheetApp.getActive();

  // ── 9. Write to API Log (presentational) ─────────────────
  const logSheet = ss.getSheetByName('API Log') || setupLogTab();
  logSheet.insertRowAfter(1);
  const rowRange = logSheet.getRange(2, 1, 1, 8);

  rowRange.setValues([[
    tzFmt('MM/dd/yy h:mm a'),
    userEmail,
    env,
    action,
    object,
    recordCount != null ? recordCount : '',
    chipLabel,
    summary
  ]]);

  rowRange
    .setBackground(AF.white)
    .setFontColor(AF.street90)
    .setFontFamily('Helvetica Neue')
    .setFontSize(10)
    .setVerticalAlignment('top')
    .setFontWeight('normal');

  if (logSheet.getLastRow() % 2 === 0) rowRange.setBackground(AF.street15);

  logSheet.getRange(2, 7)
    .setBackground(chipBg).setFontColor(chipTxt)
    .setFontWeight('bold').setHorizontalAlignment('center').setFontSize(9);

  const envCell = logSheet.getRange(2, 3);
  env === 'LIVE'
    ? envCell.setBackground(AF.liveBg).setFontColor(AF.liveTxt).setFontWeight('bold').setHorizontalAlignment('center')
    : envCell.setBackground(AF.importBg).setFontColor(AF.importTxt).setFontWeight('bold').setHorizontalAlignment('center');

  logSheet.getRange(2, 6).setHorizontalAlignment('right');

  // ── 10. Write Log ID to hidden API Log col I ─────────────
  // Stored silently — used by getActiveLogId() to pre-load the
  // sidebar when a user has an API Log row selected.
  logSheet.getRange(2, 9).setValue(logId);

  // ── 11. Write to _Log Data (structured store) ────────────
  // payload is always stringified here so getLogEntry() in
  // LogViewer.gs always receives a JSON string, never a raw object.
  // This fixes the sidebar showing "{ }" for the payload section.
  //
  // _safeCellValue() caps each JSON column at 49 000 chars to avoid
  // "Your input contains more than the maximum of 50 000 characters
  // in a single cell" — common for large batched Occupancy/Vendor
  // loads where the full payload JSON can far exceed the limit.
  const payloadStr = (request && typeof request === 'object')
    ? JSON.stringify(request)
    : String(request || '{}');

  const dataSheet = ss.getSheetByName('_Log Data') || setupLogTab();
  dataSheet.insertRowAfter(1);
  dataSheet.getRange(2, 1, 1, 8).setValues([[
    logId,
    tzFmt('MM/dd/yy h:mm:ss a'),
    object,
    action,
    _safeCellValue(payloadStr),           // Truncated if > 49 000 chars (display only)
    _safeCellValue(respText),             // Truncated if > 49 000 chars (display only)
    _safeCellArray(results),              // Record-boundary truncation — always valid JSON
    _safeCellArray(errors)                // Record-boundary truncation — always valid JSON
  ]]);

  // ── 12. Write to _Debug Log (raw HTTP detail) ────────────
  const dbSheet = ss.getSheetByName('_Debug Log') || setupLogTab();
  dbSheet.insertRowAfter(1);
  dbSheet.getRange(2, 1, 1, 6).setValues([[
    logId,
    tzFmt('MM/dd/yy h:mm:ss a'),
    object,
    statusCode || '',
    _safeCellValue(payloadStr),
    _safeCellValue(respText)
  ]]);

  _purgeOldDebugEntries(dbSheet);
}


// ── Results Extractor ─────────────────────────────────────────

/**
 * Builds a structured results array from the bulk API response.
 *
 * For DIRECT objects (Banks, Properties, etc.) bulkData contains
 * the full result set immediately — one entry per record.
 *
 * For JOB objects (Owners, Unit Types, Vendors, Occupancies) the
 * initial API response only returns a single JobId — bulkData is
 * empty at load time. In this case we build a pending placeholder
 * row for every record in the original request payload, using the
 * JobId as returnedId. updateLogDataAfterSync() will swap these
 * out for real ResourceIds once the job resolves.
 *
 * Success (direct)  → { referenceId, successful: true,  idType: 'direct', returnedId: AF_ID }
 * Success (job)     → { referenceId, successful: true,  idType: 'job',    returnedId: JobId }
 * Failure           → { referenceId, successful: false, errors: [...] }
 *
 * @private
 */
function _extractResults(bulkData, object, request, respJson) {
  const cfg    = ID_TYPE[object] || { type: 'direct', idField: 'Id' };
  const idType = cfg.type;

  // ── Job-type object with empty bulkData ───────────────────
  // The API returned only a JobId — no per-record results yet.
  // Build a pending placeholder for each record in the payload
  // so the sidebar can show "⏳ Pending sync" immediately and
  // the refresh button has rows to update once sync runs.
  if (!bulkData.length && idType === 'job' && respJson) {
    const jobId   = (respJson.data && respJson.data.JobId) || respJson.JobId || respJson.jobId || '';
    const records = (request && Array.isArray(request.data)) ? request.data : [];

    if (jobId && records.length) {
      return records.map(rec => ({
        referenceId: String(rec.ReferenceId || rec.referenceId || ''),
        successful:  true,
        idType:      'job',
        returnedId:  jobId   // placeholder — swapped for ResourceId by sync
      }));
    }
  }

  // ── No data at all — return empty ─────────────────────────
  if (!bulkData.length) return [];

  // ── Direct object — full results available immediately ────
  return bulkData.map(rec => {
    const ref = rec.ReferenceId || rec.referenceId || '';

    if (rec.successful === false) {
      return {
        referenceId: ref,
        successful:  false,
        errors: Array.isArray(rec.errors)
          ? rec.errors.map(e => ({
              field:   _cap(e.attribute || e.field || ''),
              message: _cap(e.message   || '')
            }))
          : []
      };
    }

    const idField    = cfg.idField;
    const returnedId = rec[idField] || (rec.data && rec.data[idField]) || '';

    return {
      referenceId: String(ref),
      successful:  true,
      idType:      idType,
      returnedId:  String(returnedId)
    };
  });
}


// ── Error Normaliser ─────────────────────────────────────────

/**
 * Converts errors to clean structured objects.
 * bulkData takes full priority — never double-processes.
 * Returns: [{ ref, field, message }]
 * @private
 */
function _normaliseErrors(rawErrors, bulkData) {
  if (bulkData.length) {
    const out = [];
    bulkData.forEach(rec => {
      if (rec.successful === false && Array.isArray(rec.errors)) {
        const ref = rec.ReferenceId || rec.referenceId || 'Unknown';
        rec.errors.forEach(e => out.push({
          ref:     ref,
          field:   _cap(e.attribute || e.field || ''),
          message: _cap(e.message   || 'Unknown error')
        }));
      }
    });
    return out;
  }

  const out = [];
  rawErrors.forEach(e => {
    const raw      = String(e.message || e || '');
    const fromJson = _parseJsonErrors(raw);
    if (fromJson.length) {
      fromJson.forEach(fe => out.push(fe));
    } else {
      out.push({ ref: e.row ? `Row ${e.row}` : '', field: '', message: _cap(raw) });
    }
  });
  return out;
}


/** @private */
function _parseJsonErrors(str) {
  if (!str || (str.charAt(0) !== '{' && str.charAt(0) !== '[')) return [];
  try {
    const parsed = JSON.parse(str);
    const out    = [];
    const items  = parsed.data || parsed.errors || (Array.isArray(parsed) ? parsed : [parsed]);
    items.forEach(item => {
      if (item.successful === false && Array.isArray(item.errors)) {
        const ref = item.ReferenceId || item.referenceId || '';
        item.errors.forEach(e => out.push({
          ref, field: _cap(e.attribute || e.field || ''), message: _cap(e.message || '')
        }));
      } else if (item.attribute || item.field) {
        out.push({ ref: '', field: _cap(item.attribute || item.field || ''), message: _cap(item.message || '') });
      }
    });
    return out;
  } catch (_) { return []; }
}


/** @private */
function _extractBulkData(respJson) {
  if (!respJson) return [];
  if (Array.isArray(respJson))      return respJson;
  if (Array.isArray(respJson.data)) return respJson.data;

  // AppFolio wraps 4xx validation errors as { "errors": [...] }
  // where each item has the same shape as a bulk result record
  // (ReferenceId, successful: false, errors: [{ attribute, message }]).
  // Without this, the log shows empty results for every failed batch.
  if (Array.isArray(respJson.errors)) return respJson.errors;

  return [];
}


// ── Summary Builder ───────────────────────────────────────────

/** @private */
function _buildSummary({ chipLabel, object, recordCount, errors, statusCode }) {
  if (chipLabel.startsWith('Success')) {
    const n = recordCount != null ? recordCount : '';
    return `${n ? n + ' ' : ''}${object} record${n !== 1 ? 's' : ''} loaded successfully.`.trim();
  }
  if (chipLabel === 'Error' && !errors.length) {
    return `Load failed${statusCode ? ' (HTTP ' + statusCode + ')' : ''}.\nSee _Debug Log for details.`;
  }

  const failedRefs = new Set(errors.map(e => e.ref).filter(Boolean));
  const fieldNames = new Set(errors.map(e => e.field).filter(Boolean));
  const failCount  = failedRefs.size || errors.length;
  const total      = recordCount || failCount;

  const lines = [`${failCount} of ${total} record${failCount !== 1 ? 's' : ''} failed`];
  if (fieldNames.size) {
    const f = [...fieldNames];
    lines.push(`Fields: ${f.slice(0, 5).join(', ')}${f.length > 5 ? ` (+${f.length - 5} more)` : ''}`);
  }
  lines.push('Open sidebar for record-level detail.');
  return lines.join('\n');
}


// ── Helpers ───────────────────────────────────────────────────

/**
 * Ensures a value stored in a single Google Sheets cell never
 * exceeds the 50 000-character limit. Values over maxLen are
 * truncated and suffixed with a note so the truncation is visible.
 *
 * This prevents "Your input contains more than the maximum of
 * 50 000 characters in a single cell" exceptions that silently
 * kill the entire logResponse() call — leaving no audit entry
 * and making the Audit sidebar unable to display or sync.
 *
 * @param {string} str    — The string to write
 * @param {number} maxLen — Character limit (default 49 000, well under 50 000)
 * @returns {string}
 * @private
 */
function _safeCellValue(str, maxLen) {
  maxLen = maxLen || 49000;
  if (!str || str.length <= maxLen) return str || '';
  const note = '\n…[TRUNCATED — ' + str.length + ' chars total, showing first ' + maxLen + ']';
  return str.slice(0, maxLen - note.length) + note;
}


/**
 * Serialises a results/errors array to JSON, fitting as many complete
 * records as possible within maxLen characters.
 *
 * Unlike _safeCellValue (which truncates mid-character and can produce
 * invalid JSON), this always returns a syntactically valid JSON array.
 * If records are omitted a trailing sentinel object is appended:
 *   {"_truncated":true,"omitted":N}
 * so the sidebar (and syncPendingJobs) can detect and surface the gap.
 *
 * @param {Array}  arr    — Array of result/error objects
 * @param {number} maxLen — Character limit (default 48 000)
 * @returns {string} Always a valid JSON array string
 * @private
 */
function _safeCellArray(arr, maxLen) {
  maxLen = maxLen || 48000;
  if (!Array.isArray(arr) || !arr.length) return '[]';

  const full = JSON.stringify(arr);
  if (full.length <= maxLen) return full;

  // Reserve 80 chars for the trailing truncation sentinel so we
  // always have room to close the array with valid JSON.
  const budget = maxLen - 80;
  let out   = '[';
  let count = 0;

  for (let i = 0; i < arr.length; i++) {
    const item = JSON.stringify(arr[i]);
    const sep  = count > 0 ? ',' : '';
    if (out.length + sep.length + item.length >= budget) break;
    out += sep + item;
    count++;
  }

  const omitted = arr.length - count;
  const sep     = count > 0 ? ',' : '';
  out += sep + '{"_truncated":true,"omitted":' + omitted + '}]';
  return out;
}


/** Capitalise first letter, strip trailing period. @private */
function _cap(s) {
  if (!s) return '';
  const t = String(s).trim().replace(/\.$/, '');
  return t.charAt(0).toUpperCase() + t.slice(1);
}


// ── Auto-Purge ────────────────────────────────────────────────

/** Deletes _Debug Log rows older than 30 days. @private */
function _purgeOldDebugEntries(sheet) {
  try {
    const cutoff = new Date();
    cutoff.setDate(cutoff.getDate() - 30);
    const rows     = sheet.getDataRange().getValues();
    const toDelete = [];
    for (let i = rows.length - 1; i >= 1; i--) {
      const v = rows[i][1];
      const d = v instanceof Date ? v : new Date(v);
      if (!isNaN(d) && d < cutoff) toDelete.push(i + 1);
    }
    toDelete.forEach(r => sheet.deleteRow(r));
  } catch (e) {
    console.warn('_purgeOldDebugEntries skipped: ' + e.message);
  }
}


// ── Sheet Setup ───────────────────────────────────────────────

/**
 * Creates or resets all three log sheets with AppFolio styling.
 * Safe to run manually at any time to rebuild tabs.
 *
 * @returns {Sheet} The API Log sheet
 */
function setupLogTab() {
  const ss = SpreadsheetApp.getActive();

  // ── API Log ───────────────────────────────────────────────
  let log = ss.getSheetByName('API Log');
  if (!log) log = ss.insertSheet('API Log');
  log.clear();

  const hdrs = ['Time','User','Environment','Action','Object','Records','Status','Summary'];
  log.getRange(1, 1, 1, hdrs.length)
    .setValues([hdrs])
    .setBackground(AF.sky75).setFontColor(AF.white)
    .setFontWeight('bold').setFontSize(10)
    .setFontFamily('Helvetica Neue').setHorizontalAlignment('left');

  log.setFrozenRows(1);
  log.setRowHeight(1, 30);
  [140,195,85,65,85,60,120,440].forEach((w,i) => log.setColumnWidth(i+1, w));
  log.getRange('H:H').setWrap(true).setFontFamily('Helvetica Neue').setFontSize(10);
  log.getRange('G:G').setHorizontalAlignment('center');
  log.getRange('F:F').setHorizontalAlignment('right');
  log.setTabColor(AF.sky75);

  // ── _Log Data ─────────────────────────────────────────────
  let data = ss.getSheetByName('_Log Data');
  if (!data) data = ss.insertSheet('_Log Data');
  data.clear();

  const dataHdrs = ['Log ID','Timestamp','Object','Action','Payload (JSON)','Response (Raw)','Results (JSON)','Errors (JSON)'];
  data.getRange(1, 1, 1, dataHdrs.length)
    .setValues([dataHdrs])
    .setBackground(AF.street90).setFontColor(AF.white)
    .setFontWeight('bold').setFontSize(9).setFontFamily('Courier New');

  data.setFrozenRows(1);
  [240,160,100,70,350,350,350,350].forEach((w,i) => data.setColumnWidth(i+1, w));
  data.getRange('E:H').setWrap(false).setFontFamily('Courier New').setFontSize(8);
  data.setTabColor(AF.street90);
  data.hideSheet();

  // ── _Debug Log ────────────────────────────────────────────
  let dbg = ss.getSheetByName('_Debug Log');
  if (!dbg) dbg = ss.insertSheet('_Debug Log');
  dbg.clear();

  const dbgHdrs = ['Log ID','Time','Object','HTTP Status','Payload (JSON)','Response (Raw)'];
  dbg.getRange(1, 1, 1, dbgHdrs.length)
    .setValues([dbgHdrs])
    .setBackground(AF.street90).setFontColor(AF.white)
    .setFontWeight('bold').setFontSize(9).setFontFamily('Courier New');

  dbg.setFrozenRows(1);
  [240,155,95,75,420,420].forEach((w,i) => dbg.setColumnWidth(i+1, w));
  dbg.getRange('E:F').setWrap(false).setFontFamily('Courier New').setFontSize(8);
  dbg.setTabColor(AF.street90);

  return log;
}
