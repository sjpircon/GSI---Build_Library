// ============================================================
// PROPERTIES.GS — Prep & Load for Properties and Owner Groups
//
// Owner Group logic lives here (not in a separate file) because
// it reads and writes to the Properties sheet directly.
//
//
// FUNCTIONS
// ─────────
//   prepProperties()       — validates rows, maps bank IDs via Notes
//   executePropertyLoad()  — builds payload, calls API
//   prepOwnerGroups()      — maps owner IDs via Notes on Properties sheet
//   executeOwnerGroupLoad()— POSTs owner group for each ready property row
// ============================================================


// ── Helper: Month name/number → integer 1–12 ─────────────────

/**
 * Converts a month name or number to the integer AppFolio requires
 * for FiscalYearEnd. Defaults to 12 (December) for blank/invalid input.
 *
 * @param {*} monthInput - Number, numeric string, or month name
 * @returns {number} Integer 1–12
 */
function monthToNumber(monthInput) {
  if (monthInput === null || monthInput === undefined || monthInput === '') return 12;

  const num = parseInt(monthInput);
  if (!isNaN(num) && num >= 1 && num <= 12) return num;

  const months = {
    'jan': 1,'feb': 2,'mar': 3,'apr': 4,'may': 5,'jun': 6,
    'jul': 7,'aug': 8,'sep': 9,'oct': 10,'nov': 11,'dec': 12
  };
  const clean = String(monthInput).toLowerCase().trim().substring(0, 3);
  return months[clean] || 12;
}


// ── Prep Properties ───────────────────────────────────────────

/**
 * Validates all non-Success rows on the Properties sheet.
 *
 * Checks required fields, date formats, pet policy enums,
 * and bank name lookups (reads API_IDs from Banks sheet and
 * stores them as cell Notes for use at load time).
 */
function prepProperties() {
  const ss        = SpreadsheetApp.getActive();
  const propSheet = ss.getSheetByName('Properties');
  const bankSheet = ss.getSheetByName('Banks');
  if (!propSheet) return;

  const data    = propSheet.getDataRange().getValues();
  const headers = data[0].map(h => String(h).trim());
  const h       = {};
  headers.forEach((title, i) => { h[title] = i; });

  // Build bank name → API_ID lookup from the Banks sheet
  const bankMap = {};
  if (bankSheet) {
    const bData    = bankSheet.getDataRange().getValues();
    const bHeaders = bData[0];
    const bNameIdx = bHeaders.indexOf('AccountName');
    const bApiIdx  = bHeaders.indexOf('API_ID');

    bData.forEach((r, j) => {
      if (j === 0) return;
      const name = String(r[bNameIdx]).trim();
      const id   = r[bApiIdx];
      if (id && name) bankMap[name] = id;
    });
  }

  // ── Build GL account map: GL Number → UUID ────────────────
  // Same pattern as prepRecurringCharges() in tenants.gs.
  // UUIDs are stored as Notes on the "Number" column cells
  // by syncGLAccounts() in gl_accounts.gs.
  const glAccountMap = {};
  const glSheet = ss.getSheetByName('GL Accounts');
  if (glSheet) {
    const glData    = glSheet.getDataRange().getValues();
    const glHeaders = glData[0].map(hdr => String(hdr).trim());
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

  // ── Discover Additional Cash Account columns ──────────────
  // Any column whose header starts with a GL number pattern
  // ("1010: Operating Cash", "1020: Reserve Cash", etc.) is treated
  // as an Additional Cash Account column.
  //
  // Design:
  //   Header cell Note  = GL UUID  (written once per prep run, below)
  //   Data cell value   = bank account name
  //   Data cell Note    = bank UUID (written per-row in the data loop)
  //
  const adCashColIdxs = [];
  headers.forEach((hdr, idx) => {
    if (/^\d+\s*:/.test(hdr)) adCashColIdxs.push(idx);
  });

  // Write GL UUID to each Additional Cash column's header cell Note (row 1).
  // Done once before the data loop — the UUID is the same for every property row.
  adCashColIdxs.forEach(colIdx => {
    const hdr      = headers[colIdx];
    const glNumber = hdr.split(':')[0].trim();   // "1010: Operating Cash" → "1010"
    const glId     = glAccountMap[glNumber];
    const hdrCell  = propSheet.getRange(1, colIdx + 1);
    if (glId) {
      hdrCell.setNote(glId);
    } else {
      hdrCell.setNote('GL Number "' + glNumber + '" not found in GL Accounts tab — run Sync GL Accounts first');
    }
  });

  const timestamp = new Date().getTime();

  for (let i = 1; i < data.length; i++) {
    const rowNum = i + 1;
    if (data[i][h[CONFIG.STATUS_COL]] === 'Success') continue;

    const rowErrors = [];

    // Auto-generate ReferenceId
    propSheet.getRange(rowNum, h[CONFIG.REF_ID_COL] + 1).setValue(`Prop_${timestamp}_${i}`);

    // ── Required field check ─────────────────────────────────
    CONFIG.REQUIRED_PROPERTIES.forEach(reqField => {
      if (reqField === 'ReferenceId') return;  // just auto-generated above
      if (h[reqField] === undefined || String(data[i][h[reqField]]).trim() === '') {
        rowErrors.push(`${reqField} is required`);
      }
    });

    // ── ManagementStartDate format ───────────────────────────
    const startDate = data[i][h['ManagementStartDate']];
    if (startDate && !(startDate instanceof Date) && isNaN(Date.parse(startDate))) {
      rowErrors.push('ManagementStartDate must be a valid date');
    }

    // ── Pet policy enums ─────────────────────────────────────
    const cats = String(data[i][h['CatsAllowed']] || '').trim();
    const dogs = String(data[i][h['DogsAllowed']] || '').trim();
    if (cats && !['Yes','No'].includes(cats)) {
      rowErrors.push("CatsAllowed must be 'Yes' or 'No'");
    }
    if (dogs && !['Large & Small','Small Only','No'].includes(dogs)) {
      rowErrors.push("DogsAllowed must be 'Large & Small', 'Small Only', or 'No'");
    }

    // ── Bank name → UUID lookup ──────────────────────────────
    // Stores the UUID as a Note so executePropertyLoad() can read
    // it without re-scanning the Banks sheet row by row.
    const bankFields = ['OperatingCashBankAccountId','EscrowCashBankAccountId','ExpensesCashBankAccountId'];
    bankFields.forEach(field => {
      if (h[field] === undefined) return;
      const bankName = String(data[i][h[field]]).trim();
      const cell     = propSheet.getRange(rowNum, h[field] + 1);

      if (bankName === '') {
        if (field !== 'ExpensesCashBankAccountId') rowErrors.push(`${field} is missing`);
      } else if (bankMap[bankName]) {
        cell.setNote(bankMap[bankName]).setBackground(null);
      } else {
        cell.setBackground('#f4cccc').setNote('Bank Name not found in Banks tab');
        rowErrors.push(`${field} '${bankName}' not found in Banks sheet`);
      }
    });

    // ── Additional Cash Account columns: resolve bank UUID per row ─
    // Header Note = GL UUID (already written above for all columns).
    // Data cell value = bank account name; data cell Note = bank UUID.
    // Blank cell → skip this GL/bank pair for this property (no error).
    adCashColIdxs.forEach(colIdx => {
      const bankName = String(data[i][colIdx] || '').trim();
      if (!bankName) return;   // blank = this property doesn't use this cash account

      const dataCell = propSheet.getRange(rowNum, colIdx + 1);
      const bankId   = bankMap[bankName];
      if (bankId) {
        dataCell.setNote(bankId).setBackground(null);
      } else {
        dataCell.setBackground('#f4cccc')
          .setNote('Bank name not found in Banks tab');
        rowErrors.push('"' + bankName + '" in column "' + headers[colIdx] + '" not found in Banks sheet');
      }
    });

    // ── Write status ─────────────────────────────────────────
    const statusCell = propSheet.getRange(rowNum, h[CONFIG.STATUS_COL] + 1);
    if (rowErrors.length > 0) {
      statusCell.setValue('Error:\n• ' + rowErrors.join('\n• ')).setBackground('#f4cccc');
    } else {
      statusCell.setValue('Ready').setBackground('#cfe2f3');
    }
  }

  applyConditionalRules(propSheet, headers, h[CONFIG.STATUS_COL]);
  ss.toast('Properties Validation Complete.', 'Properties Prep');
}


// ── Execute Property Load ─────────────────────────────────────

/**
 * Sends all "Ready" rows to the AppFolio bulk properties endpoint.
 *
 * Bank Account IDs are read from cell Notes (set during prep).
 * Management fee, lease fee, and site manager fields are assembled
 * into nested objects matching the API spec.
 *
 */
function executePropertyLoad() {
  const ss    = SpreadsheetApp.getActive();
  const sheet = ss.getSheetByName('Properties');
  if (!sheet) return;

  const data    = sheet.getDataRange().getValues();
  const headers = data[0].map(h => String(h).trim());
  const h       = {};
  headers.forEach((title, i) => { h[title] = i; });

  const now             = new Date();
  const defaultStart    = Utilities.formatDate(
    new Date(now.getFullYear(), now.getMonth(), 1), 'GMT', 'yyyy-MM-dd'
  );

  // ── Discover Additional Cash Account columns (once, before row loop) ─
  // Same GL number header pattern as prepProperties() ("1010: Operating Cash", etc.).
  // GL UUID = header cell Note (row 1, written during prep — same for all rows).
  // Bank UUID = data cell Note (written per-row during prep).
  const adCashColIdxs = [];
  headers.forEach((hdr, idx) => {
    if (/^\d+\s*:/.test(hdr)) adCashColIdxs.push(idx);
  });

  // Pre-read GL UUIDs from header Notes — avoids a getNote() call per row per column.
  const adCashGlIds = {};
  adCashColIdxs.forEach(colIdx => {
    adCashGlIds[colIdx] = sheet.getRange(1, colIdx + 1).getNote();
  });

  const payloadArray = [];
  const rowMapping   = [];

  for (let i = 1; i < data.length; i++) {
    if (data[i][h[CONFIG.STATUS_COL]] !== 'Ready') continue;

    const record      = {};
    const mgmtObj     = {};
    const leaseObj    = {};
    const siteManager = {};

    headers.forEach((title, idx) => {
      let val = data[i][idx];

      // Additional Cash Account columns — handled separately via adCashColIdxs
      if (adCashColIdxs.includes(idx)) return;

      // Bank fields — read UUID from Note set during prep
      if (['OperatingCashBankAccountId','EscrowCashBankAccountId','ExpensesCashBankAccountId'].includes(title)) {
        const bankId = sheet.getRange(i + 1, idx + 1).getNote();
        if (bankId) record[title] = bankId;
        return;
      }

      if (val === '' || val === null || val === undefined) return;

      // Management fee fields — MgmtFee: Type, MgmtFee: Value, etc.
      if (title.startsWith('MgmtFee:')) {
        const key    = title.split(':')[1].trim();
        const apiKey = key === 'Type' ? 'FeeType' : key;
        mgmtObj[apiKey] = castPropertyType(title, val);
        return;
      }

      // Lease fee fields
      if (title.startsWith('LeaseFee:')) {
        const key = title.split(':')[1].trim();
        if (key === 'Type') {
          leaseObj.FeeType = val;
        } else if (key === 'Value') {
          if (leaseObj.FeeType === 'Percent') {
            leaseObj.Percentage = castPropertyType(title, val);
          } else {
            leaseObj.FlatAmount = castPropertyType(title, val);
          }
        }
        return;
      }

      // Site manager fields
      if (title.startsWith('SiteManager:')) {
        siteManager[title.split(':')[1].trim()] = val;
        return;
      }

      // Standard allowlist fields
      if (CONFIG.ALLOWED_PROPERTIES.includes(title)) {
        record[title] = castPropertyType(title, val);
      }
    });

    // ── Build AdditionalCashAccounts array ────────────────────
    // GL UUID = adCashGlIds[colIdx] (pre-read from header Note — same for all rows).
    // Bank UUID = data cell Note (read per-row from the cell set during prep).
    // Blank cell value → skip this GL/bank pair for this property.
    const additionalCashAccounts = [];
    adCashColIdxs.forEach(colIdx => {
      const bankName = String(data[i][colIdx] || '').trim();
      if (!bankName) return;   // blank = skip for this property
      const glId   = adCashGlIds[colIdx];
      const bankId = sheet.getRange(i + 1, colIdx + 1).getNote();
      if (glId && bankId) {
        additionalCashAccounts.push({ GlAccountId: glId, BankAccountId: bankId });
      }
    });
    if (additionalCashAccounts.length > 0) {
      record.AdditionalCashAccounts = additionalCashAccounts;
    }

    // ── Assemble management fee ──────────────────────────────
    if (Object.keys(mgmtObj).length > 0) {
      if (mgmtObj.FeeType === 'Percent') {
        if (mgmtObj.Value) mgmtObj.Percentage = castPropertyType('Percentage', mgmtObj.Value);
        mgmtObj.Minimum = mgmtObj.Min || mgmtObj.Minimum || 0;
      } else {
        if (mgmtObj.Value) mgmtObj.FlatAmount = castPropertyType('FlatAmount', mgmtObj.Value);
      }
      delete mgmtObj.Value;
      delete mgmtObj.Min;
      mgmtObj.WaiveWhenVacant = mgmtObj.Waive !== undefined ? mgmtObj.Waive : false;
      delete mgmtObj.Waive;
      mgmtObj.StartDate = mgmtObj.StartDate || defaultStart;
      record.CurrentManagementFeePolicy = mgmtObj;
    }

    if (Object.keys(leaseObj).length  > 0) record.LeaseFeePolicy = leaseObj;
    if (Object.keys(siteManager).length > 0) record.SiteManager  = siteManager;

    payloadArray.push(record);
    rowMapping.push(i + 1);
  }

  if (!payloadArray.length) {
    ss.toast('No "Ready" rows found. Run Prep first.', 'Properties Load');
    return;
  }

  // 'Properties' → audit log object column + direct ID lookup (PropertyId)
  const result = callAppFolioAPI(CONFIG.ENDPOINTS.PROPERTIES, payloadArray, 'Properties');

  rowMapping.forEach((rowNum, index) => {
    const statusRange = sheet.getRange(rowNum, h[CONFIG.STATUS_COL] + 1);
    const idRange     = sheet.getRange(rowNum, h[CONFIG.API_ID_COL] + 1);

    if (result.success && result.data && result.data[index]) {
      const item = result.data[index];
      if (item.PropertyId || item.successful !== false) {
        idRange.setValue(item.PropertyId || 'Success');
        statusRange.setValue('Success').setBackground('#b6d7a8');
      } else {
        const errMsg = item.errors ? item.errors.map(e => e.message).join(', ') : 'Row Error';
        statusRange.setValue('Error: ' + errMsg).setBackground('#f4cccc');
      }
    } else {
      statusRange.setValue('Error: Batch Failed').setBackground('#f4cccc');
    }
  });

  ss.toast('Property Load Complete.', 'Properties Load');
}


// ── Prep Owner Groups ─────────────────────────────────────────

/**
 * Maps owner names from the Owners sheet to property rows via
 * cell Notes. Validates percent ownership totals sum to 100.
 *
 * Stores each matched owner's API_ID as a Note on the owner
 * name cell so executeOwnerGroupLoad() can build the payload
 * without re-scanning the Owners sheet.
 */
function prepOwnerGroups() {
  const ss        = SpreadsheetApp.getActive();
  const propSheet = ss.getSheetByName('Properties');
  const ownerSheet = ss.getSheetByName('Owners');
  if (!propSheet || !ownerSheet) {
    ss.toast('Properties or Owners sheet missing.', 'Error');
    return;
  }

  // Build owner fingerprint → API_ID lookup
  const ownerData = ownerSheet.getDataRange().getValues();
  const oHeaders  = ownerData[0].map(h => String(h).trim());
  const oIdx      = {
    api: oHeaders.indexOf('API_ID'),
    fn:  oHeaders.indexOf('FirstName'),
    ln:  oHeaders.indexOf('LastName'),
    co:  oHeaders.indexOf('CompanyName')
  };

  const ownerMap = {};
  for (let j = 1; j < ownerData.length; j++) {
    const apiId = ownerData[j][oIdx.api];
    if (!apiId) continue;
    const fp = (
      String(ownerData[j][oIdx.fn] || '') +
      String(ownerData[j][oIdx.ln] || '') +
      String(ownerData[j][oIdx.co] || '')
    ).toLowerCase().replace(/\s+/g, '');
    if (fp) ownerMap[fp] = apiId;
  }

  // Process Properties sheet
  const propData    = propSheet.getDataRange().getValues();
  const propHeaders = propData[0].map(h => String(h).trim());

  const idx = {
    status:  propHeaders.indexOf('Owner_Group_Status'),
    propApi: propHeaders.indexOf('API_ID'),
  };

  for (let i = 1; i < propData.length; i++) {
    const rowNum = i + 1;
    if (propData[i][idx.status] === 'Success') continue;

    const matchErrors  = [];
    let   totalPercent = 0;

    if (!propData[i][idx.propApi]) matchErrors.push('Property API_ID missing');

    // Map owner slots 1–30
    for (let slot = 1; slot <= 30; slot++) {
      const fIdx = propHeaders.indexOf(`Owner First Name #${slot}`);
      const lIdx = propHeaders.indexOf(`Owner Last Name #${slot}`);
      const cIdx = propHeaders.indexOf(`Owner Company Name #${slot}`);
      const pIdx = propHeaders.indexOf(`Owner Percent #${slot}`);

      if (fIdx === -1 && cIdx === -1) continue;

      const fp = (
        String(propData[i][fIdx] || '') +
        String(propData[i][lIdx] || '') +
        String(propData[i][cIdx] || '')
      ).toLowerCase().replace(/\s+/g, '');

      if (fp) {
        if (pIdx !== -1) totalPercent += parseFloat(propData[i][pIdx]) || 0;

        const anchorIdx  = cIdx !== -1 ? cIdx : fIdx;
        const matchedId  = ownerMap[fp];

        if (matchedId) {
          propSheet.getRange(rowNum, anchorIdx + 1).setNote(matchedId).setBackground(null);
        } else {
          propSheet.getRange(rowNum, anchorIdx + 1).setBackground('#f4cccc');
          matchErrors.push(`Owner #${slot} not found in Owners tab`);
        }
      }
    }

    if (totalPercent > 0 && Math.round(totalPercent) !== 100) {
      matchErrors.push(`Total ownership % is ${totalPercent} — must equal 100`);
    }

    const statusCell = propSheet.getRange(rowNum, idx.status + 1);
    if (matchErrors.length > 0) {
      statusCell.setValue('Errors:\n• ' + matchErrors.join('\n• '))
        .setBackground('#f4cccc')
        .setWrapStrategy(SpreadsheetApp.WrapStrategy.WRAP);
    } else {
      statusCell.setValue('Ready for Group Load').setBackground('#cfe2f3');
    }
  }

  ss.toast('Owner Group Prep Complete.', 'Owner Groups Prep');
}


// ── Execute Owner Group Load ──────────────────────────────────

/**
 * POSTs one owner group per "Ready for Group Load" property row.
 * Owner IDs are read from the cell Notes set by prepOwnerGroups().
 *
 */
// ── Execute Owner Group Load ──────────────────────────────────

/**
 * Collects all "Ready for Group Load" property rows into a single
 * payload array, then dispatches via batchWithRetry() — matching
 * the pattern used by executeOwnerLoad(), executeVendorLoad(), etc.
 *
 * Owner IDs are read from cell Notes set by prepOwnerGroups().
 */
// ── Execute Owner Group Load ──────────────────────────────────

/**
 * POSTs one owner group per "Ready for Group Load" property row.
 *
 * WHY one-at-a-time (not batchWithRetry):
 * /owner_groups is NOT a bulk endpoint. batchWithRetry wraps payloads
 * into { data: [...] } or sends them as an array — both are rejected.
 * Each row must be an individual POST of a single object.
 *
 * Results are accumulated across all rows and written to the audit
 * log in a SINGLE logResponse call at the end (one entry per run,
 * not one per row).
 *
 * FIXES
 * ─────
 *   Bug 1 — Fiscal Year Missing:  caused by array payload shape; fixed
 *            by posting single objects to the non-bulk endpoint.
 *   Bug 2 — Incorrect Error msg:  _extractOwnerGroupError() returns
 *            only the human-readable message, not the full JSON dump.
 *   Bug 3 — Missing in Audit tab: synthetic ReferenceId stamped on each
 *            response + single logResponse call makes successes and
 *            failures surface correctly in the sidebar.
 */
function executeOwnerGroupLoad() {
  const ss        = SpreadsheetApp.getActive();
  const propSheet = ss.getSheetByName('Properties');
  if (!propSheet) return;

  const data    = propSheet.getDataRange().getValues();
  const headers = data[0].map(h => String(h).trim().toLowerCase());
  const h       = {};
  headers.forEach((title, i) => { h[title.replace(/\s+/g, '')] = i; });

  const now          = new Date();
  const defaultStart = Utilities.formatDate(
    new Date(now.getFullYear(), now.getMonth(), 1), 'GMT', 'yyyy-MM-dd'
  );

  const allPayloads = [];
  const allRowNums  = [];

  // ── 1. Collect all Ready rows ─────────────────────────────
  for (let i = 1; i < data.length; i++) {
    const rowNum = i + 1;
    if (data[i][h['owner_group_status']] !== 'Ready for Group Load') continue;

    const propertyId = data[i][h['api_id']];
    if (!propertyId) continue;

    const rowFiscal = monthToNumber(data[i][h['fiscalyearend']]);

    let rowStart = defaultStart;
    if (h['startdate'] !== undefined && data[i][h['startdate']]) {
      try {
        rowStart = Utilities.formatDate(new Date(data[i][h['startdate']]), 'GMT', 'yyyy-MM-dd');
      } catch (_) {}
    }

    const rawPay  = String(data[i][h['paymenttype']] || '').toLowerCase();
    const payType = rawPay.includes('flat') ? 'Flat' : 'Net Income';

    const payload = {
      PropertyId:       String(propertyId),
      StartDate:        rowStart,
      FiscalYearEnd:    rowFiscal,
      ReserveFunds:     parseFloat(data[i][h['reservefunds']] || 0),
      PaymentType:      payType,
      OwnerPacketBasis: String(data[i][h['ownerpacketbasis']] || 'Cash').trim(),
      Owners:           []
    };

    if (payType === 'Flat') {
      payload.PaymentAmount = parseFloat(data[i][h['paymentamount']] || 0);
    }

    for (let slot = 1; slot <= 30; slot++) {
      const cIdx      = headers.indexOf(`owner company name #${slot}`);
      const fIdx      = headers.indexOf(`owner first name #${slot}`);
      const pIdx      = headers.indexOf(`owner percent #${slot}`);
      const anchorIdx = cIdx !== -1 ? cIdx : fIdx;
      if (anchorIdx !== -1) {
        const ownerId = propSheet.getRange(rowNum, anchorIdx + 1).getNote();
        if (ownerId) payload.Owners.push({
          PercentOwned: String(data[i][pIdx] || '0'),
          OwnerId:      ownerId
        });
      }
    }

    allPayloads.push(payload);
    allRowNums.push(rowNum);
  }

  if (!allPayloads.length) {
    ss.toast('No "Ready for Group Load" rows found. Run Prep first.', 'Owner Groups Load');
    return;
  }

  // ── 2. Post one-at-a-time, accumulate raw results ─────────
  // Each parsed response gets a synthetic ReferenceId stamped on it
  // so _extractResults / _extractBulkData can link it to a record
  // in the audit sidebar.
  const endpoint   = CONFIG.ENDPOINTS.OWNER_GROUPS;
  const apiHdrs    = getApiHeaders();
  const rawResults = [];   // accumulate per-row parsed responses
  const rowErrors  = [];   // for logResponse rowErrors array
  let   successCt  = 0;

  for (let i = 0; i < allPayloads.length; i++) {
    const payload    = allPayloads[i];
    const rowNum     = allRowNums[i];
    const statusCell = propSheet.getRange(rowNum, h['owner_group_status'] + 1);
    const refId      = `OG_${payload.PropertyId}`;  // synthetic per-row reference

    let respText = '', statusCode = 0, parsed = {};

    // Per-row retry loop (mirrors callAppFolioAPI retry logic)
    for (let attempt = 1; attempt <= CONFIG.MAX_RETRIES; attempt++) {
      try {
        const resp = UrlFetchApp.fetch(endpoint, {
          method:             'post',
          contentType:        'application/json',
          headers:            apiHdrs,
          payload:            JSON.stringify(payload),  // single object, NOT wrapped in {data:[]}
          muteHttpExceptions: true
        });
        respText   = resp.getContentText();
        statusCode = resp.getResponseCode();
        try { parsed = JSON.parse(respText); } catch (_) { parsed = { message: respText }; }

        if (statusCode === 429 && attempt < CONFIG.MAX_RETRIES) {
          Utilities.sleep(CONFIG.RATE_LIMIT_PAUSE_MS + attempt * CONFIG.RETRY_BASE_DELAY_MS);
          continue;
        }
        if (statusCode >= 500 && attempt < CONFIG.MAX_RETRIES) {
          Utilities.sleep(CONFIG.RETRY_BASE_DELAY_MS * Math.pow(2, attempt - 1));
          continue;
        }
        break;
      } catch (netErr) {
        if (attempt < CONFIG.MAX_RETRIES) {
          Utilities.sleep(CONFIG.RETRY_BASE_DELAY_MS * Math.pow(2, attempt - 1));
          continue;
        }
        respText = netErr.message; statusCode = 500; parsed = { message: respText };
      }
    }

    const isOk = statusCode >= 200 && statusCode < 300 && !parsed.errors && parsed.successful !== false;

    // Stamp synthetic ReferenceId so _extractResults can link this record
    // in the audit sidebar (AppFolio doesn't echo it back for this endpoint).
    // Mark failures explicitly so _normaliseErrors / _extractResults surface
    // them in the "Need Attention" block of the Audit tab.
    if (!isOk) parsed.successful = false;
    parsed.ReferenceId = refId;
    rawResults.push(parsed);

    if (isOk) {
      successCt++;
      statusCell.setValue('Success').setBackground('#b6d7a8');
    } else {
      const errMsg = _extractOwnerGroupError(parsed);
      statusCell.setValue('Error: ' + errMsg).setBackground('#f4cccc');
      rowErrors.push({ ref: refId, field: '', message: errMsg });
    }

    if (i % 10 === 0) SpreadsheetApp.flush();
    Utilities.sleep(200);  // brief pause — be a good API citizen
  }

  // ── 3. Single audit log entry for the entire script run ───
  // Passing responseJson: { data: rawResults } lets _extractBulkData
  // return the array and _extractResults process each item individually,
  // surfacing successes (returnedId = parsed.Id) and failures (errors)
  // correctly in the sidebar.
  const overallCode = rowErrors.length === 0 ? 200 : (successCt > 0 ? 207 : 400);
  logResponse({
    action:       'Load',
    object:       'Owner Groups',
    recordCount:  allPayloads.length,
    request:      { data: allPayloads },
    responseText: `${successCt} of ${allPayloads.length} owner groups succeeded`,
    responseJson: { data: rawResults },
    statusCode:   overallCode,
    rowErrors:    rowErrors
  });

  ss.toast('Owner Group Load Complete.', 'Owner Groups Load');
}


/**
 * Extracts a concise, human-readable error message from an AppFolio
 * /owner_groups failure response.
 *
 * Handles the common shapes:
 *   { errors: [{ attribute: 'FiscalYearEnd', message: '...' }] }
 *   { message: '...' }
 *   { error: '...' }
 *
 * Caps at 3 validation errors to keep the status cell readable.
 *
 * @param {Object} parsed — Parsed JSON response from AppFolio
 * @returns {string}
 * @private
 */
function _extractOwnerGroupError(parsed) {
  if (!parsed || typeof parsed !== 'object') return 'Unknown error';

  // AppFolio validation errors array
  if (Array.isArray(parsed.errors) && parsed.errors.length) {
    return parsed.errors.slice(0, 3).map(e => {
      const field = e.attribute || e.field || '';
      const msg   = e.message   || String(e);
      return field ? `${field}: ${msg}` : msg;
    }).join('; ');
  }

  if (typeof parsed.message === 'string') return parsed.message.substring(0, 150);
  if (typeof parsed.error   === 'string') return parsed.error.substring(0, 150);
  return 'Check API Log for details';
}
