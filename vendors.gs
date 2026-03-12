// ============================================================
// VENDORS.GS — Prep, Load, and Sync for Vendor bulk creation
//
//
// FUNCTIONS
// ─────────
//   prepVendors()          — validates rows, stamps ReferenceIds
//   buildVendorPayload()   — constructs per-row API payload
//   executeVendorLoad()    — batches Ready rows, calls API
//   syncVendorJobStatuses()— polls jobs endpoint, writes AF Vendor IDs
// ============================================================


// ── Prep ─────────────────────────────────────────────────────

/**
 * Validates all non-complete rows on the Vendors sheet.
 * Identity logic: CompanyName OR (FirstName + LastName) required.
 * Address logic: if any address field present, all four required.
 */
function prepVendors() {
  const ss    = SpreadsheetApp.getActive();
  const sheet = ss.getSheetByName('Vendors');
  if (!sheet) return;

  const data    = sheet.getDataRange().getValues();
  const headers = data[0].map(h => String(h).trim());
  const h       = {};
  headers.forEach((title, i) => { h[title] = i; });

  const timestamp = new Date().getTime();

  for (let i = 1; i < data.length; i++) {
    const row    = data[i];
    const rowNum = i + 1;
    const status = String(row[h[CONFIG.STATUS_COL]] || '');

    // Skip already-processed rows
    if (status === 'Success' || status.includes('Pending')) continue;

    const errors = [];

    // ── Identity check ────────────────────────────────────────
    const companyName = String(row[h['CompanyName']] || '').trim();
    const firstName   = String(row[h['FirstName']]   || '').trim();
    const lastName    = String(row[h['LastName']]     || '').trim();

    if (!companyName && (!firstName || !lastName)) {
      errors.push('Individual vendors require both First and Last Name if CompanyName is blank');
    }

    // ── Address co-dependency ─────────────────────────────────
    const hasAnyAddr = ['Address1','City','State','Zip']
      .some(f => String(row[h[f]] || '').trim() !== '');
    if (hasAnyAddr) {
      if (!row[h['Address1']] || !row[h['City']] || !row[h['State']] || !row[h['Zip']]) {
        errors.push('Full address (Address1, City, State, Zip) required if any part is provided');
      }
    }

    // ── State format ──────────────────────────────────────────
    const stateVal = String(row[h['State']] || '').trim();
    if (stateVal && stateVal.length !== 2) errors.push('State must be 2-letter code');

    // ── Write status & ReferenceId ────────────────────────────
    const statusCell = sheet.getRange(rowNum, h[CONFIG.STATUS_COL] + 1);
    const refCell    = sheet.getRange(rowNum, h[CONFIG.REF_ID_COL] + 1);

    if (errors.length > 0) {
      statusCell.setValue('Error:\n• ' + errors.join('\n• ')).setBackground('#f4cccc');
      refCell.setValue('');
    } else {
      refCell.setValue(`Vnd_${timestamp}_${i}`);
      statusCell.setValue('Ready').setBackground('#cfe2f3');
    }
  }

  applyConditionalRules(sheet, headers, h[CONFIG.STATUS_COL]);
  ss.toast('Vendor Prep Complete.', 'Vendors Prep');
}


// ── Payload Builder ───────────────────────────────────────────

/**
 * Constructs a single vendor API payload from one sheet row.
 *
 * Handles:
 *   • Company vs. individual identity detection
 *   • Smart TaxpayerName / UseCompanyNameAsTaxpayerName logic
 *   • Nested PhoneNumbers and Emails arrays
 *   • CountryCode defaulting when Address1 is present
 *
 * @param {Array} rowData - Single data row from sheet
 * @param {Object} h      - Header → column index map
 * @returns {Object}      - API-ready vendor payload
 */
function buildVendorPayload(rowData, h) {
  const vendor = {};

  const rawCompany    = String(rowData[h['CompanyName']]   || '').trim();
  const rawFirst      = String(rowData[h['FirstName']]     || '').trim();
  const rawLast       = String(rowData[h['LastName']]      || '').trim();
  const rawTaxpayer   = String(rowData[h['TaxpayerName']]  || '').trim();

  // ── 1. Determine identity type ───────────────────────────
  vendor.IsCompany = rawCompany !== '';

  // ── 2. TaxpayerName logic ────────────────────────────────
  // Priority: explicit TaxpayerName > CompanyName toggle > First+Last fallback
  if (rawTaxpayer !== '') {
    if (vendor.IsCompany && rawTaxpayer === rawCompany) {
      // Exact match to company name — use the toggle instead of repeating the value
      vendor.UseCompanyNameAsTaxpayerName = true;
    } else {
      vendor.UseCompanyNameAsTaxpayerName = false;
      vendor.TaxpayerName                 = rawTaxpayer;
    }
  } else {
    if (vendor.IsCompany) {
      vendor.UseCompanyNameAsTaxpayerName = true;
    } else {
      vendor.UseCompanyNameAsTaxpayerName = false;
      vendor.TaxpayerName                 = `${rawFirst} ${rawLast}`.trim();
    }
  }

  // ── 3. Reference ID ──────────────────────────────────────
  vendor.ReferenceId = String(rowData[h[CONFIG.REF_ID_COL]]).trim();

  // ── 4. Identity fields ───────────────────────────────────
  if (vendor.IsCompany) {
    vendor.CompanyName = rawCompany;
  } else {
    vendor.FirstName = rawFirst;
    vendor.LastName  = rawLast;
  }

  // ── 5. Dynamic fields (phones, emails, standard fields) ──
  const emails = [];
  const phones = [];

  const manualFields = [
    'CompanyName','FirstName','LastName','TaxpayerName','ReferenceId',
    CONFIG.STATUS_COL, CONFIG.API_ID_COL,
    'IsCompany','UseCompanyNameAsTaxpayerName'
  ];

  Object.keys(h).forEach(header => {
    const idx = h[header];
    const val = rowData[idx];
    if (manualFields.includes(header) || val === '' || val === null) return;

    // Phone fields — PhoneNumber1, Label1, AdditionalDetails1
    if (header.match(/^(PhoneNumber|Label|AdditionalDetails)\d+$/)) {
      const num    = header.replace(/\D/g, '');
      const key    = header.replace(/\d/g, '');
      const pIdx   = parseInt(num) - 1;
      if (!phones[pIdx]) phones[pIdx] = { IsPrimary: pIdx === 0 };
      phones[pIdx][key === 'PhoneNumber' ? 'Number' : key] = String(val);
      return;
    }

    // Email fields — EmailAddress1, EmailAddress2, etc.
    if (header.startsWith('EmailAddress')) {
      const eIdx = parseInt(header.replace(/\D/g, '')) - 1;
      emails.push({ EmailAddress: String(val), IsPrimary: eIdx === 0 });
      return;
    }

    // Standard allowlist fields
    if (CONFIG.ALLOWED_VENDORS.includes(header)) {
      vendor[header] = castPropertyType(header, val);
    }
  });

  if (emails.length > 0) vendor.Emails = emails;

  const cleanPhones = phones.filter(p => p && p.Number && String(p.Number).trim() !== '');
  if (cleanPhones.length > 0) vendor.PhoneNumbers = cleanPhones;

  // Default CountryCode when an address is present
  if (vendor.Address1 && !vendor.CountryCode) vendor.CountryCode = 'US';

  return vendor;
}


// ── Execute Load ─────────────────────────────────────────────

/**
 * Sends Ready rows to the AppFolio bulk endpoint in batches of 40.
 *
 * BATCHING: All chunks are logged as a single Audit entry (one entry
 * per script run rather than one per 40-record batch). Each record's
 * pending placeholder points to its chunk's JobId so sync still works.
 */
function executeVendorLoad() {
  const ss      = SpreadsheetApp.getActive();
  const sheet   = ss.getSheetByName('Vendors');
  const data    = sheet.getDataRange().getValues();
  const headers = data[0].map(h => String(h).trim());
  const h       = {};
  headers.forEach((title, i) => { h[title] = i; });

  const BATCH_SIZE  = 40;
  const allPayloads = [];
  const allRowNums  = [];

  // Collect all Ready rows
  for (let i = 1; i < data.length; i++) {
    if (String(data[i][h[CONFIG.STATUS_COL]]).trim() === 'Ready') {
      allPayloads.push(buildVendorPayload(data[i], h));
      allRowNums.push(i + 1);
    }
  }

  if (!allPayloads.length) {
    ss.toast('No "Ready" rows found. Run Prep first.', 'Vendors Load');
    return;
  }

  // ── Accumulate for single log entry ──────────────────────
  const allLogRecords    = [];  // full payload records for the log entry
  const allPendingResults = []; // one pending entry per vendor with correct JobId

  // Process in batches of 40 — suppress per-chunk logging
  for (let i = 0; i < allPayloads.length; i += BATCH_SIZE) {
    const chunkPayloads = allPayloads.slice(i, i + BATCH_SIZE);
    const chunkRows     = allRowNums.slice(i, i + BATCH_SIZE);

    // suppressLog = true — we log once at the end
    const response = callAppFolioAPI(CONFIG.ENDPOINTS.VENDORS, chunkPayloads, 'Vendors', 'Load', true);

    const jobId = (response.data && response.data.JobId)
      ? response.data.JobId
      : ((response.message && response.message.match(/"JobId"\s*:\s*"([^"]+)"/) || [])[1] || null);

    if ((response.success || (response.message && response.message.includes('JobId'))) && jobId) {
      chunkRows.forEach(rowNum => {
        sheet.getRange(rowNum, h[CONFIG.STATUS_COL] + 1)
          .setValue(`Pending: ${jobId}`)
          .setBackground('#fff2cc');
      });
      // Build pending result entries pointing to this chunk's JobId
      chunkPayloads.forEach(vendor => {
        allPendingResults.push({
          referenceId: String(vendor.ReferenceId || ''),
          successful:  true,
          idType:      'job',
          returnedId:  jobId
        });
      });
      allLogRecords.push(...chunkPayloads);
    } else {
      chunkRows.forEach(rowNum => {
        sheet.getRange(rowNum, h[CONFIG.STATUS_COL] + 1)
          .setValue('Error: ' + (response.message || 'Unknown error'))
          .setBackground('#f4cccc');
      });
    }

    SpreadsheetApp.flush();
    if (i + BATCH_SIZE < allPayloads.length) Utilities.sleep(300);
  }

  // ── Single audit log entry for the entire run ─────────────
  if (allLogRecords.length) {
    logResponse({
      action:          'Load',
      object:          'Vendors',
      recordCount:     allLogRecords.length,
      request:         { data: allLogRecords },
      responseText:    `Submitted ${allLogRecords.length} vendors in ${Math.ceil(allLogRecords.length / BATCH_SIZE)} batch(es)`,
      responseJson:    {},
      statusCode:      200,
      prebuiltResults: allPendingResults
    });
  }

  ss.toast('Vendor load submitted. Run Sync to confirm status.', 'Vendors Load');
}


// ── Sync Job Statuses ─────────────────────────────────────────

/**
 * Polls the AF Jobs endpoint for all "Pending: <jobId>" rows.
 * On resolution:
 *   • Writes AF Vendor ResourceId to API_ID column
 *   • Updates API_Status to "Success" or "Error: <reason>"
 *   • Calls updateLogDataAfterSync() so Log Viewer shows
 *     resolved ResourceIds instead of the placeholder JobId
 */
function syncVendorJobStatuses() {
  const ss    = SpreadsheetApp.getActive();
  const sheet = ss.getSheetByName('Vendors');
  if (!sheet) return;

  const data      = sheet.getDataRange().getValues();
  const headers   = data[0].map(h => String(h).trim());
  const statusIdx = headers.indexOf('API_Status');
  const apiIdIdx  = headers.indexOf('API_ID');
  const refIdIdx  = headers.indexOf('ReferenceId');

  // Collect pending: { jobId → [rowNumber, ...] }
  const pendingJobs = {};
  for (let i = 1; i < data.length; i++) {
    const status = String(data[i][statusIdx]);
    if (status.includes('Pending:')) {
      const jobId = status.split('Pending:')[1].trim();
      if (!pendingJobs[jobId]) pendingJobs[jobId] = [];
      pendingJobs[jobId].push(i + 1);
    }
  }

  const jobIds = Object.keys(pendingJobs);
  if (!jobIds.length) {
    ss.toast('No pending Vendor jobs found.', 'Vendor Sync');
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

      // Build referenceId → result map
      const apiMap = {};
      job.Result.forEach(item => {
        apiMap[String(item.ReferenceId).trim()] = {
          success:    item.Successful || item.successful || false,
          resourceId: item.ResourceId || null,
          error:      item.Error || item.error || item.message || ''
        };
      });

      // Write results to sheet rows
      pendingJobs[jobId].forEach(rowNum => {
        const rowRefId = String(data[rowNum - 1][refIdIdx]).trim();
        const match    = apiMap[rowRefId];
        if (!match) return;

        if (match.success && match.resourceId) {
          sheet.getRange(rowNum, apiIdIdx  + 1).setValue(match.resourceId);
          sheet.getRange(rowNum, statusIdx + 1).setValue('Success').setBackground('#b6d7a8');
        } else {
          sheet.getRange(rowNum, statusIdx + 1)
            .setValue('Error: ' + String(match.error || 'Unknown error'))
            .setBackground('#f4cccc');
        }
      });

      // Update _Log Data for Log Viewer sidebar
      const resolvedPayload = job.Result.map(item => ({
        referenceId: String(item.ReferenceId).trim(),
        resourceId:  item.ResourceId  || '',
        successful:  item.Successful  || item.successful || false,
        error:       item.Error       || item.error      || ''
      }));

      updateLogDataAfterSync(jobId, resolvedPayload);

    } catch (e) {
      console.error('syncVendorJobStatuses — error on job ' + jobId + ': ' + e.message);
    }
  });

  ss.toast('Vendor Sync Complete', 'Vendor Sync');
}
