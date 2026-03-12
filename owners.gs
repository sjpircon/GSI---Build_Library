// ============================================================
// OWNERS.GS — Prep, Load, and Sync for Owner bulk creation
//
// FUNCTIONS
// ─────────
//   prepOwners()           — validates rows, stamps ReferenceIds
//   executeOwnerLoad()     — builds payload, calls API in batches, stamps status
//   syncOwnerJobStatuses() — polls jobs endpoint, writes AF Owner IDs
// ============================================================


// ── Prep ─────────────────────────────────────────────────────

/**
 * Validates all non-Success rows on the Owners sheet.
 * Sets ReferenceId for clean rows, marks errors in red.
 * Safe to run repeatedly — never calls the API.
 */
function prepOwners() {
  const ss    = SpreadsheetApp.getActive();
  const sheet = ss.getSheetByName('Owners');
  if (!sheet) return;

  const data    = sheet.getDataRange().getValues();
  const headers = data[0].map(h => String(h).trim());
  const h       = {};
  headers.forEach((title, i) => { h[title] = i; });

  const timestamp = new Date().getTime();

  const isReallyTrue = val => {
    if (val === true || val === 1) return true;
    if (typeof val === 'string') {
      const s = val.trim().toUpperCase();
      return s === 'TRUE' || s === 'YES' || s === '1';
    }
    return false;
  };

  for (let i = 1; i < data.length; i++) {
    const row    = data[i];
    const status = String(row[h['API_Status']] || '');

    if (status === 'Success' || status.includes('Pending')) continue;

    const errors = [];

    // ── 1. Identity check ────────────────────────────────────
    const hasName = (row[h['FirstName']] && row[h['LastName']]) || row[h['CompanyName']];
    if (!hasName) errors.push('Missing Name/Company');

    const stateVal = String(row[h['State']] || '').trim();
    if (stateVal && stateVal.length !== 2) errors.push('State must be 2-letter abbreviation');

    // ── 2. Bank info conditional check ───────────────────────
    const achEnabled = isReallyTrue(row[h['OwnerPaidByACH']]);
    if (achEnabled) {
      const routing = String(row[h['BankAccountRoutingNumber']] || '').trim();
      const account = String(row[h['BankAccountNumber']]        || '').trim();

      if (!routing) {
        errors.push('BankAccountRoutingNumber required when OwnerPaidByACH is TRUE');
        sheet.getRange(i + 1, h['BankAccountRoutingNumber'] + 1).setBackground('#f4cccc');
      } else {
        sheet.getRange(i + 1, h['BankAccountRoutingNumber'] + 1).setNumberFormat('@').setBackground(null);
      }

      if (!account) {
        errors.push('BankAccountNumber required when OwnerPaidByACH is TRUE');
        sheet.getRange(i + 1, h['BankAccountNumber'] + 1).setBackground('#f4cccc');
      } else {
        sheet.getRange(i + 1, h['BankAccountNumber'] + 1).setNumberFormat('@').setBackground(null);
      }
    }

    // ── 3. TaxId formatting ──────────────────────────────────
    const taxId = String(row[h['TaxId']] || '').replace(/\D/g, '');
    if (taxId && taxId.length === 9) {
      sheet.getRange(i + 1, h['TaxId'] + 1).setNumberFormat('@');
    } else if (taxId && taxId.length !== 9) {
      errors.push('TaxID must be 9 digits');
    }

    if (h['PostalCode'] !== undefined) {
      sheet.getRange(i + 1, h['PostalCode'] + 1).setNumberFormat('@');
    }

    // ── 4. Write status & ReferenceId ────────────────────────
    const statusCell = sheet.getRange(i + 1, h['API_Status']  + 1);
    const refCell    = sheet.getRange(i + 1, h['ReferenceId'] + 1);

    if (errors.length > 0) {
      statusCell.setValue('Error:\n• ' + errors.join('\n• ')).setBackground('#f4cccc');
      refCell.setValue('');
    } else {
      refCell.setValue(`Owner_${timestamp}_${i}`);
      statusCell.setValue('Ready').setBackground('#cfe2f3');
    }
  }

  SpreadsheetApp.flush();
  SpreadsheetApp.getActive().toast('Validation Complete.', 'Owners Prep');
}


// ── Execute Load ─────────────────────────────────────────────

/**
 * Builds the owner payload from all "Ready" rows and sends to the AF
 * bulk create endpoint in batches of 40.
 *
 * Owners is an ASYNC endpoint — the API returns a JobId per chunk,
 * NOT per-row results. Rows are stamped "Pending: <jobId>" until
 * syncOwnerJobStatuses() resolves them.
 *
 * FIX: replaced the banks-style `rowResult = chunkResult.data[index]`
 * pattern (which caused "rowResult is not defined") with the correct
 * JobId extraction pattern used by vendors and late fees.
 */
function executeOwnerLoad() {
  const ss    = SpreadsheetApp.getActive();
  const sheet = ss.getSheetByName('Owners');
  if (!sheet) return;

  SpreadsheetApp.flush();

  const data    = sheet.getDataRange().getValues();
  const headers = data[0].map(h => String(h).trim());
  const h       = {};
  headers.forEach((title, i) => { h[title] = i; });

  const ownerArray      = [];
  const readyRowIndices = [];

  const isTrue = v => String(v).toUpperCase() === 'TRUE' || v === true;

  for (let i = 1; i < data.length; i++) {
    const row = data[i];
    if (String(row[h['API_Status']]).trim() !== 'Ready') continue;

    const record     = {};
    const phones     = {};
    const addressObj = {};
    let   emailVal   = '';

    // ── Mandatory fields ─────────────────────────────────────
    record.ReferenceId    = String(row[h['ReferenceId']]).trim();
    record.OwnerPaidByACH = isTrue(row[h['OwnerPaidByACH']]);

    if (row[h['FirstName']])   record.FirstName  = String(row[h['FirstName']]).trim();
    if (row[h['LastName']])    record.LastName    = String(row[h['LastName']]).trim();
    if (row[h['CompanyName']]) record.CompanyName = String(row[h['CompanyName']]).trim();

    // ── Dynamic field mapping ─────────────────────────────────
    headers.forEach((title, idx) => {
      const val = row[idx];
      if (val === '' || val === null) return;

      const clean = title.toLowerCase().replace(/\s+/g, '').replace(/_/g, '');

      const manual = ['referenceid','apistatus','ownerpaidbyach','useonlinepayables',
                      'firstname','lastname','companyname'];
      if (manual.includes(clean)) return;

      // Phone fields — PhoneNumber1, Label1, etc.
      const phoneMatch = title.match(/^(PhoneNumber|Label)(\d+)$/);
      if (phoneMatch) {
        const [, key, num] = phoneMatch;
        if (!phones[num]) phones[num] = {};
        phones[num][key === 'PhoneNumber' ? 'Number' : key] = String(val);
        return;
      }

      // Address fields
      if (['address1','address2','city','state','postalcode'].includes(clean)) {
        addressObj[title] = (clean === 'postalcode') ? String(val).trim() : val;
        return;
      }

      // Email
      if (clean === 'email') { emailVal = val; return; }

      // Bank + tax fields — force string to preserve leading zeros
      const bankTax = ['bankaccountroutingnumber','bankaccountnumber','taxid','taxpayerid',
                       'taxpayername','savingsaccount','send1099','sending1099preference'];
      if (bankTax.includes(clean)) {
        record[title] = (typeof val === 'boolean') ? isTrue(val) : String(val).trim();
        return;
      }

      // Remaining fields via allowlist
      if (CONFIG.ALLOWED_OWNERS.includes(title)) {
        record[title] = castPropertyType(title, val);
      }
    });

    // ── Build nested arrays ───────────────────────────────────
    if (emailVal) {
      record.Emails = [{ EmailAddress: String(emailVal), IsPrimary: true }];
    }

    const phoneItems = Object.keys(phones)
      .sort()
      .map((key, idx) => ({
        Number:    String(phones[key].Number || ''),
        Label:     phones[key].Label || 'Mobile',
        IsPrimary: idx === 0
      }))
      .filter(p => p.Number !== '');
    if (phoneItems.length) record.PhoneNumbers = phoneItems;

    if (addressObj.Address1) {
      record.Addresses = [{ ...addressObj, CountryCode: 'US', IsPrimary: true }];
    }

    if (row[h['AlternatePayeeName']]) {
      record.UseAlternatePayee           = true;
      record.AlternatePaymentCountryCode = 'US';
    }

    ownerArray.push(record);
    readyRowIndices.push(i + 1);
  }

  if (!ownerArray.length) {
    ss.toast('No "Ready" rows found. Run Prep first.', 'Owners Load');
    return;
  }

  // ── Batch load — async endpoint (returns JobId, not per-row data) ─────
  // Each chunk gets its own JobId. All rows in that chunk are stamped
  // "Pending: <jobId>" so syncOwnerJobStatuses() can resolve them later.
  // Never reference chunkResult.data[index] here — that's the sync pattern
  // for direct endpoints (Banks, Properties) and will throw for async ones.
  batchWithRetry({
    endpoint:    CONFIG.ENDPOINTS.OWNERS,
    payloads:    ownerArray,
    rowNums:     readyRowIndices,
    logObject:   'Owners',
    chunkSize:   40,
    onChunkDone: ({ chunkResult, chunkRows }) => {

      // Extract the JobId from whichever shape the response came back in
      const jobId = (chunkResult.data && chunkResult.data.JobId)
        ? chunkResult.data.JobId
        : ((chunkResult.message && chunkResult.message.match(/"JobId"\s*:\s*"([^"]+)"/) || [])[1] || null);

      if (jobId) {
        chunkRows.forEach(rowNum => {
          sheet.getRange(rowNum, h['API_Status'] + 1)
            .setValue(`Pending: ${jobId}`)
            .setBackground('#fff2cc');
        });
      } else {
        // No JobId returned — chunk-level failure
        chunkRows.forEach(rowNum => {
          sheet.getRange(rowNum, h['API_Status'] + 1)
            .setValue('Error: ' + (chunkResult.message || 'Check Logs'))
            .setBackground('#f4cccc');
        });
      }

      SpreadsheetApp.flush();
    }
  });

  ss.toast(`${ownerArray.length} owner(s) submitted. Run Sync to confirm status.`, 'Owners Load');
}


// ── Sync Job Statuses ─────────────────────────────────────────

/**
 * Polls the AF Jobs endpoint for all "Pending: <jobId>" rows.
 * On resolution:
 *   • Writes AF Owner ResourceId to API_ID column
 *   • Updates API_Status to "Success" or "Error: <reason>"
 *   • Calls updateLogDataAfterSync() so the Log Viewer sidebar
 *     shows resolved ResourceIds instead of the placeholder JobId
 */
function syncOwnerJobStatuses() {
  const ss    = SpreadsheetApp.getActive();
  const sheet = ss.getSheetByName('Owners');
  if (!sheet) return;

  const data      = sheet.getDataRange().getValues();
  const headers   = data[0].map(h => String(h).trim());
  const statusIdx = headers.indexOf('API_Status');
  const apiIdIdx  = headers.indexOf('API_ID');
  const refIdIdx  = headers.indexOf('ReferenceId');

  // ── Collect pending jobs: { jobId → [rowNumber, ...] } ───
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
    ss.toast('No pending jobs found.', 'Owner Sync');
    return;
  }

  // ── Poll each job ─────────────────────────────────────────
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

      // Write results back to sheet rows
      pendingJobs[jobId].forEach(rowNum => {
        const rowRefId = String(data[rowNum - 1][refIdIdx]).trim();
        const match    = apiMap[rowRefId];
        if (!match) return;

        if (match.success && match.resourceId) {
          sheet.getRange(rowNum, apiIdIdx  + 1).setValue(match.resourceId);
          sheet.getRange(rowNum, statusIdx + 1).setValue('Success').setBackground('#b6d7a8');
        } else {
          sheet.getRange(rowNum, statusIdx + 1)
            .setValue('Error: ' + match.error)
            .setBackground('#f4cccc');
        }
      });

      // Update _Log Data so the Log Viewer reflects resolved ResourceIds
      const resolvedPayload = job.Result.map(item => ({
        referenceId: String(item.ReferenceId).trim(),
        resourceId:  item.ResourceId  || '',
        successful:  item.Successful  || item.successful || false,
        error:       item.Error       || item.error      || ''
      }));

      updateLogDataAfterSync(jobId, resolvedPayload);

    } catch (e) {
      console.error('syncOwnerJobStatuses — error on job ' + jobId + ': ' + e.message);
    }
  });

  ss.toast('Sync Complete.', 'Owner Sync');
}
