// ============================================================
// GL_ACCOUNTS.GS — Sync GL Accounts from AppFolio
//
// Reads all GL Accounts from the AppFolio API and stores each
// account's UUID as a cell Note in the 'Number' column of the
// 'GL Accounts' sheet. The Note is then read by
// prepRecurringCharges() (in tenants.gs) to resolve GL IDs
// without additional API calls at load time.
//
//
// CALLED FROM
// ───────────
//   • onOpen.gs menu: "Onboarding API > Sync GL Accounts"
//   • prepRecurringCharges() prereq — must run first
// ============================================================


/**
 * Syncs all GL Accounts from AppFolio to the 'GL Accounts' sheet.
 *
 * For each account returned by the API:
 *   - Matches the sheet row by GL Number (column A)
 *   - Stores the AppFolio UUID as a cell Note on the Number cell
 *   - Optionally fills in Account Name and Account Type if blank
 *
 * The UUID Note is the key mechanism — prepRecurringCharges()
 * reads getNotes() on the Number column to resolve IDs at prep
 * time without any additional API calls.
 *
 * Pagination: iterates all pages (default page size 1000) to
 * ensure the full chart of accounts is captured, not just the
 * first page.
 */
function syncGLAccounts() {
  const ss    = SpreadsheetApp.getActive();
  let   sheet = ss.getSheetByName('GL Accounts');

  // Auto-create the sheet with standard headers if it's missing
  if (!sheet) {
    sheet = ss.insertSheet('GL Accounts');
    sheet.getRange(1, 1, 1, 4).setValues([['Number','Account Name','Account Type','Fund Accounts']]);
    sheet.getRange(1, 1, 1, 4)
      .setBackground('#1E2430').setFontColor('#FFFFFF')
      .setFontWeight('bold').setFontSize(10);
    sheet.setFrozenRows(1);
    ss.toast('GL Accounts sheet created. Syncing now…', 'GL Sync');
  }

  const options = {
    method:             'get',
    headers:            getApiHeaders(),
    muteHttpExceptions: true
  };

  // ── Paginate through all GL accounts ─────────────────────
  // The endpoint supports page[number] and page[size].
  // We collect everything before writing so one bad page
  // doesn't leave the sheet in a partial state.
  let allAccounts = [];
  let pageNum     = 1;
  const PAGE_SIZE = 1000;

  try {
    while (true) {
      const url      = `${CONFIG.ENDPOINTS.GL_ACCOUNTS}?page[number]=${pageNum}&page[size]=${PAGE_SIZE}`;
      const response = UrlFetchApp.fetch(url, options);
      const code     = response.getResponseCode();

      if (code !== 200) {
        ss.toast(`Fetch failed (HTTP ${code}). Check credentials.`, 'GL Sync Error');
        return;
      }

      const json = JSON.parse(response.getContentText());
      const page = json.data || [];
      allAccounts = allAccounts.concat(page);

      // Stop if this page returned fewer than PAGE_SIZE — we've hit the last page
      if (page.length < PAGE_SIZE) break;
      pageNum++;
    }
  } catch (e) {
    ss.toast('Error fetching GL Accounts: ' + e.message, 'GL Sync Error');
    return;
  }

  if (!allAccounts.length) {
    ss.toast('No GL Accounts returned from API.', 'GL Sync');
    return;
  }

  // ── Build API map: Number → account object ────────────────
  const apiMap = {};
  allAccounts.forEach(acc => {
    apiMap[String(acc.Number).trim()] = acc;
  });

  // ── Read sheet and write Notes + fill blanks ──────────────
  const sheetData = sheet.getDataRange().getValues();
  const headers   = sheetData[0].map(h => String(h).trim());

  const numIdx  = headers.indexOf('Number');
  const nameIdx = headers.indexOf('Account Name');
  const typeIdx = headers.indexOf('Account Type');

  if (numIdx === -1) {
    ss.toast('"Number" column not found in GL Accounts sheet.', 'GL Sync Error');
    return;
  }

  let synced = 0;

  for (let i = 1; i < sheetData.length; i++) {
    const rowNum    = i + 1;
    const sheetNum  = String(sheetData[i][numIdx]).trim();
    const acc       = apiMap[sheetNum];

    if (!acc) continue;  // GL number in sheet not found in API — skip silently

    // Store the UUID as a Note on the Number cell
    // prepRecurringCharges() reads this Note via getNotes()
    sheet.getRange(rowNum, numIdx + 1).setNote(acc.Id);

    // Fill in Account Name if the cell is blank
    if (nameIdx !== -1 && !String(sheetData[i][nameIdx]).trim()) {
      sheet.getRange(rowNum, nameIdx + 1).setValue(acc.Name || acc.AccountName || '');
    }

    // Fill in Account Type if the cell is blank
    if (typeIdx !== -1 && !String(sheetData[i][typeIdx]).trim()) {
      sheet.getRange(rowNum, typeIdx + 1).setValue(acc.Type || '');
    }

    synced++;
  }

  ss.toast(
    `${synced} GL Account UUIDs synced to Notes. (${allAccounts.length} total in API)`,
    'GL Sync Complete'
  );
}
