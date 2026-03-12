// ============================================================
// UNITS.GS — Prep, POST (MFR), and PATCH (SFR) for Units
//
//
// FUNCTIONS
// ─────────
//   prepUnits()           — validates rows, maps PropertyId + UnitTypeId via Notes
//   runUnitPOST()         — menu wrapper → confirmAndRun
//   executeUnitPOST()     — bulk POST for Multi-Family (MFR) units
//   runUnitPATCH()        — menu wrapper → confirmAndRun
//   executeUnitPATCH()    — individual PATCH for Single-Family (SFR) units
//   runUnifiedUnitLoad()  — runs both POST and PATCH in sequence
//   getAutoUnitId()       — fetches existing Unit ID for a property (SFR helper)
//   buildUnitPayload()    — constructs per-row API payload
// ============================================================


// ── Prep Units ────────────────────────────────────────────────

/**
 * Validates all non-Success rows on the Units sheet.
 *
 * For each row:
 *   1. Looks up the Property's API_ID from the Properties sheet
 *      and stores it as a Note on the PropertyName cell.
 *   2. Sets Action to "POST" (MFR) or "PATCH" (SFR) based on
 *      the property's PropertyType field.
 *   3. For MFR rows, resolves the UnitType name to its API_ID
 *      using a composite key (PropertyId|UnitTypeName) and
 *      stores it as a Note on the UnitType cell.
 *   4. Auto-generates ReferenceId.
 *   5. Stamps Ready or Error.
 */
function prepUnits() {
  const ss        = SpreadsheetApp.getActive();
  const unitSheet = ss.getSheetByName('Units');
  const propSheet = ss.getSheetByName('Properties');
  const utSheet   = ss.getSheetByName('Unit Types');
  if (!unitSheet || !propSheet) return;

  const unitData    = unitSheet.getDataRange().getValues();
  const unitHeaders = unitData[0].map(h => String(h).trim());
  const u           = {};
  unitHeaders.forEach((title, i) => { u[title] = i; });

  // ── Property lookup: Name → { id, type } ─────────────────
  const propData    = propSheet.getDataRange().getValues();
  const propHeaders = propData[0].map(h => String(h).trim());
  const pNameIdx    = propHeaders.indexOf('Name');
  const pApiIdx     = propHeaders.indexOf(CONFIG.API_ID_COL);
  const pTypeIdx    = propHeaders.indexOf('PropertyType');

  const propLookup = {};
  propData.forEach((row, j) => {
    if (j === 0) return;
    const name = String(row[pNameIdx]).trim();
    if (name) propLookup[name] = { id: row[pApiIdx], type: row[pTypeIdx] };
  });

  // ── Unit Type lookup: "PropId|TypeName" → UnitType API_ID ─
  // Uses composite key so two properties with a "Studio" type
  // don't collide with each other.
  const utLookup = {};
  if (utSheet) {
    const utData       = utSheet.getDataRange().getValues();
    const utHeaders    = utData[0].map(h => String(h).trim());
    const utNameIdx    = utHeaders.indexOf('Name');
    const utPropNmIdx  = utHeaders.indexOf('PropertyName');
    const utApiIdx     = utHeaders.indexOf(CONFIG.API_ID_COL);

    // Read PropertyId UUIDs from Notes on the PropertyName column
    const utPropNotes = utSheet.getRange(1, utPropNmIdx + 1, utData.length, 1).getNotes();

    utData.forEach((row, j) => {
      if (j === 0) return;
      const utName   = String(row[utNameIdx]).trim();
      const utPropId = utPropNotes[j][0]; // UUID of the property this Unit Type belongs to
      const utId     = row[utApiIdx];

      if (utName && utPropId && utId && utId !== 'Success') {
        utLookup[`${utPropId}|${utName}`] = utId;
      }
    });
  }

  const timestamp = new Date().getTime();

  for (let i = 1; i < unitData.length; i++) {
    const rowNum = i + 1;
    if (unitData[i][u[CONFIG.STATUS_COL]] === 'Success') continue;

    const rowErrors  = [];
    const propName   = String(unitData[i][u['PropertyName']]).trim();
    const propInfo   = propLookup[propName];

    let action = null;  // set inside the propInfo block; used for conditional validation below

    if (!propInfo || !propInfo.id) {
      rowErrors.push('Property not found or missing API_ID');
    } else {
      // Store PropertyId as Note for use at load time
      unitSheet.getRange(rowNum, u['PropertyName'] + 1).setNote(propInfo.id);

      // Determine action based on property type
      action = propInfo.type === 'Single-Family' ? 'PATCH' : 'POST';
      unitSheet.getRange(rowNum, u['Action'] + 1).setValue(action);

      // ── Property-aware Unit Type mapping ─────────────────
      // Composite key ensures "Studio" on Property A doesn't
      // resolve to Property B's Studio type
      const unitTypeName = String(unitData[i][u['UnitType']] || '').trim();
      if (unitTypeName) {
        const utId = utLookup[`${propInfo.id}|${unitTypeName}`];
        if (utId) {
          unitSheet.getRange(rowNum, u['UnitType'] + 1).setNote(utId);
        } else if (propInfo.type !== 'Single-Family') {
          // MFR rows need a valid Unit Type — flag as error
          rowErrors.push(`Unit Type '${unitTypeName}' not found for this Property`);
        }
      }
    }

    // ── Required field validation ─────────────────────────
    // Name is required for every unit (POST and PATCH)
    if (!String(unitData[i][u['Name']] || '').trim()) {
      rowErrors.push('Name is required');
    }

    // Address fields are required for MFR (POST) — SFR uses the property address
    if (action === 'POST') {
      ['Address1', 'City', 'State', 'Zip'].forEach(field => {
        if (!String(unitData[i][u[field]] || '').trim()) {
          rowErrors.push(`${field} is required`);
        }
      });
    }

    // Pet policy enums — only validated when no UnitType is assigned
    // (AppFolio inherits pet policy from UnitType when one exists)
    const hasUnitType = String(unitData[i][u['UnitType']] || '').trim();
    if (!hasUnitType) {
      const catsVal = String(unitData[i][u['CatsAllowed']] || '').trim();
      const dogsVal = String(unitData[i][u['DogsAllowed']] || '').trim();
      if (catsVal && !['Yes', 'No'].includes(catsVal)) {
        rowErrors.push('CatsAllowed must be "Yes" or "No"');
      }
      if (dogsVal && !['Large & Small', 'Small Only', 'No'].includes(dogsVal)) {
        rowErrors.push('DogsAllowed must be "Large & Small", "Small Only", or "No"');
      }
    }

    // Auto-generate ReferenceId — includes property name so Audit tab filters work
    const safePropUnit = propName.replace(/[^a-zA-Z0-9]/g, '_').replace(/_+/g, '_').replace(/^_|_$/g, '') || 'Property';
    unitSheet.getRange(rowNum, u[CONFIG.REF_ID_COL] + 1).setValue(`${safePropUnit}_Unit_${timestamp}_${i}`);

    const statusCell = unitSheet.getRange(rowNum, u[CONFIG.STATUS_COL] + 1);
    if (rowErrors.length > 0) {
      statusCell.setValue('Errors:\n• ' + rowErrors.join('\n• '));
    } else {
      statusCell.setValue('Ready');
    }
  }

  applyConditionalRules(unitSheet, unitHeaders, u[CONFIG.STATUS_COL]);
  ss.toast('Units Prepped with Property-Specific Unit Types.', 'Units Prep');
}


// ── Execute Unit POST (Multi-Family) ──────────────────────────

function runUnitPOST() {
  confirmAndRun(executeUnitPOST, 'Unit Bulk Load (POST)');
}

/**
 * Bulk POST for MFR units — all rows with Action = "POST".
 *
 * Rows are grouped by PropertyId and sent in sub-batches of 40
 * to stay within AppFolio's bulk payload limits.
 *
 * FIX: callAppFolioAPI() third arg is now 'Units'.
 */
function executeUnitPOST() {
  const ss    = SpreadsheetApp.getActive();
  const sheet = ss.getSheetByName('Units');
  if (!sheet) return;

  const data    = sheet.getDataRange().getValues();
  const headers = data[0].map(h => String(h).trim());
  const h       = {};
  headers.forEach((title, i) => { h[title] = i; });

  // Read property and unit type UUIDs from cell Notes
  const propertyNotes = sheet.getRange(1, h['PropertyName'] + 1, data.length, 1).getNotes();
  const unitTypeNotes = sheet.getRange(1, h['UnitType']     + 1, data.length, 1).getNotes();

  const SUB_BATCH_SIZE  = 40;
  const propertyBatches = {};

  // Group Ready POST rows by PropertyId
  for (let i = 1; i < data.length; i++) {
    const status = String(data[i][h[CONFIG.STATUS_COL]]).trim();
    const action = String(data[i][h['Action']]).trim();
    if (status !== 'Ready' || action !== 'POST') continue;

    const propId = propertyNotes[i][0];
    if (!propId) {
      sheet.getRange(i + 1, h[CONFIG.STATUS_COL] + 1)
        .setValue('Error: Missing Property ID Note').setBackground('#f4cccc');
      continue;
    }

    const utId    = unitTypeNotes[i][0];
    const payload = buildUnitPayload(data[i], h, propId);
    // Inject UnitTypeId if a note was found during prep
    if (utId) payload.UnitTypeId = utId;

    if (!propertyBatches[propId]) propertyBatches[propId] = [];
    propertyBatches[propId].push({ rowIdx: i, payload });
  }

  // Process each property's batch in sub-batches of 40
  for (const propId in propertyBatches) {
    const fullBatch = propertyBatches[propId];

    for (let i = 0; i < fullBatch.length; i += SUB_BATCH_SIZE) {
      const subBatch = fullBatch.slice(i, i + SUB_BATCH_SIZE);
      const payloads = subBatch.map(item => item.payload);

      // 'Units' → audit log object column + direct ID lookup (UnitId)
      const result = callAppFolioAPI(CONFIG.ENDPOINTS.UNITS_BULK, payloads, 'Units');

      subBatch.forEach((item, index) => {
        const rowNum = item.rowIdx + 1;

        if (result.success && result.data && result.data[index]) {
          const unitResult = result.data[index];
          if (unitResult.UnitId || unitResult.successful === true) {
            sheet.getRange(rowNum, h[CONFIG.API_ID_COL] + 1).setValue(unitResult.UnitId || 'Success');
            sheet.getRange(rowNum, h[CONFIG.STATUS_COL] + 1).setValue('Success').setBackground('#b6d7a8');
          } else {
            const err = unitResult.error || unitResult.message || 'Unknown Row Error';
            sheet.getRange(rowNum, h[CONFIG.STATUS_COL] + 1).setValue('Error: ' + err).setBackground('#f4cccc');
          }
        } else {
          const err = result.message || 'Batch Error';
          sheet.getRange(rowNum, h[CONFIG.STATUS_COL] + 1)
            .setValue('Error: ' + err.substring(0, 150)).setBackground('#f4cccc');
        }
      });

      SpreadsheetApp.flush();
    }
  }

  ss.toast('Unit POST Load Complete.', 'Units Load');
}


// ── Execute Unit PATCH (Single-Family) ────────────────────────

/**
 * Menu wrapper — requires confirmation before running.
 * confirmAndRun() lives in onOpen.gs.
 */
function runUnitPATCH() {
  confirmAndRun(executeUnitPATCH, 'Unit Update (PATCH)');
}

/**
 * Individual PATCH for SFR units — all rows with Action = "PATCH".
 *
 * Looks up the existing Unit ID by PropertyId (SFR properties
 * have one unit auto-created by AppFolio at property creation),
 * then PATCHes it with the sheet data.
 *
 * FIX: callAppFolioAPI() third arg is now 'Units'.
 * FIX: Endpoint uses CONFIG.BASE_URL (not CONFIG.ENDPOINTS.UNITS
 *      which doesn't exist in CONFIG).
 */
function executeUnitPATCH() {
  const ss    = SpreadsheetApp.getActive();
  const sheet = ss.getSheetByName('Units');
  const data  = sheet.getDataRange().getValues();
  const headers = data[0].map(h => String(h).trim());
  const h     = {};
  headers.forEach((title, i) => { h[title] = i; });

  for (let i = 1; i < data.length; i++) {
    const rowNum = i + 1;
    const status = String(data[i][h[CONFIG.STATUS_COL]]).trim();
    const action = String(data[i][h['Action']]).trim();
    if (status !== 'Ready' || action !== 'PATCH') continue;

    const propId = sheet.getRange(rowNum, h['PropertyName'] + 1).getNote();

    try {
      const unitId = getAutoUnitId(propId);
      if (!unitId) throw new Error('Existing Unit not found on property.');

      const payload  = buildUnitPayload(data[i], h);
      // FIX: CONFIG.BASE_URL used here — CONFIG.ENDPOINTS.UNITS does not exist
      const endpoint = `${CONFIG.BASE_URL}/units/${unitId}`;

      // 'Units' → audit log object column
      const result = callAppFolioAPI(endpoint, payload, 'Units', 'Patch');

      if (result.success) {
        sheet.getRange(rowNum, h[CONFIG.API_ID_COL] + 1).setValue(unitId);
        sheet.getRange(rowNum, h[CONFIG.STATUS_COL] + 1).setValue('Success').setBackground('#b6d7a8');
      } else {
        throw new Error(result.message);
      }
    } catch (e) {
      sheet.getRange(rowNum, h[CONFIG.STATUS_COL] + 1)
        .setValue('Error: ' + e.message).setBackground('#f4cccc');
    }
  }

  ss.toast('Unit PATCH Complete.', 'Units Load');
}


// ── Unified Unit Load ─────────────────────────────────────────

/**
 * Runs both POST (MFR) and PATCH (SFR) in sequence.
 * Called from the dashboard via onOpen.gs _executeUnifiedUnitLoad().
 *
 * FIX: Removed dependency on old 'SELECTED_ENVIRONMENT' credential
 * key which no longer exists. Environment/company now read via
 * getSettingsForUI() consistent with all other loaders.
 */
function runUnifiedUnitLoad() {
  const settings = getSettingsForUI();  // reads live AF_* keys via setupLogic.gs
  const env      = settings.environment || 'Unknown';
  const company  = settings.companyName || 'Unknown Client';

  const ui       = SpreadsheetApp.getUi();
  const response = ui.alert(
    'Confirm Unit Load',
    `Target: ${company}\nEnvironment: ${env}\n\nThis will process both Multi-Family (POST) and Single-Family (PATCH) units. Proceed?`,
    ui.ButtonSet.YES_NO
  );

  if (response === ui.Button.YES) {
    executeUnitPOST();
    executeUnitPATCH();
    SpreadsheetApp.getActive().toast('Unit Load Complete.', 'Units Load');
  }
}


// ── Helper: Fetch existing Unit ID for a property ─────────────

/**
 * GETs the first unit on a property — used for SFR PATCH.
 * SFR properties have exactly one auto-created unit in AppFolio.
 *
 * FIX: Uses CONFIG.BASE_URL (not CONFIG.ENDPOINTS.UNITS).
 *
 * @param {string} propertyId - AppFolio Property UUID
 * @returns {string|null}     - Unit UUID, or null if not found
 */
function getAutoUnitId(propertyId) {
  const url     = `${CONFIG.BASE_URL}/units?filters[PropertyId]=${propertyId}`;
  const options = { method: 'get', headers: getApiHeaders(), muteHttpExceptions: true };
  const resp    = UrlFetchApp.fetch(url, options);
  const json    = JSON.parse(resp.getContentText());
  return (json.data && json.data.length > 0) ? json.data[0].Id : null;
}


// ── Payload Builder ───────────────────────────────────────────

/**
 * Constructs a single unit API payload from one sheet row.
 *
 * Pet policy (CatsAllowed / DogsAllowed) is only included when no
 * UnitType is assigned — if a UnitType is present the API inherits
 * pet policy from it and will error if the payload also sets it.
 *
 * @param {Array}   rowData  - Single data row
 * @param {Object}  h        - Header → column index map
 * @param {string}  [propId] - PropertyId (required for POST, omit for PATCH)
 * @returns {Object}
 */
function buildUnitPayload(rowData, h, propId = null) {
  const toStrOrNull  = val => (val === '' || val === null) ? null : String(val).trim();
  const toCleanArray = val => (!val || String(val).trim() === '')
    ? []
    : String(val).split(',').map(s => s.trim()).filter(Boolean);

  const p = {
    Name:                 String(rowData[h['Name']]),
    Address1:             String(rowData[h['Address1']]),
    Address2:             String(rowData[h['Address2']] || ''),
    City:                 String(rowData[h['City']]),
    State:                String(rowData[h['State']]),
    Zip:                  String(rowData[h['Zip']]),
    MarketRent:           castPropertyType('MarketRent',  rowData[h['MarketRent']]),
    SquareFeet:           castPropertyType('SquareFeet',  rowData[h['SquareFeet']]),
    Deposit:              castPropertyType('Deposit',     rowData[h['Deposit']]),
    Bedrooms:             castPropertyType('Bedrooms',    rowData[h['Bedrooms']]),
    Bathrooms:            castPropertyType('Bathrooms',   rowData[h['Bathrooms']]),
    MarketingDescription: toStrOrNull(rowData[h['MarketingDescription']]),
    YouTubeURL:           toStrOrNull(rowData[h['YoutubeURL']]),
    Tags:                 toCleanArray(rowData[h['Tags']])
  };

  // Only include pet policy when no UnitType is assigned —
  // AppFolio inherits pet policy from the UnitType if one exists
  // and will return an error if the payload also sets it.
  const hasUnitType = rowData[h['UnitType']] && String(rowData[h['UnitType']]).trim() !== '';
  if (!hasUnitType) {
    p.CatsAllowed = rowData[h['CatsAllowed']] === 'Yes' ? 'Yes' : 'No';
    p.DogsAllowed = rowData[h['DogsAllowed']] === 'Yes' ? 'Large & Small' : 'No';
  }

  // PropertyId and ReferenceId only needed for POST (bulk create)
  if (propId) {
    p.PropertyId  = propId;
    p.ReferenceId = String(rowData[h[CONFIG.REF_ID_COL]]);
  }

  return p;
}
