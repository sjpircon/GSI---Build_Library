// ============================================================
// ONOPEN.GS — Menu Builder, Sidebar Launchers, Step State Store
//
// Builds the "Onboarding API" top-level menu on spreadsheet open.
// All Prep / Load / Sync actions are exposed through the Dashboard
// sidebar — the menu here is intentionally minimal so non-technical
// users have a single entry point rather than nested submenus.
//
// STEP STATE PERSISTENCE
// ──────────────────────
// The UnifiedSidebar tracks workflow progress (none → prepped → loaded)
// per object type. States are saved to UserProperties so they survive
// sidebar reloads, tab switches, and browser refreshes.
//
//   saveStepStates(statesJson)  — called by sidebar after any action
//   getStepStates()             — called by sidebar on Load tab open
//   clearStepStates()           — utility; call from menu to reset
//   getSheetSummary(stepId)     — called by sidebar after prep/load
//                                 to populate per-step badge counts
// ============================================================


function onOpen() {
  const props   = PropertiesService.getUserProperties();
  const env     = props.getProperty('AF_ACTIVE_SET') || 'IMPORT';
  const company = props.getProperty('AF_COMPANY_' + env) || 'Not Connected';

  const ui          = SpreadsheetApp.getUi();
  const envIcon     = (env === 'LIVE') ? '🚀' : '💡';
  const statusLabel = `${envIcon} ${env}: ${company}`;

  ui.createMenu('Onboarding API')
    .addItem(statusLabel, 'showSetupSidebar')                  // Clicking status opens connection settings
    .addItem('🧰  Open Toolbox',  'showUnifiedSidebar')        // Primary entry point
    .addSeparator()
    .addItem('🗑️  Reset Checklist', 'clearStepStates')          // Utility — resets sidebar progress badges
    //.addItem('🔓  Release Occupancy Lock', 'forceReleaseOccupancyLock') // Recovery — clears stuck mutex
    .addItem('⚡  Clear Dependencies',  'forceClearDependencies') // Recovery — marks all steps loaded so any action is accessible
    .addToUi();

  if (company !== 'Not Connected') {
    SpreadsheetApp.getActive().toast(`Target: ${company}`, `${envIcon} ${env} ACTIVE`, 5);
  }
}


// ── Sidebar / Dialog Launchers ───────────────────────────────

function showSetupSidebar() {
  const html = HtmlService.createHtmlOutputFromFile('SetupUI')
    .setTitle('API Connection')
    .setWidth(340);
  SpreadsheetApp.getUi().showSidebar(html);
}

// NOTE: showDashboard() left here as an alias for backward compatibility
// but showUnifiedSidebar() is the canonical launcher for the current UI.
function showDashboard() {
  showUnifiedSidebar();
}

function showInstructionSidebar() {
  const html = HtmlService.createHtmlOutputFromFile('ChecklistSidebar')
    .setTitle('Onboarding Checklist')
    .setWidth(340);
  SpreadsheetApp.getUi().showSidebar(html);
}

function showUnifiedSidebar() {
  const html = HtmlService.createHtmlOutputFromFile('UnifiedSidebar')
    .setTitle('AppFolio Load Tool')
    .setWidth(340);
  SpreadsheetApp.getUi().showSidebar(html);
}

// Called by SetupUI.html when the user wants to switch environments
// from inside the UnifiedSidebar flow.
function showApiSetupSidebar() {
  showSetupSidebar();
}


// ── Dashboard Data Providers ─────────────────────────────────
// NOTE: getDashboardHeader is defined ONCE here. It was previously
// duplicated at the bottom of this file — that duplicate has been
// removed to prevent silent resolution conflicts in Apps Script.

/**
 * Returns connection state and active environment for the sidebar header.
 * Called by UnifiedSidebar.html and SetupUI.html on load.
 */
function getDashboardHeader() {
  const props   = PropertiesService.getUserProperties();
  const env     = props.getProperty('AF_ACTIVE_SET') || 'IMPORT';
  const company = props.getProperty('AF_COMPANY_' + env) || 'Not Connected';
  const devId   = props.getProperty('AF_DEV_ID') || '';
  return {
    env,
    company,
    connected: (company !== 'Not Connected'),
    devId
  };
}

/**
 * Reads every workflow sheet and returns a status summary per object type.
 * Used by the Dashboard to show pill counts (e.g. "12 Ready", "3 Errors").
 *
 * Returns an array: { label, sheetName, ready, errors, success, pending, total }
 */
function getDashboardStatus() {
  const ss = SpreadsheetApp.getActive();

  const objects = [
    { label: 'Banks',         sheetName: 'Banks'      },
    { label: 'Owners',        sheetName: 'Owners'     },
    { label: 'Properties',    sheetName: 'Properties' },
    { label: 'Owner Groups',  sheetName: 'Properties', statusCol: 'Owner_Group_Status' },
    { label: 'Unit Types',    sheetName: 'Unit Types' },
    { label: 'Units',         sheetName: 'Units'      },
    { label: 'Occupancies',   sheetName: 'Tenants'    },
    { label: 'Rec. Charges',  sheetName: 'Tenants',   statusCol: 'Charge_Load_Status' },
    { label: 'Vendors',       sheetName: 'Vendors'    }
  ];

  return objects.map(obj => {
    const sheet = ss.getSheetByName(obj.sheetName);
    if (!sheet) return { ...obj, ready: 0, errors: 0, success: 0, pending: 0, total: 0, missing: true };

    const data    = sheet.getDataRange().getValues();
    const headers = data[0].map(h => String(h).trim());
    const colName = obj.statusCol || 'API_Status';
    const idx     = headers.indexOf(colName);
    if (idx === -1) return { ...obj, ready: 0, errors: 0, success: 0, pending: 0, total: 0 };

    let ready = 0, errors = 0, success = 0, pending = 0;
    for (let i = 1; i < data.length; i++) {
      const s = String(data[i][idx]).trim();
      if (!s) continue;
      if (s === 'Ready' || s === 'Ready for Group Load') ready++;
      else if (s === 'Success')          success++;
      else if (s.includes('Pending'))    pending++;
      else if (s.includes('Error'))      errors++;
    }

    return {
      label: obj.label, sheetName: obj.sheetName,
      ready, errors, success, pending,
      total: ready + errors + success + pending
    };
  });
}


// ── Step State Persistence ────────────────────────────────────
// The UnifiedSidebar uses a WORKFLOW_STEPS array to track which
// objects have been prepped / loaded. These functions persist that
// state to UserProperties so it survives sidebar reloads.
//
// Key: STEP_STATES
// Value: JSON string — shape: { steps: [...], checklist: {...} }
//        (the sidebar owns the shape; these functions are intentionally
//         shape-agnostic so they don't need updating when the sidebar
//         evolves its state structure)

/**
 * Saves the sidebar's step progress to UserProperties.
 * Called by UnifiedSidebar.html after every Prep, Load, or Sync action.
 *
 * @param {string} statesJson — JSON string; shape managed by the sidebar
 */
function saveStepStates(statesJson) {
  if (!statesJson) return;
  try {
    // Validate it's parseable before saving — bad JSON would break
    // the sidebar silently on next restore.
    JSON.parse(statesJson);
    PropertiesService.getUserProperties().setProperty('STEP_STATES', statesJson);
  } catch (e) {
    console.warn('saveStepStates: invalid JSON, not saved. ' + e.message);
  }
}

/**
 * Returns the saved step progress JSON string, or null if nothing saved.
 * Called by UnifiedSidebar.html on Load tab open.
 *
 * @returns {string|null}
 */
function getStepStates() {
  return PropertiesService.getUserProperties().getProperty('STEP_STATES') || null;
}

/**
 * Clears all saved step progress — bound to "Reset Checklist" menu item.
 * Useful when starting a new onboarding project on the same sheet.
 */
function clearStepStates() {
  PropertiesService.getUserProperties().deleteProperty('STEP_STATES');
  SpreadsheetApp.getActive().toast(
    'Workflow progress reset. Refresh the sidebar to see changes.',
    '🗑️ Step Progress Cleared'
  );
}

/**
 * Releases the occupancy lock and bypasses dependency gates in the sidebar
 * without altering any step states, badges, or record counts.
 *
 * Sets depsUnlocked: true in STEP_STATES. The sidebar reads this flag into
 * _depsUnlocked and returns 'ok' from depTier() for every step regardless
 * of actual state — making all PREP/LOAD buttons clickable.
 *
 * The flag is cleared by "Reset Checklist" (which deletes STEP_STATES).
 * After running, refresh the sidebar (close and reopen) to see the change.
 */
function forceClearDependencies() {
  // 1. Release any active GAS locks
  forceReleaseOccupancyLock();

  // 2. Add depsUnlocked flag to existing STEP_STATES — preserves all step
  //    states, badges and record counts; only bypasses the dep gate.
  const existing = getStepStates();
  let payload;
  try   { payload = existing ? JSON.parse(existing) : { steps: [], checklist: {} }; }
  catch (e) { payload = { steps: [], checklist: {} }; }
  payload.depsUnlocked = true;
  saveStepStates(JSON.stringify(payload));

  SpreadsheetApp.getActive().toast(
    'All locks released. Step dependencies bypassed.\nRefresh the sidebar to see changes.',
    '⚡ Dependencies Cleared'
  );
}

/**
 * Returns live status counts for a single workflow step.
 * Called by UnifiedSidebar.html after every Prep or Load action
 * to populate the per-step badge (e.g. "12 Ready · 3 Error").
 *
 * Each stepId maps to a sheet name and status column. The mapping
 * mirrors getDashboardStatus() so both views always reflect the same
 * underlying data — if you rename a sheet or status column, update
 * STEP_MAP here and the objects array in getDashboardStatus() together.
 *
 * Returned shape:
 *   { ready, errors, success, pending, fail, total }
 *
 *   ready   — rows with status "Ready" or "Ready for Group Load"
 *   errors  — rows whose status contains "Error"
 *   success — rows with status "Success"
 *   pending — rows whose status contains "Pending"
 *   fail    — alias for errors (sidebar uses both labels)
 *   total   — sum of all non-blank rows counted above
 *
 * Returns zeroes (not an exception) when the sheet is missing or the
 * status column cannot be found, so the sidebar degrades gracefully.
 *
 * @param  {string} stepId — matches the `id` field in WORKFLOW_STEPS
 *                           (e.g. 'banks', 'owners', 'ownerGroups')
 * @returns {{ ready:number, errors:number, success:number,
 *             pending:number, fail:number, total:number }}
 */
function getSheetSummary(stepId) {

  // ── Step → sheet mapping ─────────────────────────────────
  // Keys MUST match the `id` values in WORKFLOW_STEPS in UnifiedSidebar.html
  // exactly (case-sensitive). Current sidebar IDs use all-lowercase, no
  // separators: 'ownergroups', 'unittypes', 'reccharges', 'occupancies'.
  //
  // 'ownergroups' and 'reccharges' share a sheet with a different status
  // column — intentional, mirrors getDashboardStatus().
  const STEP_MAP = {
    banks:             { sheetName: 'Banks'      },
    owners:            { sheetName: 'Owners'     },
    properties:        { sheetName: 'Properties' },
    ownergroups:       { sheetName: 'Properties', statusCol: 'Owner_Group_Status' },
    propertylatefees:  { sheetName: 'Properties', statusCol: 'LateFee_Status'     },
    unittypes:         { sheetName: 'Unit Types' },
    units:             { sheetName: 'Units'      },
    occupancies:       { sheetName: 'Tenants'    },
    reccharges:        { sheetName: 'Tenants',   statusCol: 'Charge_Load_Status'  },
    occupancylatefees: { sheetName: 'Tenants',   statusCol: 'LateFee_Status'      },
    vendors:           { sheetName: 'Vendors'    }
  };

  // Zero-value fallback returned on any lookup failure
  const EMPTY = { ready: 0, errors: 0, success: 0, pending: 0, fail: 0, total: 0 };

  // ── Resolve mapping ──────────────────────────────────────
  const mapping = STEP_MAP[stepId];
  if (!mapping) {
    console.warn(`getSheetSummary: unknown stepId "${stepId}"`);
    return EMPTY;
  }

  // ── Read sheet ───────────────────────────────────────────
  const ss    = SpreadsheetApp.getActive();
  const sheet = ss.getSheetByName(mapping.sheetName);
  if (!sheet) {
    console.warn(`getSheetSummary: sheet "${mapping.sheetName}" not found`);
    return EMPTY;
  }

  const data    = sheet.getDataRange().getValues();
  if (data.length < 2) return EMPTY; // header-only or empty sheet

  const headers = data[0].map(h => String(h).trim());
  const colName = mapping.statusCol || 'API_Status';
  const idx     = headers.indexOf(colName);
  if (idx === -1) {
    console.warn(`getSheetSummary: column "${colName}" not found in sheet "${mapping.sheetName}"`);
    return EMPTY;
  }

  // ── Count statuses ───────────────────────────────────────
  // Mirrors the counting logic in getDashboardStatus() exactly.
  // If you change status string conventions there, change them here too.
  let ready = 0, errors = 0, success = 0, pending = 0;

  for (let i = 1; i < data.length; i++) {
    const s = String(data[i][idx]).trim();
    if (!s) continue; // skip blank rows

    if      (s === 'Ready' || s === 'Ready for Group Load') ready++;
    else if (s === 'Success')        success++;
    else if (s.includes('Pending'))  pending++;
    else if (s.includes('Error'))    errors++;
    // Any other non-blank status (e.g. 'Skipped') is intentionally ignored
    // so it doesn't inflate totals — add cases here as new statuses emerge.
  }

  return {
    ready,
    errors,
    success,
    pending,
    fail:  errors,   // alias — sidebar badge logic uses both 'errors' and 'fail'
    total: ready + errors + success + pending
  };
}


// ── Safety Gate ──────────────────────────────────────────────

/**
 * Universal confirmation dialog before any destructive API call.
 * All "Load" menu actions route through this before executing.
 *
 * @param {Function} callback   — Function to run if confirmed
 * @param {string}   actionName — Human-readable label shown in dialog
 */
// ── Safety Gate ──────────────────────────────────────────────
// FIX: `return callback()` so load functions that return a signal
// object (e.g. { timedOut: true }) propagate that value back to
// google.script.run's withSuccessHandler in the sidebar.
// Previously `callback()` was called without return, swallowing
// the return value — the sidebar always received undefined.

function confirmAndRun(callback, actionName) {
  const props   = PropertiesService.getUserProperties();
  const env     = props.getProperty('AF_ACTIVE_SET') || 'IMPORT';
  const company = props.getProperty('AF_COMPANY_' + env) || 'Unknown Client';
  const ui      = SpreadsheetApp.getUi();

  const title = (env === 'LIVE') ? '🚨 WARNING: LIVE LOAD 🚨' : 'Confirm API Load';
  const msg   = `Action: ${actionName}\nTarget: ${company}\nEnvironment: ${env}\n\nAre you sure you want to proceed?`;

  if (ui.alert(title, msg, ui.ButtonSet.YES_NO) === ui.Button.YES) {
    return callback();   // ← return added
  } else {
    SpreadsheetApp.getActive().toast('Action Cancelled', 'Safety System');
    return { cancelled: true };
  }
}


// ── Wrapper Triggers ─────────────────────────────────────────
// FIX: All wrappers now `return confirmAndRun(...)` so the signal
// object flows through to the sidebar's withSuccessHandler.

function runBankLoad()            { return confirmAndRun(executeBankLoad,              'Bank Account Bulk Load');      }
function runOwnerLoad()           { return confirmAndRun(executeOwnerLoad,             'Owner Bulk Load');              }
function runPropertyLoad()        { return confirmAndRun(executePropertyLoad,          'Property Bulk Load');           }
function runOwnerGroupLoad()      { return confirmAndRun(executeOwnerGroupLoad,        'Owner Group Load');             }
function runUnitTypeLoad()        { return confirmAndRun(executeUnitTypeLoad,          'Unit Type Bulk Load');          }
function runUnifiedUnitLoad()     { return confirmAndRun(_executeUnifiedUnitLoad,      'Unit Load (POST + PATCH)');     }
function runTenantLoad()          { return confirmAndRun(executeTenantLoad,            'Bulk Tenant Load');             }
function runRecurringChargeLoad()  { return confirmAndRun(executeRecurringChargeLoad,  'Recurring Charge Load');         }
function runPropertyLateFeeLoad()  { return confirmAndRun(executePropertyLateFeeLoad,  'Property Late Fee Load');        }
function runOccupancyLateFeeLoad() { return confirmAndRun(executeOccupancyLateFeeLoad, 'Occupancy Late Fee Load');       }
function runVendorLoad()           { return confirmAndRun(executeVendorLoad,           'Vendor Bulk Load');              }

function runUnitLoad()            { return confirmAndRun(_executeUnifiedUnitLoad,      'Unit Load (POST + PATCH)');     }

function _executeUnifiedUnitLoad() {
  executeUnitPOST();
  executeUnitPATCH();
  SpreadsheetApp.getActive().toast('Unit Load Complete', 'Success');
  return { timedOut: false, complete: true };
}

// Sync aliases (unchanged — these are synchronous, no return needed)
function runBankLoadSync()            { SpreadsheetApp.getActive().toast('Banks load is synchronous — no sync needed.', 'Sync'); }
function runOwnerLoadSync()           { syncOwnerJobStatuses(); }
function runPropertyLoadSync()        { SpreadsheetApp.getActive().toast('Properties load is synchronous — no sync needed.', 'Sync'); }
function runOwnerGroupLoadSync()      { SpreadsheetApp.getActive().toast('Owner Groups load is synchronous — no sync needed.', 'Sync'); }
function runUnitTypeLoadSync()        { syncUnitTypeJobStatuses(); }
function runUnitLoadSync()            { SpreadsheetApp.getActive().toast('Units load is synchronous — no sync needed.', 'Sync'); }
function runTenantLoadSync()          { syncTenantJobStatuses(); }
function runRecurringChargeLoadSync()  { SpreadsheetApp.getActive().toast('Recurring Charges are synchronous — no sync needed.', 'Sync'); }
function runPropertyLateFeeLoadSync()  { syncPropertyLateFeeStatuses(); }
function runOccupancyLateFeeLoadSync() { syncOccupancyLateFeeStatuses(); }
function runVendorLoadSync()           { syncVendorJobStatuses(); }
