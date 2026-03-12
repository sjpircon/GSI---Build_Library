// ============================================================
// WRAPPERS.GS
// Delegates all calls to the live_APIUpmarketBSS library.
// Add this file to the bound Apps Script of any copied template.
// ============================================================

// ── Menu / Sidebar Launchers ─────────────────────────────────
function onOpen()                     { live_APIUpmarketBSS.onOpen(); }
function showSetupSidebar()           { live_APIUpmarketBSS.showSetupSidebar(); }
function showUnifiedSidebar()         { live_APIUpmarketBSS.showUnifiedSidebar(); }

// ── Utility / Recovery ───────────────────────────────────────
function clearStepStates()            { live_APIUpmarketBSS.clearStepStates(); }
function forceReleaseOccupancyLock()  { live_APIUpmarketBSS.forceReleaseOccupancyLock(); }
function forceClearDependencies()     { live_APIUpmarketBSS.forceClearDependencies(); }

// ── Settings ─────────────────────────────────────────────────
function saveAndVerify(args)          { return live_APIUpmarketBSS.saveAndVerify(args); }
function switchActiveSet(target)      { return live_APIUpmarketBSS.switchActiveSet(target); }
function getSettingsForUI()           { return live_APIUpmarketBSS.getSettingsForUI(); }

// ── Step State / Sidebar Helpers ─────────────────────────────
function saveStepStates(json)         { return live_APIUpmarketBSS.saveStepStates(json); }
function getStepStates()              { return live_APIUpmarketBSS.getStepStates(); }
function getSheetSummary(stepId)      { return live_APIUpmarketBSS.getSheetSummary(stepId); }
function getConnectionStatus()        { return live_APIUpmarketBSS.getConnectionStatus(); }

// ── Log Viewer ────────────────────────────────────────────────
function getLogList()                 { return live_APIUpmarketBSS.getLogList(); }
function getLogEntry(logId)           { return live_APIUpmarketBSS.getLogEntry(logId); }
function getActiveLogId()             { return live_APIUpmarketBSS.getActiveLogId(); }
function showApiSetupSidebar()        { return live_APIUpmarketBSS.showApiSetupSidebar(); }

// ── Banks ─────────────────────────────────────────────────────
function prepBanks()                  { return live_APIUpmarketBSS.prepBanks(); }
function runBankLoad()                { return live_APIUpmarketBSS.runBankLoad(); }
function executeBankLoad()            { return live_APIUpmarketBSS.executeBankLoad(); }

// ── Properties ───────────────────────────────────────────────
function prepProperties()             { return live_APIUpmarketBSS.prepProperties(); }
function runPropertyLoad()            { return live_APIUpmarketBSS.runPropertyLoad(); }
function executePropertyLoad()        { return live_APIUpmarketBSS.executePropertyLoad(); }
function runPropertyLoadSync()        { return live_APIUpmarketBSS.runPropertyLoadSync(); }

// ── Owners ───────────────────────────────────────────────────
function prepOwners()                 { return live_APIUpmarketBSS.prepOwners(); }
function runOwnerLoad()               { return live_APIUpmarketBSS.runOwnerLoad(); }
function executeOwnerLoad()           { return live_APIUpmarketBSS.executeOwnerLoad(); }
function runOwnerLoadSync()           { return live_APIUpmarketBSS.runOwnerLoadSync(); }
function syncOwnerJobStatuses()       { return live_APIUpmarketBSS.syncOwnerJobStatuses(); }

// ── Owner Groups ─────────────────────────────────────────────
function prepOwnerGroups()            { return live_APIUpmarketBSS.prepOwnerGroups(); }
function runOwnerGroupLoad()          { return live_APIUpmarketBSS.runOwnerGroupLoad(); }
function executeOwnerGroupLoad()      { return live_APIUpmarketBSS.executeOwnerGroupLoad(); }
function runOwnerGroupLoadSync()      { return live_APIUpmarketBSS.runOwnerGroupLoadSync(); }

// ── Unit Types ───────────────────────────────────────────────
function prepUnitTypes()              { return live_APIUpmarketBSS.prepUnitTypes(); }
function runUnitTypeLoad()            { return live_APIUpmarketBSS.runUnitTypeLoad(); }
function executeUnitTypeLoad()        { return live_APIUpmarketBSS.executeUnitTypeLoad(); }
function runUnitTypeLoadSync()        { return live_APIUpmarketBSS.runUnitTypeLoadSync(); }
function syncUnitTypeJobStatuses()    { return live_APIUpmarketBSS.syncUnitTypeJobStatuses(); }

// ── Units ────────────────────────────────────────────────────
function prepUnits()                  { return live_APIUpmarketBSS.prepUnits(); }
function runUnitLoad()                { return live_APIUpmarketBSS.runUnitLoad(); }
function executeUnitLoad()            { return live_APIUpmarketBSS.executeUnitLoad(); }
function runUnitLoadSync()            { return live_APIUpmarketBSS.runUnitLoadSync(); }

// ── Tenants ──────────────────────────────────────────────────
function prepTenants()                { return live_APIUpmarketBSS.prepTenants(); }
function runTenantLoad()              { return live_APIUpmarketBSS.runTenantLoad(); }
function executeTenantLoad()          { return live_APIUpmarketBSS.executeTenantLoad(); }
function runTenantLoadSync()          { return live_APIUpmarketBSS.runTenantLoadSync(); }
function syncTenantJobStatuses()      { return live_APIUpmarketBSS.syncTenantJobStatuses(); }

// ── Recurring Charges ────────────────────────────────────────
function prepRecurringCharges()           { return live_APIUpmarketBSS.prepRecurringCharges(); }
function runRecurringChargeLoad()         { return live_APIUpmarketBSS.runRecurringChargeLoad(); }
function executeRecurringChargeLoad()     { return live_APIUpmarketBSS.executeRecurringChargeLoad(); }
function runRecurringChargeLoadSync()     { return live_APIUpmarketBSS.runRecurringChargeLoadSync(); }

// ── Late Fees — Properties ───────────────────────────────────
function prepPropertyLateFees()           { return live_APIUpmarketBSS.prepPropertyLateFees(); }
function executePropertyLateFeeLoad()     { return live_APIUpmarketBSS.executePropertyLateFeeLoad(); }
function syncPropertyLateFeeStatuses()    { return live_APIUpmarketBSS.syncPropertyLateFeeStatuses(); }
function runPropertyLateFeeLoadSync()     { return live_APIUpmarketBSS.runPropertyLateFeeLoadSync(); }

// ── Late Fees — Occupancy ────────────────────────────────────
function prepOccupancyLateFees()          { return live_APIUpmarketBSS.prepOccupancyLateFees(); }
function executeOccupancyLateFeeLoad()    { return live_APIUpmarketBSS.executeOccupancyLateFeeLoad(); }
function syncOccupancyLateFeeStatuses()   { return live_APIUpmarketBSS.syncOccupancyLateFeeStatuses(); }
function runOccupancyLateFeeLoadSync()    { return live_APIUpmarketBSS.runOccupancyLateFeeLoadSync(); }

// ── Vendors ──────────────────────────────────────────────────
function prepVendors()                { return live_APIUpmarketBSS.prepVendors(); }
function runVendorLoad()              { return live_APIUpmarketBSS.runVendorLoad(); }
function executeVendorLoad()          { return live_APIUpmarketBSS.executeVendorLoad(); }
function runVendorLoadSync()          { return live_APIUpmarketBSS.runVendorLoadSync(); }
function syncVendorJobStatuses()      { return live_APIUpmarketBSS.syncVendorJobStatuses(); }

// ── GL Accounts ──────────────────────────────────────────────
function syncGLAccounts()             { return live_APIUpmarketBSS.syncGLAccounts(); }
