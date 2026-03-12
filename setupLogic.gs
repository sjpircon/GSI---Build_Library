// ============================================================
// COPY DETECTION
// ─────────────
// DocumentProperties follow the file when a spreadsheet is copied.
// To prevent credentials from carrying over to a new copy, we store
// the spreadsheet's immutable ID alongside the credentials at save time.
//
// On every credential read, _assertNotCopied() compares the stored ID
// to the current spreadsheet ID. If they differ, the sheet has been
// copied — credentials are wiped and an error is thrown so no API
// call can proceed silently with the wrong credentials.
//
// This runs in getApiHeaders() (every API call) and getSettingsForUI()
// (sidebar load) so the user sees the prompt immediately on open.
// ============================================================

const _BOUND_SS_KEY = 'AF_BOUND_SPREADSHEET_ID';

/**
 * Checks whether the current spreadsheet is the one credentials
 * were saved to. If not, wipes all credentials and throws so the
 * caller cannot proceed.
 *
 * Called at the top of getApiHeaders() and getSettingsForUI().
 */
function _assertNotCopied() {
  const props    = PropertiesService.getDocumentProperties();
  const boundId  = props.getProperty(_BOUND_SS_KEY);
  const currentId = SpreadsheetApp.getActive().getId();

  if (!boundId) return; // No credentials saved yet — nothing to protect

  if (boundId !== currentId) {
    // This sheet is a copy — wipe everything before anyone can use it
    _wipeCredentials();
    throw new Error(
      'This spreadsheet appears to be a copy. Credentials have been cleared. ' +
      'Please re-enter your credentials for this environment.'
    );
  }
}

/**
 * Clears all stored credentials and the bound spreadsheet ID.
 * Called automatically on copy detection, and available as a
 * manual recovery option.
 */
function _wipeCredentials() {
  const props = PropertiesService.getDocumentProperties();
  [
    _BOUND_SS_KEY,
    'AF_ACTIVE_SET',
    'AF_DEV_ID',
    'AF_ENCODED_CREDS_IMPORT',
    'AF_ENCODED_CREDS_LIVE',
    'AF_CLIENT_ID_IMPORT',
    'AF_CLIENT_ID_LIVE',
    'AF_COMPANY_IMPORT',
    'AF_COMPANY_LIVE'
  ].forEach(key => props.deleteProperty(key));
}


// ── Active Set Management ────────────────────────────────────

function getActiveSet() {
  return PropertiesService.getDocumentProperties().getProperty('AF_ACTIVE_SET') || 'IMPORT';
}

function switchActiveSet(targetSet) {
  const props = PropertiesService.getDocumentProperties();

  if (!props.getProperty('AF_ENCODED_CREDS_' + targetSet)) {
    return {
      success: false,
      message: `"${targetSet}" has no credentials saved yet. Configure it first.`,
      newSet: getActiveSet()
    };
  }

  props.setProperty('AF_ACTIVE_SET', targetSet);
  onOpen();

  const company = props.getProperty('AF_COMPANY_' + targetSet) || 'Connected';
  SpreadsheetApp.getActive().toast(`Now targeting: ${company}`, `🔄 Switched to ${targetSet}`, 5);

  return { success: true, message: '', newSet: targetSet };
}


// ── Credential Storage & Verification ───────────────────────

function saveAndVerify({ set, devId, clientId, clientSecret }) {
  const props   = PropertiesService.getDocumentProperties();
  const cId     = String(clientId     || '').trim();
  const secret  = String(clientSecret || '').trim();
  const dId     = String(devId        || '').trim();

  if (!cId || !secret || !dId) {
    return { success: false, message: 'Developer ID, Client ID, and Client Secret are all required.' };
  }

  const encoded  = Utilities.base64Encode(cId + ':' + secret);
  const authVal  = 'Basic ' + encoded;
  const verifyUrl = 'https://api.appfolio.com/api/v0/portfolios?filters[LastUpdatedAtFrom]=2020-01-01';

  try {
    const resp = UrlFetchApp.fetch(verifyUrl, {
      method: 'get',
      headers: {
        'X-AppFolio-Developer-ID': dId,
        'Authorization': authVal,
        'Content-Type': 'application/json'
      },
      muteHttpExceptions: true
    });

    const code = resp.getResponseCode();
    if (code !== 200) {
      return { success: false, message: `Verification failed (HTTP ${code}). Check your credentials and try again.` };
    }

    let company = 'Verified Account';
    try {
      const json    = JSON.parse(resp.getContentText());
      const primary = (json.data || []).find(p => p.Default === true) || (json.data || [])[0];
      if (primary && primary.Name) company = String(primary.Name).trim();
    } catch (e) { /* leave default */ }

    // ── Persist credentials + bind to this spreadsheet ───────
    // AF_BOUND_SPREADSHEET_ID is the copy-detection anchor.
    // It is always written as part of saveAndVerify() so it stays
    // in sync with the actual credentials — never stale.
    props.setProperty(_BOUND_SS_KEY,                SpreadsheetApp.getActive().getId());
    props.setProperty('AF_DEV_ID',                  dId);
    props.setProperty('AF_ENCODED_CREDS_' + set,    encoded);
    props.setProperty('AF_CLIENT_ID_' + set,        cId);
    props.setProperty('AF_COMPANY_' + set,          company);
    props.setProperty('AF_ACTIVE_SET',              set);

    onOpen();
    SpreadsheetApp.getActive().toast(`Connected to: ${company}`, `✅ ${set} Saved`, 6);

    return { success: true, message: '', company };

  } catch (e) {
    return { success: false, message: 'Connection error: ' + e.message };
  }
}


// ── Auth Header Builder ──────────────────────────────────────

/**
 * Builds HTTP headers for the active credential set.
 * Runs _assertNotCopied() first — if the sheet has been copied,
 * this throws before any API call can be made.
 */
function getApiHeaders() {
  _assertNotCopied(); // ← copy guard — must be first

  const props   = PropertiesService.getDocumentProperties();
  const set     = getActiveSet();
  const devId   = props.getProperty('AF_DEV_ID');
  const encoded = props.getProperty('AF_ENCODED_CREDS_' + set);

  if (!devId || !encoded) {
    throw new Error(
      `No credentials found for "${set}". Open "Onboarding API > Connection Settings" to connect.`
    );
  }

  return {
    'X-AppFolio-Developer-ID': devId,
    'Authorization': 'Basic ' + encoded,
    'Content-Type': 'application/json'
  };
}


// ── Settings UI Data Provider ────────────────────────────────

/**
 * Returns credential state for the SetupUI sidebar.
 * Runs _assertNotCopied() so the sidebar immediately shows the
 * "credentials cleared" state if the sheet was copied.
 */
function getSettingsForUI() {
  try {
    _assertNotCopied();
  } catch (e) {
    // Return an empty state — the sidebar will show no saved credentials
    // and the user will be prompted to enter new ones.
    return {
      activeSet: 'IMPORT',
      devId: '',
      IMPORT: { clientId: '', company: '', saved: false },
      LIVE:   { clientId: '', company: '', saved: false },
      copiedWarning: e.message
    };
  }

  const props = PropertiesService.getDocumentProperties();
  return {
    activeSet: getActiveSet(),
    devId: props.getProperty('AF_DEV_ID') || '',
    IMPORT: {
      clientId: props.getProperty('AF_CLIENT_ID_IMPORT') || '',
      company:  props.getProperty('AF_COMPANY_IMPORT')   || '',
      saved:    !!props.getProperty('AF_ENCODED_CREDS_IMPORT')
    },
    LIVE: {
      clientId: props.getProperty('AF_CLIENT_ID_LIVE') || '',
      company:  props.getProperty('AF_COMPANY_LIVE')   || '',
      saved:    !!props.getProperty('AF_ENCODED_CREDS_LIVE')
    }
  };
}


// ── getDashboardHeader ───────────────────────────────────────

function getDashboardHeader() {
  try {
    _assertNotCopied();
  } catch (e) {
    return { env: 'IMPORT', company: 'Not Connected', connected: false, devId: '', copiedWarning: e.message };
  }

  const props   = PropertiesService.getDocumentProperties();
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


// ── Checklist + Step State ───────────────────────────────────

function getChecklistState() {
  const raw = PropertiesService.getDocumentProperties().getProperty('CHECKLIST_STATE');
  try { return raw ? JSON.parse(raw) : {}; } catch (e) { return {}; }
}

function saveChecklistState(stateObj) {
  PropertiesService.getDocumentProperties()
    .setProperty('CHECKLIST_STATE', JSON.stringify(stateObj || {}));
}

function saveStepStates(statesJson) {
  if (!statesJson) return;
  try {
    JSON.parse(statesJson);
    PropertiesService.getDocumentProperties().setProperty('STEP_STATES', statesJson);
  } catch (e) {
    console.warn('saveStepStates: invalid JSON, not saved. ' + e.message);
  }
}

function getStepStates() {
  return PropertiesService.getDocumentProperties().getProperty('STEP_STATES') || null;
}

function clearStepStates() {
  PropertiesService.getDocumentProperties().deleteProperty('STEP_STATES');
  SpreadsheetApp.getActive().toast(
    'Workflow progress reset. Refresh the sidebar to see changes.',
    '🗑️ Checklist Reset'
  );
}
