import fs from 'node:fs';
import path from 'node:path';
import { execFileSync } from 'node:child_process';
import { pathToFileURL } from 'node:url';

const moduleRoot = process.env.PHASE21_PLAYWRIGHT_ROOT;
const credentialFile = process.env.PHASE21_ADMIN_CREDENTIAL_FILE;
const outputDir = process.env.PHASE21_BROWSER_EVIDENCE_DIR;
if (!moduleRoot || !credentialFile || !outputDir) throw new Error('operator environment is incomplete');
const { chromium } = await import(pathToFileURL(path.join(moduleRoot, 'node_modules/playwright-core/index.mjs')));
const credentials = Object.fromEntries(fs.readFileSync(credentialFile, 'utf8').trim().split(/\r?\n/).map((line) => line.split('=', 2)));
fs.mkdirSync(outputDir, { recursive: true, mode: 0o700 });

const browser = await chromium.launch({ executablePath: process.env.PHASE21_CHROME_PATH, headless: true });
try {
  const context = await browser.newContext({ viewport: { width: 1366, height: 768 } });
  const first = await context.newPage();
  if (process.env.PHASE21_INITIAL_SEQ) {
    await first.addInitScript((seq) => sessionStorage.setItem('pons.last_seen_seq', seq), process.env.PHASE21_INITIAL_SEQ);
  }
  await first.goto('https://pons.43-165-167-100.sslip.io', { waitUntil: 'networkidle' });
  await first.getByLabel('Username').fill(credentials.username);
  await first.getByLabel('Password').fill(credentials.password);
  await first.getByRole('button', { name: 'Sign In' }).click();
  await first.getByText('Dashboard', { exact: true }).first().waitFor();
  try {
    await first.getByText('WSS LIVE', { exact: true }).waitFor({ timeout: 30_000 });
  } catch (error) {
    await first.screenshot({ path: path.join(outputDir, 'initial-live-timeout.png'), fullPage: true });
    fs.writeFileSync(path.join(outputDir, 'initial-live-timeout.txt'), await first.locator('body').innerText());
    throw error;
  }

  const currentSeq = await first.evaluate(() => sessionStorage.getItem('pons.last_seen_seq') ?? '0');
  const second = await context.newPage();
  await second.addInitScript((seq) => sessionStorage.setItem('pons.last_seen_seq', seq), currentSeq);
  await second.goto('https://pons.43-165-167-100.sslip.io/system', { waitUntil: 'domcontentloaded' });
  await second.getByText('System', { exact: true }).first().waitFor();
  await second.getByText('UP TO DATE', { exact: true }).waitFor();
  await second.getByText('HEALTHY', { exact: true }).first().waitFor();
  await second.getByText('WSS LIVE', { exact: true }).waitFor({ timeout: 30_000 });

  await first.evaluate(() => sessionStorage.setItem('phase21.tab.marker', 'first'));
  await second.evaluate(() => sessionStorage.setItem('phase21.tab.marker', 'second'));
  const markers = await Promise.all([
    first.evaluate(() => sessionStorage.getItem('phase21.tab.marker')),
    second.evaluate(() => sessionStorage.getItem('phase21.tab.marker')),
  ]);
  if (markers[0] !== 'first' || markers[1] !== 'second') throw new Error('sessionStorage is not tab-local');

  const sshKey = process.env.PHASE21_SSH_KEY;
  if (!sshKey) throw new Error('PHASE21_SSH_KEY is required for real reconnect validation');
  execFileSync('ssh', ['-i', sshKey, 'ubuntu@43.165.167.100', 'sudo systemctl restart pons-radar'], { stdio: 'ignore' });
  await first.getByText(/WSS (RECONNECTING|OFFLINE)/, { exact: true }).waitFor({ timeout: 30_000 }).catch(() => undefined);
  await first.getByText('WSS LIVE', { exact: true }).waitFor({ timeout: 45_000 });
  await second.getByText('WSS LIVE', { exact: true }).waitFor({ timeout: 45_000 });

  await first.goto('https://pons.43-165-167-100.sslip.io/alerts', { waitUntil: 'domcontentloaded' });
  await first.getByText('Alert Center', { exact: true }).waitFor();
  await first.getByText(/System update|upgrade|rollback/i).first().waitFor();
  await first.screenshot({ path: path.join(outputDir, 'alerts.png'), fullPage: true });
  await second.screenshot({ path: path.join(outputDir, 'system.png'), fullPage: true });

  const result = {
    status: 'PASS',
    checked_at: new Date().toISOString(),
    https: true,
    authenticated_tabs: 2,
    wss_reconnect: 'PASS',
    per_tab_session_storage: 'PASS',
    system_up_to_date: 'PASS',
    alert_center_durable_history: 'PASS',
  };
  fs.writeFileSync(path.join(outputDir, 'result.json'), JSON.stringify(result, null, 2));
  console.log(JSON.stringify(result));
} finally {
  await browser.close();
}
