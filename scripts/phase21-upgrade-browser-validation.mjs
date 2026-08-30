import fs from 'node:fs';
import path from 'node:path';
import { execFileSync } from 'node:child_process';
import { pathToFileURL } from 'node:url';

const moduleRoot = process.env.PHASE21_PLAYWRIGHT_ROOT;
const credentialFile = process.env.PHASE21_ADMIN_CREDENTIAL_FILE;
const outputDir = process.env.PHASE21_BROWSER_EVIDENCE_DIR;
const sshKey = process.env.PHASE21_SSH_KEY;
if (!moduleRoot || !credentialFile || !outputDir || !sshKey) throw new Error('operator environment is incomplete');
const { chromium } = await import(pathToFileURL(path.join(moduleRoot, 'node_modules/playwright-core/index.mjs')));
const credentials = Object.fromEntries(fs.readFileSync(credentialFile, 'utf8').trim().split(/\r?\n/).map((line) => line.split('=', 2)));
fs.mkdirSync(outputDir, { recursive: true, mode: 0o700 });

const browser = await chromium.launch({ executablePath: process.env.PHASE21_CHROME_PATH, headless: true });
try {
  const context = await browser.newContext({ viewport: { width: 1366, height: 768 } });
  const clean = await context.newPage();
  await clean.goto('https://pons.43-165-167-100.sslip.io', { waitUntil: 'domcontentloaded' });
  await clean.getByLabel('Username').fill(credentials.username);
  await clean.getByLabel('Password').fill(credentials.password);
  await clean.getByRole('button', { name: 'Sign In' }).click();
  await clean.getByText('Dashboard', { exact: true }).first().waitFor();
  await clean.getByText('v0.1.7', { exact: true }).waitFor();

  const dirty = await context.newPage();
  await dirty.goto('https://pons.43-165-167-100.sslip.io/admin', { waitUntil: 'domcontentloaded' });
  await dirty.getByPlaceholder('handle').fill('phase21-unsaved-do-not-submit');

  execFileSync('ssh', ['-i', sshKey, 'ubuntu@43.165.167.100', "sudo env PONS_BASE_URL=https://pons.43-165-167-100.sslip.io PONS_ALLOWED_ORIGIN=https://pons.43-165-167-100.sslip.io PONS_ADMIN_COOKIE_FILE=/root/pons-radar-admin.cookies bash /tmp/phase21-install-update.sh"], { stdio: 'ignore', timeout: 120_000 });

  await clean.getByRole('heading', { name: '系统升级已经完成' }).waitFor({ timeout: 180_000 });
  await dirty.getByRole('heading', { name: '系统升级已经完成' }).waitFor({ timeout: 180_000 });
  await dirty.getByText(/存在未保存修改/).waitFor();
  await dirty.screenshot({ path: path.join(outputDir, 'dirty-admin-refresh-guard.png'), fullPage: true });
  await clean.screenshot({ path: path.join(outputDir, 'clean-dashboard-refresh-modal.png'), fullPage: true });

  await clean.getByText('v0.1.8', { exact: true }).waitFor({ timeout: 45_000 });
  const dirtyBody = await dirty.locator('body').innerText();
  if (!dirtyBody.includes('0.1.7 / stage-refresh-final')) throw new Error('dirty tab was unexpectedly refreshed');
  if (await dirty.getByPlaceholder('handle').inputValue() !== 'phase21-unsaved-do-not-submit') throw new Error('dirty form value was lost');

  const result = {
    status: 'PASS',
    checked_at: new Date().toISOString(),
    old_version: '0.1.7',
    new_version: '0.1.8',
    build_mismatch_modal: 'PASS',
    read_only_auto_refresh: 'PASS',
    dirty_admin_refresh_guard: 'PASS',
    tab_local_refresh_decision: 'PASS',
  };
  fs.writeFileSync(path.join(outputDir, 'result.json'), JSON.stringify(result, null, 2));
  console.log(JSON.stringify(result));
} finally {
  await browser.close();
}
