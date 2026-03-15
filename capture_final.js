const { chromium } = require('playwright');
const path = require('path');
const fs = require('fs');
const OUT = path.join(__dirname, 'screenshots');
if (!fs.existsSync(OUT)) fs.mkdirSync(OUT, { recursive: true });

const BASE = 'http://localhost:5174';

(async () => {
  const browser = await chromium.launch({ headless: true });
  const ctx = await browser.newContext({ viewport: { width: 1440, height: 900 } });
  const page = await ctx.newPage();
  page.setDefaultNavigationTimeout(30000);
  page.setDefaultTimeout(20000);

  // 1. Login page
  await page.goto(`${BASE}/login`, { waitUntil: 'load' });
  await page.waitForTimeout(1500);
  await page.screenshot({ path: path.join(OUT, '01_login.png') });
  console.log('✓ 01 Login page');

  // Perform login
  const inputs = await page.locator('input').all();
  for (const inp of inputs) {
    const type = await inp.getAttribute('type');
    if (type === 'password') await inp.fill('admin@123');
    else await inp.fill('admin');
  }
  await page.locator('button[type=submit]').click();
  await page.waitForTimeout(2500);

  // 2. Dashboard
  await page.screenshot({ path: path.join(OUT, '02_dashboard.png') });
  console.log('✓ 02 Dashboard');

  // 3. ERP Source page
  await page.goto(`${BASE}/erp-source`, { waitUntil: 'load' });
  await page.waitForTimeout(1800);
  await page.screenshot({ path: path.join(OUT, '03_erp_source.png') });
  console.log('✓ 03 ERP Source page');

  // 4. Open wizard
  const wizBtn = page.locator('button').filter({ hasText: /Start.*Wizard|Start New ERP/ }).first();
  if (await wizBtn.count() > 0) {
    await wizBtn.click();
    await page.waitForTimeout(1500);
    await page.screenshot({ path: path.join(OUT, '04_wizard_step1.png') });
    console.log('✓ 04 Wizard Step 1 - ERP grid');

    // Select Oracle Fusion
    const oraBtn = page.locator('button').filter({ hasText: 'Oracle Fusion' }).first();
    if (await oraBtn.count() > 0) {
      await oraBtn.click();
      await page.waitForTimeout(600);
      await page.screenshot({ path: path.join(OUT, '04b_wizard_erp_selected.png') });
      console.log('✓ 04b Wizard - ERP selected');
    }
  }

  // 5. ERP Comparison page
  await page.goto(`${BASE}/erp-comparison`, { waitUntil: 'load' });
  await page.waitForTimeout(2000);
  await page.screenshot({ path: path.join(OUT, '06_erp_comparison.png') });
  console.log('✓ 06 ERP Comparison');

  // Click any "Field" tab
  try {
    const fTab = page.locator('button, [role=tab]').filter({ hasText: /Field/i }).first();
    if (await fTab.count() > 0) {
      await fTab.click();
      await page.waitForTimeout(1000);
      await page.screenshot({ path: path.join(OUT, '07_erp_field_tab.png') });
      console.log('✓ 07 ERP Field Comparison tab');
    }
  } catch(e) { console.log('  (no field tab found)'); }

  // 6. Settings
  await page.goto(`${BASE}/settings`, { waitUntil: 'load' });
  await page.waitForTimeout(1800);
  await page.screenshot({ path: path.join(OUT, '08_settings.png') });
  console.log('✓ 08 Settings - Connections');

  try {
    const uTab = page.locator('button, [role=tab]').filter({ hasText: /User/i }).first();
    if (await uTab.count() > 0) {
      await uTab.click();
      await page.waitForTimeout(1000);
      await page.screenshot({ path: path.join(OUT, '09_settings_users.png') });
      console.log('✓ 09 Settings - Users tab');
    }
  } catch(e) { console.log('  (no users tab found)'); }

  await browser.close();
  const files = fs.readdirSync(OUT).filter(f => f.endsWith('.png')).sort();
  console.log('\nDone. Captured files:');
  files.forEach(f => console.log('  ', f));
})().catch(err => {
  console.error('ERROR:', err.message);
  process.exit(1);
});
