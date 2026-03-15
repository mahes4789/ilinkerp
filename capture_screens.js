const { chromium } = require('playwright');
const path = require('path');
const fs = require('fs');

const BASE_URL = 'http://localhost:5174';
const OUT_DIR = path.join(__dirname, 'screenshots');

if (!fs.existsSync(OUT_DIR)) fs.mkdirSync(OUT_DIR, { recursive: true });

async function capture() {
  const browser = await chromium.launch({ headless: true });
  const context = await browser.newContext({ viewport: { width: 1440, height: 900 } });
  const page = await context.newPage();

  // 1. Login page
  await page.goto(`${BASE_URL}/login`);
  await page.waitForTimeout(1500);
  await page.screenshot({ path: path.join(OUT_DIR, '01_login.png'), fullPage: false });
  console.log('✓ Login page');

  // Login
  await page.fill('input[type="text"], input[name="username"], input[placeholder*="user" i]', 'admin');
  await page.fill('input[type="password"]', 'admin@123');
  await page.click('button[type="submit"]');
  await page.waitForTimeout(2000);

  // 2. Dashboard
  await page.goto(`${BASE_URL}/`);
  await page.waitForTimeout(2000);
  await page.screenshot({ path: path.join(OUT_DIR, '02_dashboard.png'), fullPage: false });
  console.log('✓ Dashboard');

  // 3. ERP Source Wizard - Step 1 (select ERP)
  await page.goto(`${BASE_URL}/sources`);
  await page.waitForTimeout(2000);
  await page.screenshot({ path: path.join(OUT_DIR, '03_erp_source_step1.png'), fullPage: false });
  console.log('✓ ERP Source Step 1');

  // Click "Connect New ERP" or first ERP card to start wizard
  const connectBtn = page.locator('button:has-text("Connect"), button:has-text("Add"), button:has-text("New ERP")').first();
  if (await connectBtn.count() > 0) {
    await connectBtn.click();
    await page.waitForTimeout(1500);
    await page.screenshot({ path: path.join(OUT_DIR, '04_erp_wizard_start.png'), fullPage: false });
    console.log('✓ ERP Wizard launched');
  }

  // Try to pick SAP ERP to advance wizard
  const sapBtn = page.locator('text=SAP, [data-erp="SAP"], button:has-text("SAP")').first();
  if (await sapBtn.count() > 0) {
    await sapBtn.click();
    await page.waitForTimeout(1000);
  }
  // Click Next
  const nextBtn = page.locator('button:has-text("Next"), button:has-text("Continue")').first();
  if (await nextBtn.count() > 0) {
    await nextBtn.click();
    await page.waitForTimeout(1000);
    await page.screenshot({ path: path.join(OUT_DIR, '05_erp_wizard_step2_connect.png'), fullPage: false });
    console.log('✓ ERP Wizard Step 2 (Connect)');
  }

  // 4. ERP Analysis / Comparison page
  await page.goto(`${BASE_URL}/erp-analysis`);
  await page.waitForTimeout(2000);
  await page.screenshot({ path: path.join(OUT_DIR, '06_erp_comparison.png'), fullPage: false });
  console.log('✓ ERP Comparison page');

  // Click "Field Comparison" tab if present
  const fieldTab = page.locator('button:has-text("Field"), [role="tab"]:has-text("Field")').first();
  if (await fieldTab.count() > 0) {
    await fieldTab.click();
    await page.waitForTimeout(1000);
    await page.screenshot({ path: path.join(OUT_DIR, '07_erp_field_comparison.png'), fullPage: false });
    console.log('✓ ERP Field Comparison tab');
  }

  // 5. Settings - Connections tab
  await page.goto(`${BASE_URL}/settings`);
  await page.waitForTimeout(2000);
  await page.screenshot({ path: path.join(OUT_DIR, '08_settings.png'), fullPage: false });
  console.log('✓ Settings page');

  // Click Users tab
  const usersTab = page.locator('button:has-text("User"), [role="tab"]:has-text("User")').first();
  if (await usersTab.count() > 0) {
    await usersTab.click();
    await page.waitForTimeout(1000);
    await page.screenshot({ path: path.join(OUT_DIR, '09_settings_users.png'), fullPage: false });
    console.log('✓ Settings Users tab');
  }

  // 6. AI Query page if exists
  await page.goto(`${BASE_URL}/ai-query`);
  await page.waitForTimeout(1500);
  const title = await page.title();
  if (!title.includes('404') && !title.includes('Not Found')) {
    await page.screenshot({ path: path.join(OUT_DIR, '10_ai_query.png'), fullPage: false });
    console.log('✓ AI Query page');
  }

  await browser.close();
  console.log('\nAll screenshots saved to:', OUT_DIR);
  console.log('Files:', fs.readdirSync(OUT_DIR).join(', '));
}

capture().catch(err => {
  console.error('Error:', err.message);
  process.exit(1);
});
