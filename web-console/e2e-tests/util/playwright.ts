/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import * as playwright from 'playwright-chromium';

const TRUE = 'true';
const WIDTH = 1250;
const HEIGHT = 760;
const PADDING = 128;

export async function createBrowser(): Promise<playwright.Browser> {
  const headless = process.env['DRUID_E2E_TEST_HEADLESS'] || TRUE;
  const debug = headless !== TRUE;
  const launchOptions: any = {
    args: [`--window-size=${WIDTH},${HEIGHT + PADDING}`, `--disable-local-storage`],
  };
  if (debug) {
    launchOptions.headless = false;
    launchOptions.slowMo = 20;
  }
  return playwright.chromium.launch(launchOptions);
}

export async function createPage(browser: playwright.Browser): Promise<playwright.Page> {
  const context = await browser.newContext();
  const page = await context.newPage();
  await page.setViewportSize({ width: WIDTH, height: HEIGHT });

  // eslint-disable-next-line @typescript-eslint/no-misused-promises
  page.on('response', async response => {
    if (response.status() < 400) return;

    const request = response.request();
    let bodyText: string;
    try {
      bodyText = await response.text();
    } catch (e) {
      bodyText = `Could not get the body of the error message due to: ${e.message}`;
    }

    console.log(`==============================================`);
    console.log(`Request failed on ${request.url()} (with status ${response.status()})`);
    console.log(`Body: ${bodyText}`);
    console.log(`==============================================`);
  });

  return page;
}

export async function getLabeledInput(page: playwright.Page, label: string): Promise<string> {
  return await page.$eval(
    `//*[text()="${label}"]/following-sibling::div//input`,
    el => (el as HTMLInputElement).value,
  );
}

export async function getLabeledTextarea(page: playwright.Page, label: string): Promise<string> {
  return await page.$eval(
    `//*[text()="${label}"]/following-sibling::div//textarea`,
    el => (el as HTMLInputElement).value,
  );
}

export async function setLabeledInput(
  page: playwright.Page,
  label: string,
  value: string,
): Promise<void> {
  return setLabeledElement(page, 'input', label, value);
}

export async function setLabeledTextarea(
  page: playwright.Page,
  label: string,
  value: string,
): Promise<void> {
  return setLabeledElement(page, 'textarea', label, value);
}

async function setLabeledElement(
  page: playwright.Page,
  type: string,
  label: string,
  value: string,
): Promise<void> {
  const element = await page.$(`//*[text()="${label}"]/following-sibling::div//${type}`);
  await setInput(element!, value);
}

export async function setInput(
  input: playwright.ElementHandle<Element>,
  value: string,
): Promise<void> {
  await input.fill('');
  await input.type(value);
}

function buttonSelector(text: string) {
  return `//button/*[contains(text(),"${text}")]`;
}

export async function clickButton(page: playwright.Page, text: string): Promise<void> {
  await page.click(buttonSelector(text));
}

export async function clickLabeledButton(
  page: playwright.Page,
  label: string,
  text: string,
): Promise<void> {
  await page.click(`//*[text()="${label}"]/following-sibling::div${buttonSelector(text)}`);
}

export async function clickText(page: playwright.Page, text: string): Promise<void> {
  await page.click(`//*[text()="${text}"]`);
}

export async function selectSuggestibleInput(
  page: playwright.Page,
  label: string,
  value: string,
): Promise<void> {
  await page.click(`//*[text()="${label}"]/following-sibling::div//button`);
  await page.click(`"${value}"`);
}
