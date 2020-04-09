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

import * as playwright from 'playwright-core';

const DEBUG = true;
const WIDTH = 1920;
const HEIGHT = 1080;
const PADDING = 128;

export async function createBrowserNormal(): Promise<playwright.Browser> {
  return createBrowserInternal(!DEBUG);
}

export async function createBrowserDebug(): Promise<playwright.Browser> {
  return createBrowserInternal(DEBUG);
}

async function createBrowserInternal(debug: boolean): Promise<playwright.Browser> {
  const launchOptions: any = {
    args: [`--window-size=${WIDTH},${HEIGHT + PADDING}`],
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
  return page;
}
