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

import { resolve } from 'path';
import type * as playwright from 'playwright-chromium';

export async function saveScreenshotIfError(
  filenamePrefix: string,
  page: playwright.Page,
  test: () => Promise<void>,
) {
  try {
    await test();
  } catch (e) {
    console.log(`Grabbing error screenshot for: ${filenamePrefix}`);
    const resolvedPath = resolve(filenamePrefix + '-error-screenshot.jpeg');
    try {
      const imageBuffer = await page.screenshot({ path: resolvedPath, type: 'jpeg', quality: 80 });
      console.log(`Image: data:image/jpeg;base64,${imageBuffer.toString('base64')}`);
      console.log(`Written error screenshot to: ${resolvedPath}`);
    } catch (screenshotError) {
      console.log(`Failed to capture screenshot due to: ${screenshotError.message}`);
    }
    throw e;
  }
}
