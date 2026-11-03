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

import type { Locator, Page } from '@playwright/test';

const CONSOLE_PATH = '/unified-console.html';

/**
 * Opens a view of the console (`view` is its hash route, like 'datasources'), optionally with its table filtered to
 * the rows where each `filter` column equals the given value. If the console is already open it is reloaded, so that
 * the view shows fresh data.
 */
export async function openView(
  page: Page,
  view: string,
  filter?: Record<string, string>,
): Promise<void> {
  // Encoded like TableFilters.eq(filter).toString() in src/utils/table-filters (which can't be imported here as it
  // brings in the whole of src/utils)
  const filterParam = Object.entries(filter ?? {})
    .map(
      ([column, value]) =>
        `${column}=${value.replace(/[\\|]/g, '\\$&').replace(/[#%&/?]/g, encodeURIComponent)}`,
    )
    .join('&');
  const wasOpen = page.url().includes(CONSOLE_PATH);
  await page.goto(`${CONSOLE_PATH}#${view}${filterParam ? `/${filterParam}` : ''}`);
  if (wasOpen) await page.reload();
}

function escapeRegExp(text: string): string {
  return text.replace(/[$()*+.?[\\\]^{|}]/g, '\\$&');
}

/**
 * The form group (a Blueprint FormGroup, as rendered by AutoForm and FormGroupWithInfo) labeled `label`. The labels
 * are not linked to their inputs, so `getByLabel` can not find them.
 */
export function formGroup(scope: Page | Locator, label: string): Locator {
  // The `has` locator is matched relative to each form group, so it must not start from `scope`
  const page = 'page' in scope ? scope.page() : scope;
  return scope.locator('.bp6-form-group').filter({
    has: page.locator(':scope > .bp6-label', {
      hasText: new RegExp(`^\\s*${escapeRegExp(label)}\\s*$`),
    }),
  });
}

export function labeledInput(scope: Page | Locator, label: string): Locator {
  return formGroup(scope, label).locator('input');
}

export function labeledTextarea(scope: Page | Locator, label: string): Locator {
  return formGroup(scope, label).locator('textarea');
}

/**
 * Sets a labeled boolean field of an AutoForm (its False / True buttons).
 */
export async function setLabeledBoolean(
  scope: Page | Locator,
  label: string,
  value: boolean,
): Promise<void> {
  await formGroup(scope, label)
    .getByText(value ? 'True' : 'False', { exact: true })
    .click();
}

export async function getLabeledInput(scope: Page | Locator, label: string): Promise<string> {
  return await labeledInput(scope, label).inputValue();
}

export async function getLabeledTextarea(scope: Page | Locator, label: string): Promise<string> {
  return await labeledTextarea(scope, label).inputValue();
}

export async function setLabeledInput(
  scope: Page | Locator,
  label: string,
  value: string,
): Promise<void> {
  await labeledInput(scope, label).fill(value);
}

export async function setLabeledTextarea(
  scope: Page | Locator,
  label: string,
  value: string,
): Promise<void> {
  await labeledTextarea(scope, label).fill(value);
}

/**
 * Picks `value` from the suggestions of a labeled SuggestibleInput.
 */
export async function selectSuggestibleInput(
  page: Page,
  label: string,
  value: string,
): Promise<void> {
  await formGroup(page, label).getByRole('button').click();
  await page.getByRole('menuitem', { name: value, exact: true }).click();
}

export async function setQueryInput(page: Page, value: string): Promise<void> {
  // The query input is a CodeMirror editor, its editable surface is a contenteditable div
  const input = page.locator('.flexible-query-input .cm-content');
  await input.fill(value);
  // Closes the autocomplete that typing opens, which would cover the query (and its results)
  await input.press('Escape');
}

export function button(scope: Page | Locator, text: string): Locator {
  return scope.getByRole('button', { name: text, exact: true });
}

export async function clickButton(scope: Page | Locator, text: string): Promise<void> {
  await button(scope, text).click();
}

export async function clickMenuItem(page: Page, text: string): Promise<void> {
  // Not exact: a menu item's label (like "(debug)") is part of its name
  await page.getByRole('menuitem', { name: text }).click();
}
