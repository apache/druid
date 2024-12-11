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

import 'regenerator-runtime/runtime';
import './bootstrap/ace';

import { OverlaysProvider } from '@blueprintjs/core';
import { QueryRunner } from 'druid-query-toolkit';
import { createRoot } from 'react-dom/client';

import { bootstrapJsonParse } from './bootstrap/json-parser';
import { bootstrapReactTable } from './bootstrap/react-table-defaults';
import { ConsoleApplication } from './console-application';
import type { QueryContext } from './druid-models';
import type { Links } from './links';
import { setLinkOverrides } from './links';
import { Api, UrlBaser } from './singletons';
import { initMouseTooltip, setLocalStorageNamespace } from './utils';

import './entry.scss';

bootstrapReactTable();
bootstrapJsonParse();

const container = document.getElementsByClassName('app-container')[0];
if (!container) throw new Error('container not found');

interface ConsoleConfig {
  // A custom title for the page
  title?: string;

  // An alternative URL which to use for the stem of an AJAX call
  baseURL?: string;

  // A custom header name/value to set on every AJAX request
  customHeaderName?: string;
  customHeaderValue?: string;

  // A set of custom headers name/value to set on every AJAX request
  customHeaders?: Record<string, string>;

  baseQueryContext?: QueryContext;

  // The query context to set one new query tabs
  defaultQueryContext?: QueryContext;

  // Extra context properties that will be added to all query requests
  mandatoryQueryContext?: QueryContext;

  // The default context that is set by the server
  serverQueryContext?: QueryContext;

  // Allow for link overriding to different docs
  linkOverrides?: Links;

  // Allow for namespacing the local storage in case multiple clusters share a URL due to proxying
  localStorageNamespace?: string;
}

const consoleConfig: ConsoleConfig = (window as any).consoleConfig;
if (typeof consoleConfig.title === 'string') {
  window.document.title = consoleConfig.title;
}

const apiConfig = Api.getDefaultConfig();

if (consoleConfig.baseURL) {
  apiConfig.baseURL = consoleConfig.baseURL;
  UrlBaser.baseUrl = consoleConfig.baseURL;
}
if (consoleConfig.customHeaderName && consoleConfig.customHeaderValue) {
  // The apiConfig.headers is a union type and TS does not like this assigment
  (apiConfig.headers as any)[consoleConfig.customHeaderName] = consoleConfig.customHeaderValue;
}
if (consoleConfig.customHeaders) {
  Object.assign(apiConfig.headers!, consoleConfig.customHeaders);
}

Api.initialize(apiConfig);

if (consoleConfig.linkOverrides) {
  setLinkOverrides(consoleConfig.linkOverrides);
}

if (consoleConfig.localStorageNamespace) {
  setLocalStorageNamespace(consoleConfig.localStorageNamespace);
}

QueryRunner.defaultQueryExecutor = (payload, isSql, cancelToken) => {
  return Api.instance.post(`/druid/v2${isSql ? '/sql' : ''}`, payload, { cancelToken });
};

createRoot(container).render(
  <OverlaysProvider>
    <ConsoleApplication
      baseQueryContext={consoleConfig.baseQueryContext}
      defaultQueryContext={consoleConfig.defaultQueryContext}
      mandatoryQueryContext={consoleConfig.mandatoryQueryContext}
      serverQueryContext={consoleConfig.serverQueryContext}
    />
  </OverlaysProvider>,
);

initMouseTooltip();

// ---------------------------------
// Taken from https://hackernoon.com/removing-that-ugly-focus-ring-and-keeping-it-too-6c8727fefcd2

let mode: 'mouse' | 'tab' = 'mouse';

function handleTab(e: KeyboardEvent) {
  if (e.key !== 'Tab') return;
  if (mode === 'tab') return;
  mode = 'tab';
  document.body.classList.remove('mouse-mode');
  document.body.classList.add('tab-mode');
  window.removeEventListener('keydown', handleTab);
  window.addEventListener('mousedown', handleMouseDown);
}

function handleMouseDown() {
  if (mode === 'mouse') return;
  mode = 'mouse';
  document.body.classList.remove('tab-mode');
  document.body.classList.add('mouse-mode');
  window.removeEventListener('mousedown', handleMouseDown);
  window.addEventListener('keydown', handleTab);
}

window.addEventListener('keydown', handleTab);
