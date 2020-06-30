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

import axios from 'axios';
import 'brace'; // Import Ace editor and all the sub components used in the app
import 'brace/ext/language_tools';
import 'brace/theme/solarized_dark';
import 'core-js/stable';
import React from 'react';
import ReactDOM from 'react-dom';
import 'regenerator-runtime/runtime';

import './ace-modes/dsql';
import './ace-modes/hjson';
import './bootstrap/react-table-defaults';
import { ConsoleApplication } from './console-application';
import { Links, setLinkOverrides } from './links';
import { UrlBaser } from './singletons/url-baser';

import './entry.scss';

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

  // The URL for where to load the example manifest, a JSON document that tells the console where to find all the example datasets
  exampleManifestsUrl?: string;

  // The query context to set if the user does not have one saved in local storage, defaults to {}
  defaultQueryContext?: Record<string, any>;

  // Extra context properties that will be added to all query requests
  mandatoryQueryContext?: Record<string, any>;

  // Allow for link overriding to different docs
  linkOverrides?: Links;
}

const consoleConfig: ConsoleConfig = (window as any).consoleConfig;
if (typeof consoleConfig.title === 'string') {
  window.document.title = consoleConfig.title;
}

if (consoleConfig.baseURL) {
  axios.defaults.baseURL = consoleConfig.baseURL;
  UrlBaser.baseUrl = consoleConfig.baseURL;
}
if (consoleConfig.customHeaderName && consoleConfig.customHeaderValue) {
  axios.defaults.headers.common[consoleConfig.customHeaderName] = consoleConfig.customHeaderValue;
}
if (consoleConfig.customHeaders) {
  Object.assign(axios.defaults.headers, consoleConfig.customHeaders);
}
if (consoleConfig.linkOverrides) {
  setLinkOverrides(consoleConfig.linkOverrides);
}

ReactDOM.render(
  React.createElement(ConsoleApplication, {
    exampleManifestsUrl: consoleConfig.exampleManifestsUrl,
    defaultQueryContext: consoleConfig.defaultQueryContext,
    mandatoryQueryContext: consoleConfig.mandatoryQueryContext,
  }) as any,
  container,
);

// ---------------------------------
// Taken from https://hackernoon.com/removing-that-ugly-focus-ring-and-keeping-it-too-6c8727fefcd2

let mode: 'mouse' | 'tab' = 'mouse';

function handleTab(e: KeyboardEvent) {
  if (e.keyCode !== 9) return;
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
