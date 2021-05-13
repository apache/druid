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

import { setLabeledInput } from '../../../util/playwright';

import { clickApplyButton, DataConnector } from './data-connector';

/**
 * Reindexing connector for data loader input data.
 */
export class ReindexDataConnector implements DataConnector {
  readonly name: string;
  readonly needParse: boolean;
  private readonly page: playwright.Page;

  constructor(page: playwright.Page, props: ReindexDataConnectorProps) {
    Object.assign(this, props);
    this.name = 'Reindex from Druid';
    this.needParse = false;
    this.page = page;
  }

  async connect() {
    await setLabeledInput(this.page, 'Datasource', this.datasourceName);
    await setLabeledInput(this.page, 'Interval', this.interval);
    await clickApplyButton(this.page);
  }
}

interface ReindexDataConnectorProps {
  readonly datasourceName: string;
  readonly interval: string;
}

export interface ReindexDataConnector extends ReindexDataConnectorProps {}
