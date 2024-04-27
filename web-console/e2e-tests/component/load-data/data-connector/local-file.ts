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

import type * as playwright from 'playwright-chromium';

import { setLabeledInput } from '../../../util/playwright';

import type { DataConnector } from './data-connector';
import { clickApplyButton } from './data-connector';

/**
 * Local file connector for data loader input data.
 */
export class LocalFileDataConnector implements DataConnector {
  readonly name: string;
  readonly needParse: boolean;
  private readonly page: playwright.Page;

  constructor(page: playwright.Page, props: LocalFileDataConnectorProps) {
    Object.assign(this, props);
    this.name = 'Local disk';
    this.needParse = true;
    this.page = page;
  }

  async connect() {
    await setLabeledInput(this.page, 'Base directory', this.baseDirectory);
    await setLabeledInput(this.page, 'File filter', this.fileFilter);
    await clickApplyButton(this.page);
  }
}

interface LocalFileDataConnectorProps {
  readonly baseDirectory: string;
  readonly fileFilter: string;
}

export interface LocalFileDataConnector extends LocalFileDataConnectorProps {}
