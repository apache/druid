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

import { clickButton, setLabeledInput, setLabeledTextarea } from '../../util/playwright';

import type { ConfigureSchemaConfig } from './config/configure-schema';
import type { ConfigureTimestampConfig } from './config/configure-timestamp';
import type { PartitionConfig } from './config/partition';
import type { PublishConfig } from './config/publish';
import type { DataConnector } from './data-connector/data-connector';

/**
 * Represents load data tab.
 */
export class DataLoader {
  private readonly baseUrl: string;

  constructor(props: DataLoaderProps) {
    Object.assign(this, props);
    this.baseUrl = props.unifiedConsoleUrl + '#data-loader';
  }

  /**
   * Execute each step to load data.
   */
  async load() {
    await this.page.goto(this.baseUrl);
    await this.startNewSpecIfNeeded();
    await this.start();
    await this.connect(this.connector, this.connectValidator);
    if (this.connector.needParse) {
      await this.parseData();
      await this.parseTime(this.configureTimestampConfig);
    }
    await this.transform();
    await this.filter();
    await this.configureSchema(this.configureSchemaConfig);
    await this.partition(this.partitionConfig);
    await this.tune();
    await this.publish(this.publishConfig);
    await this.editSpec();
  }

  private async startNewSpecIfNeeded() {
    const startNewSpecLocator = this.page.locator(`//*[contains(text(),"Start a new")]`);
    if (await startNewSpecLocator.count()) {
      await startNewSpecLocator.click();
    }
  }

  private async start() {
    const cardSelector = `//*[contains(@class,"bp4-card")][p[contains(text(),"${this.connector.name}")]]`;
    await this.page.click(cardSelector);
    await clickButton(this.page, 'Connect data');
  }

  private async connect(connector: DataConnector, validator: (preview: string) => void) {
    await connector.connect();
    await this.validateConnect(validator);
    const next = this.connector.needParse ? 'Parse data' : 'Transform';
    await clickButton(this.page, `Next: ${next}`);
  }

  private async validateConnect(validator: (preview: string) => void) {
    const previewSelector = '.raw-lines';
    await this.page.waitForSelector(previewSelector);
    const preview = await this.page.$eval(previewSelector, el => (el as HTMLTextAreaElement).value);
    validator(preview);
  }

  private async parseData() {
    await this.page.waitForSelector('.parse-data-table');
    await clickButton(this.page, 'Next: Parse time');
  }

  private async parseTime(configureTimestampConfig?: ConfigureTimestampConfig) {
    await this.page.waitForSelector('.parse-time-table');
    if (configureTimestampConfig) {
      await this.applyConfigureTimestampConfig(configureTimestampConfig);
    }
    await clickButton(this.page, 'Next: Transform');
  }

  private async transform() {
    await this.page.waitForSelector('.transform-table');
    await clickButton(this.page, 'Next: Filter');
  }

  private async filter() {
    await this.page.waitForSelector('.filter-table');
    await clickButton(this.page, 'Next: Configure schema');
  }

  private async configureSchema(configureSchemaConfig: ConfigureSchemaConfig) {
    await this.page.waitForSelector('.schema-table');
    await this.applyConfigureSchemaConfig(configureSchemaConfig);
    await clickButton(this.page, 'Next: Partition');
  }

  private async applyConfigureTimestampConfig(configureTimestampConfig: ConfigureTimestampConfig) {
    await clickButton(this.page, 'Expression');
    await setLabeledInput(this.page, 'Expression', configureTimestampConfig.timestampExpression);
    await clickButton(this.page, 'Apply');
  }

  private async applyConfigureSchemaConfig(configureSchemaConfig: ConfigureSchemaConfig) {
    const rollupSelector = '//*[text()="Rollup"]';
    const rollupInput = await this.page.$(`${rollupSelector}/input`);
    const rollupChecked = await rollupInput!.evaluate(el => (el as HTMLInputElement).checked);
    if (rollupChecked !== configureSchemaConfig.rollup) {
      await this.page.click(rollupSelector);
      const confirmationDialogSelector = '//*[contains(@class,"bp4-alert-body")]';
      await this.page.waitForSelector(confirmationDialogSelector);
      await clickButton(this.page, 'Yes');
      const statusMessageSelector = '.recipe-toaster';
      await this.page.waitForSelector(statusMessageSelector);
      await this.page.click(`${statusMessageSelector} button`);
    }
  }

  private async partition(partitionConfig: PartitionConfig) {
    await this.page.waitForSelector('div.load-data-view.partition');
    await this.applyPartitionConfig(partitionConfig);
    await clickButton(this.page, 'Next: Tune');
  }

  private async applyPartitionConfig(partitionConfig: PartitionConfig) {
    await setLabeledInput(this.page, 'Segment granularity', partitionConfig.segmentGranularity);
    if (partitionConfig.timeIntervals) {
      await setLabeledTextarea(this.page, 'Time intervals', partitionConfig.timeIntervals);
    }
    if (partitionConfig.partitionsSpec != null) {
      await partitionConfig.partitionsSpec.apply(this.page);
    }
  }

  private async tune() {
    await this.page.waitForSelector('div.load-data-view.tuning');
    await clickButton(this.page, 'Next: Publish');
  }

  private async publish(publishConfig: PublishConfig) {
    await this.page.waitForSelector('div.load-data-view.publish');
    await this.applyPublishConfig(publishConfig);
    await clickButton(this.page, 'Edit spec');
  }

  private async applyPublishConfig(publishConfig: PublishConfig) {
    if (publishConfig.datasourceName != null) {
      await setLabeledInput(this.page, 'Datasource name', publishConfig.datasourceName);
    }
  }

  private async editSpec() {
    await this.page.waitForSelector('div.load-data-view.spec');
    await clickButton(this.page, 'Submit');
  }
}

interface DataLoaderProps {
  readonly page: playwright.Page;
  readonly unifiedConsoleUrl: string;
  readonly connector: DataConnector;
  readonly connectValidator: (preview: string) => void;
  readonly configureTimestampConfig?: ConfigureTimestampConfig;
  readonly configureSchemaConfig: ConfigureSchemaConfig;
  readonly partitionConfig: PartitionConfig;
  readonly publishConfig: PublishConfig;
}

export interface DataLoader extends DataLoaderProps {}
