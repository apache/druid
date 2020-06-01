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

import { ConfigureSchemaConfig } from './config/configure-schema';
import { PartitionConfig } from './config/partition';
import { PublishConfig } from './config/publish';
import { DataConnector } from './data-connector/data-connector';

/**
 * Represents load data tab.
 */
export class DataLoader {
  private readonly baseUrl: string;

  constructor(props: DataLoaderProps) {
    Object.assign(this, props);
    this.baseUrl = props.unifiedConsoleUrl! + '#load-data';
  }

  /**
   * Execute each step to load data.
   */
  async load() {
    await this.page.goto(this.baseUrl);
    await this.start();
    await this.connect(this.connector, this.connectValidator);
    await this.parseData();
    await this.parseTime();
    await this.transform();
    await this.filter();
    await this.configureSchema(this.configureSchemaConfig);
    await this.partition(this.partitionConfig);
    await this.tune();
    await this.publish(this.publishConfig);
    await this.editSpec();
  }

  private async start() {
    await this.page.click(`"${this.connector.name}"`);
    await this.clickButton('Connect data');
  }

  private async connect(connector: DataConnector, validator: (preview: string) => void) {
    await connector.connect();
    await this.validateConnect(validator);
    await this.clickButton('Next: Parse data');
  }

  private async validateConnect(validator: (preview: string) => void) {
    const previewSelector = '.raw-lines';
    await this.page.waitFor(previewSelector);
    const preview = await this.page.$eval(previewSelector, el => (el as HTMLTextAreaElement).value);
    validator(preview!);
  }

  private async parseData() {
    await this.page.waitFor('.parse-data-table');
    await this.clickButton('Next: Parse time');
  }

  private async parseTime() {
    await this.page.waitFor('.parse-time-table');
    await this.clickButton('Next: Transform');
  }

  private async transform() {
    await this.page.waitFor('.transform-table');
    await this.clickButton('Next: Filter');
  }

  private async filter() {
    await this.page.waitFor('.filter-table');
    await this.clickButton('Next: Configure schema');
  }

  private async configureSchema(configureSchemaConfig: ConfigureSchemaConfig) {
    await this.page.waitFor('.schema-table');
    await this.applyConfigureSchemaConfig(configureSchemaConfig);
    await this.clickButton('Next: Partition');
  }

  private async applyConfigureSchemaConfig(configureSchemaConfig: ConfigureSchemaConfig) {
    const rollup = await this.page.$('//*[text()="Rollup"]/input');
    const rollupChecked = await rollup!.evaluate(el => (el as HTMLInputElement).checked);
    if (rollupChecked !== configureSchemaConfig.rollup) {
      await rollup!.click();
      const confirmationDialogSelector = '//*[contains(@class,"bp3-alert-body")]';
      await this.page.waitFor(confirmationDialogSelector);
      await this.clickButton('Yes');
      const statusMessageSelector = '.recipe-toaster';
      await this.page.waitFor(statusMessageSelector);
      await this.page.click(`${statusMessageSelector} button`);
    }
  }

  private async partition(partitionConfig: PartitionConfig) {
    await this.page.waitFor('div.load-data-view.partition');
    await this.applyPartitionConfig(partitionConfig);
    await this.clickButton('Next: Tune');
  }

  private async applyPartitionConfig(partitionConfig: PartitionConfig) {
    const segmentGranularity = await this.page.$(
      '//*[text()="Segment granularity"]/following-sibling::div//input',
    );
    await this.setInput(segmentGranularity!, partitionConfig.segmentGranularity);
  }

  private async tune() {
    await this.page.waitFor('div.load-data-view.tuning');
    await this.clickButton('Next: Publish');
  }

  private async publish(publishConfig: PublishConfig) {
    await this.page.waitFor('div.load-data-view.publish');
    await this.applyPublishConfig(publishConfig);
    await this.clickButton('Edit spec');
  }

  private async applyPublishConfig(publishConfig: PublishConfig) {
    if (publishConfig.datasourceName != null) {
      const datasourceName = await this.page.$(
        '//*[text()="Datasource name"]/following-sibling::div//input',
      );
      await this.setInput(datasourceName!, publishConfig.datasourceName);
    }
  }

  private async editSpec() {
    await this.page.waitFor('div.load-data-view.spec');
    await this.clickButton('Submit');
  }

  private async clickButton(text: string) {
    await this.page.click(`//button/*[contains(text(),"${text}")]`, { waitUntil: 'load' } as any);
  }

  private async setInput(input: playwright.ElementHandle<Element>, value: string) {
    await input.fill('');
    await input.type(value);
  }
}

interface DataLoaderProps {
  readonly page: playwright.Page;
  readonly unifiedConsoleUrl: string;
  readonly connector: DataConnector;
  readonly connectValidator: (preview: string) => void;
  readonly configureSchemaConfig: ConfigureSchemaConfig;
  readonly partitionConfig: PartitionConfig;
  readonly publishConfig: PublishConfig;
}

export interface DataLoader extends DataLoaderProps {}
