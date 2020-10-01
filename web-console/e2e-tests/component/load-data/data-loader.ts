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

import { clickButton } from '../../util/playwright';
import { clickLabeledButton } from '../../util/playwright';
import { setLabeledInput } from '../../util/playwright';
import { setLabeledTextarea } from '../../util/playwright';

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
    if (this.connector.needParse) {
      await this.parseData();
      await this.parseTime();
    }
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
    await this.page.waitFor(previewSelector);
    const preview = await this.page.$eval(previewSelector, el => (el as HTMLTextAreaElement).value);
    validator(preview!);
  }

  private async parseData() {
    await this.page.waitFor('.parse-data-table');
    await clickButton(this.page, 'Next: Parse time');
  }

  private async parseTime() {
    await this.page.waitFor('.parse-time-table');
    await clickButton(this.page, 'Next: Transform');
  }

  private async transform() {
    await this.page.waitFor('.transform-table');
    await clickButton(this.page, 'Next: Filter');
  }

  private async filter() {
    await this.page.waitFor('.filter-table');
    await clickButton(this.page, 'Next: Configure schema');
  }

  private async configureSchema(configureSchemaConfig: ConfigureSchemaConfig) {
    await this.page.waitFor('.schema-table');
    await this.applyConfigureSchemaConfig(configureSchemaConfig);
    await clickButton(this.page, 'Next: Partition');
  }

  private async applyConfigureSchemaConfig(configureSchemaConfig: ConfigureSchemaConfig) {
    const rollup = await this.page.$('//*[text()="Rollup"]/input');
    const rollupChecked = await rollup!.evaluate(el => (el as HTMLInputElement).checked);
    if (rollupChecked !== configureSchemaConfig.rollup) {
      await rollup!.click();
      const confirmationDialogSelector = '//*[contains(@class,"bp3-alert-body")]';
      await this.page.waitFor(confirmationDialogSelector);
      await clickButton(this.page, 'Yes');
      const statusMessageSelector = '.recipe-toaster';
      await this.page.waitFor(statusMessageSelector);
      await this.page.click(`${statusMessageSelector} button`);
    }
  }

  private async partition(partitionConfig: PartitionConfig) {
    await this.page.waitFor('div.load-data-view.partition');
    await this.applyPartitionConfig(partitionConfig);
    await clickButton(this.page, 'Next: Tune');
  }

  private async applyPartitionConfig(partitionConfig: PartitionConfig) {
    await setLabeledInput(this.page, 'Segment granularity', partitionConfig.segmentGranularity);
    if (partitionConfig.forceGuaranteedRollup) {
      await clickLabeledButton(
        this.page,
        'Force guaranteed rollup',
        partitionConfig.forceGuaranteedRollupText,
      );
      await setLabeledTextarea(this.page, 'Time intervals', partitionConfig.timeIntervals!);
    }
    if (partitionConfig.partitionsSpec != null) {
      await partitionConfig.partitionsSpec.apply(this.page);
    }
  }

  private async tune() {
    await this.page.waitFor('div.load-data-view.tuning');
    await clickButton(this.page, 'Next: Publish');
  }

  private async publish(publishConfig: PublishConfig) {
    await this.page.waitFor('div.load-data-view.publish');
    await this.applyPublishConfig(publishConfig);
    await clickButton(this.page, 'Edit spec');
  }

  private async applyPublishConfig(publishConfig: PublishConfig) {
    if (publishConfig.datasourceName != null) {
      await setLabeledInput(this.page, 'Datasource name', publishConfig.datasourceName);
    }
  }

  private async editSpec() {
    await this.page.waitFor('div.load-data-view.spec');
    await clickButton(this.page, 'Submit');
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
