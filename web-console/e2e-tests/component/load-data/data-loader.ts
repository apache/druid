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

import type { Page } from '@playwright/test';

import { clickButton, openView, setLabeledInput, setLabeledTextarea } from '../../util/playwright';

import type { ConfigureSchemaConfig } from './config/configure-schema';
import type { ConfigureTimestampConfig } from './config/configure-timestamp';
import type { PartitionConfig } from './config/partition';
import type { PublishConfig } from './config/publish';
import type { DataConnector } from './data-connector/data-connector';

/**
 * Represents load data tab.
 */
export class DataLoader {
  constructor(props: DataLoaderProps) {
    Object.assign(this, props);
  }

  /**
   * Execute each step to load data.
   */
  async load() {
    await openView(this.page, 'data-loader');
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

  private async start() {
    await this.page
      .locator('.bp6-card')
      .filter({ has: this.page.locator('p', { hasText: this.connector.name }) })
      .click();
    await clickButton(this.page, 'Connect data');
  }

  private async clickNext(step: string) {
    await clickButton(this.page.locator('.next-bar'), `Next: ${step}`);
  }

  private async connect(connector: DataConnector, validator: (previewLines: string[]) => void) {
    await connector.connect();
    await this.validateConnect(validator);
    await this.clickNext(this.connector.needParse ? 'Parse data' : 'Transform');
  }

  private async validateConnect(validator: (previewLines: string[]) => void) {
    const rawLines = this.page.locator('.raw-lines .raw-line');
    await rawLines.first().waitFor();
    validator(await rawLines.allTextContents());
  }

  private async parseData() {
    await this.page.locator('.parse-data-table').waitFor();
    await this.clickNext('Parse time');
  }

  private async parseTime(configureTimestampConfig?: ConfigureTimestampConfig) {
    await this.page.locator('.parse-time-table').waitFor();
    if (configureTimestampConfig) {
      await this.applyConfigureTimestampConfig(configureTimestampConfig);
    }
    await this.clickNext('Transform');
  }

  private async transform() {
    await this.page.locator('.transform-table').waitFor();
    await this.clickNext('Filter');
  }

  private async filter() {
    await this.page.locator('.filter-table').waitFor();
    await this.clickNext('Configure schema');
  }

  private async configureSchema(configureSchemaConfig: ConfigureSchemaConfig) {
    await this.page.locator('.schema-table').waitFor();
    await this.applyConfigureSchemaConfig(configureSchemaConfig);
    await this.clickNext('Partition');
  }

  private async applyConfigureTimestampConfig(configureTimestampConfig: ConfigureTimestampConfig) {
    await clickButton(this.page, 'Expression');
    await setLabeledInput(this.page, 'Expression', configureTimestampConfig.timestampExpression);
    await clickButton(this.page, 'Apply');
  }

  private async applyConfigureSchemaConfig(configureSchemaConfig: ConfigureSchemaConfig) {
    const { rollup } = configureSchemaConfig;
    const rollupSwitch = this.page.getByLabel('Rollup', { exact: true });
    if ((await rollupSwitch.isChecked()) !== rollup) {
      // The switch's input is visually hidden, so click its label (which asks for confirmation)
      await this.page.locator('label', { has: rollupSwitch }).click();
      await clickButton(
        this.page.locator('.bp6-alert'),
        `Yes - ${rollup ? 'enable' : 'disable'} rollup`,
      );
      await this.page.locator('.recipe-toaster').getByRole('button').click();
    }
  }

  private async partition(partitionConfig: PartitionConfig) {
    await this.page.locator('.load-data-view.partition').waitFor();
    await this.applyPartitionConfig(partitionConfig);
    await this.clickNext('Tune');
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
    await this.page.locator('.load-data-view.tuning').waitFor();
    await this.clickNext('Publish');
  }

  private async publish(publishConfig: PublishConfig) {
    await this.page.locator('.load-data-view.publish').waitFor();
    await this.applyPublishConfig(publishConfig);
    await this.clickNext('Edit spec');
  }

  private async applyPublishConfig(publishConfig: PublishConfig) {
    if (publishConfig.datasourceName != null) {
      await setLabeledInput(this.page, 'Datasource name', publishConfig.datasourceName);
    }
  }

  private async editSpec() {
    await this.page.locator('.load-data-view.spec').waitFor();
    await clickButton(this.page.locator('.next-bar'), 'Submit task');
  }
}

interface DataLoaderProps {
  readonly page: Page;
  readonly connector: DataConnector;
  readonly connectValidator: (previewLines: string[]) => void;
  readonly configureTimestampConfig?: ConfigureTimestampConfig;
  readonly configureSchemaConfig: ConfigureSchemaConfig;
  readonly partitionConfig: PartitionConfig;
  readonly publishConfig: PublishConfig;
}

export interface DataLoader extends DataLoaderProps {}
