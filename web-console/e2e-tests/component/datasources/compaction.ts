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

/* tslint:disable max-classes-per-file */

const PARTITIONING_TYPE = 'Partitioning type';

interface CompactionPartitionsSpec {
  readonly type: string;
  apply(page: playwright.Page): Promise<void>;
}

export class CompactionHashPartitionsSpec implements CompactionPartitionsSpec {
  readonly type: string;

  constructor(props: CompactionHashPartitionsSpecProps) {
    Object.assign(this, props);
    this.type = 'hashed';
  }

  async apply(page: playwright.Page): Promise<void> {
    await setInput(page, PARTITIONING_TYPE, this.type);
    if (this.numShards != null) {
      await setInput(page, 'Num shards', String(this.numShards));
    }
  }
}

async function setInput(page: playwright.Page, label: string, value: string): Promise<void> {
  const input = await page.$(`//*[text()="${label}"]/following-sibling::div//input`);
  await input!.fill('');
  await input!.type(value);
}

interface CompactionHashPartitionsSpecProps {
  readonly numShards: number | null;
}

export interface CompactionHashPartitionsSpec extends CompactionHashPartitionsSpecProps {}

/**
 * Datasource compaction configuration
 */
export class CompactionConfig {
  constructor(props: CompactionConfigProps) {
    Object.assign(this, props);
  }
}

interface CompactionConfigProps {
  readonly skipOffsetFromLatest: string;
  readonly partitionsSpec: CompactionPartitionsSpec;
}

export interface CompactionConfig extends CompactionConfigProps {}
