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

/* eslint-disable max-classes-per-file */

import * as playwright from 'playwright-chromium';

import { getLabeledInput, selectSuggestibleInput, setLabeledInput } from '../../../util/playwright';

/**
 * Possible values for partition step segment granularity.
 */
export enum SegmentGranularity {
  HOUR = 'hour',
  DAY = 'day',
  MONTH = 'month',
  YEAR = 'year',
}

const PARTITIONING_TYPE = 'Partitioning type';

export interface PartitionsSpec {
  readonly type: string;
  apply(page: playwright.Page): Promise<void>;
}

export async function readPartitionSpec(page: playwright.Page): Promise<PartitionsSpec | null> {
  const type = await getLabeledInput(page, PARTITIONING_TYPE);
  switch (type) {
    case HashedPartitionsSpec.TYPE:
      return HashedPartitionsSpec.read(page);
    case SingleDimPartitionsSpec.TYPE:
      return SingleDimPartitionsSpec.read(page);
  }
  return null;
}

export class HashedPartitionsSpec implements PartitionsSpec {
  public static TYPE = 'hashed';
  private static readonly NUM_SHARDS = 'Num shards';

  readonly type: string;

  static async read(page: playwright.Page): Promise<HashedPartitionsSpec> {
    // The shards control may not be visible in that case this is not an error, it is simply not set (null)
    let numShards: number | null = null;
    try {
      numShards = await getLabeledInputAsNumber(page, HashedPartitionsSpec.NUM_SHARDS);
    } catch {}
    return new HashedPartitionsSpec({ numShards });
  }

  constructor(props: HashedPartitionsSpecProps) {
    Object.assign(this, props);
    this.type = HashedPartitionsSpec.TYPE;
  }

  async apply(page: playwright.Page): Promise<void> {
    await setLabeledInput(page, PARTITIONING_TYPE, this.type);
    if (this.numShards != null) {
      await setLabeledInput(page, HashedPartitionsSpec.NUM_SHARDS, String(this.numShards));
    }
  }
}

async function getLabeledInputAsNumber(
  page: playwright.Page,
  label: string,
): Promise<number | null> {
  const valueString = await getLabeledInput(page, label);
  return valueString === '' ? null : Number(valueString);
}

interface HashedPartitionsSpecProps {
  readonly numShards: number | null;
}

export interface HashedPartitionsSpec extends HashedPartitionsSpecProps {}

export class SingleDimPartitionsSpec implements PartitionsSpec {
  public static TYPE = 'single_dim';
  private static readonly PARTITION_DIMENSION = 'Partition dimension';
  private static readonly TARGET_ROWS_PER_SEGMENT = 'Target rows per segment';
  private static readonly MAX_ROWS_PER_SEGMENT = 'Max rows per segment';

  readonly type: string;

  static async read(page: playwright.Page): Promise<SingleDimPartitionsSpec> {
    const partitionDimension = await getLabeledInput(
      page,
      SingleDimPartitionsSpec.PARTITION_DIMENSION,
    );
    const targetRowsPerSegment = await getLabeledInputAsNumber(
      page,
      SingleDimPartitionsSpec.TARGET_ROWS_PER_SEGMENT,
    );
    const maxRowsPerSegment = await getLabeledInputAsNumber(
      page,
      SingleDimPartitionsSpec.MAX_ROWS_PER_SEGMENT,
    );
    return new SingleDimPartitionsSpec({
      partitionDimension,
      targetRowsPerSegment,
      maxRowsPerSegment,
    });
  }

  constructor(props: SingleDimPartitionsSpecProps) {
    Object.assign(this, props);
    this.type = SingleDimPartitionsSpec.TYPE;
  }

  async apply(page: playwright.Page): Promise<void> {
    await selectSuggestibleInput(page, PARTITIONING_TYPE, this.type);
    await setLabeledInput(
      page,
      SingleDimPartitionsSpec.PARTITION_DIMENSION,
      this.partitionDimension,
    );
    if (this.targetRowsPerSegment) {
      await setLabeledInput(
        page,
        SingleDimPartitionsSpec.TARGET_ROWS_PER_SEGMENT,
        String(this.targetRowsPerSegment),
      );
    }
    if (this.maxRowsPerSegment) {
      await setLabeledInput(
        page,
        SingleDimPartitionsSpec.MAX_ROWS_PER_SEGMENT,
        String(this.maxRowsPerSegment),
      );
    }
  }
}

interface SingleDimPartitionsSpecProps {
  readonly partitionDimension: string;
  readonly targetRowsPerSegment: number | null;
  readonly maxRowsPerSegment: number | null;
}

export interface SingleDimPartitionsSpec extends SingleDimPartitionsSpecProps {}

/**
 * Data loader partition step configuration.
 */
export class PartitionConfig {
  constructor(props: PartitionConfigProps) {
    Object.assign(this, props);
  }
}

interface PartitionConfigProps {
  readonly segmentGranularity: SegmentGranularity;
  readonly timeIntervals: string | null;
  readonly partitionsSpec: PartitionsSpec | null;
}

export interface PartitionConfig extends PartitionConfigProps {}
