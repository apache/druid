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

import {
  getLabeledInput,
  getLabeledTextarea,
  selectSuggestibleInput,
  setLabeledInput,
  setLabeledTextarea,
} from '../../../util/playwright';

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
    case RangePartitionsSpec.TYPE:
      return RangePartitionsSpec.read(page);
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

async function getLabeledTextareaAsArray(page: playwright.Page, label: string): Promise<string[]> {
  const valueString = await getLabeledTextarea(page, label);
  return valueString === '' ? [] : valueString.split(',').map(v => v.trim());
}

interface HashedPartitionsSpecProps {
  readonly numShards: number | null;
}

export interface HashedPartitionsSpec extends HashedPartitionsSpecProps {}

export class RangePartitionsSpec implements PartitionsSpec {
  public static TYPE = 'range';
  private static readonly PARTITION_DIMENSIONS = 'Partition dimensions';
  private static readonly TARGET_ROWS_PER_SEGMENT = 'Target rows per segment';
  private static readonly MAX_ROWS_PER_SEGMENT = 'Max rows per segment';

  readonly type: string;

  static async read(page: playwright.Page): Promise<RangePartitionsSpec> {
    const partitionDimensions = await getLabeledTextareaAsArray(
      page,
      RangePartitionsSpec.PARTITION_DIMENSIONS,
    );
    const targetRowsPerSegment = await getLabeledInputAsNumber(
      page,
      RangePartitionsSpec.TARGET_ROWS_PER_SEGMENT,
    );
    const maxRowsPerSegment = await getLabeledInputAsNumber(
      page,
      RangePartitionsSpec.MAX_ROWS_PER_SEGMENT,
    );
    return new RangePartitionsSpec({
      partitionDimensions,
      targetRowsPerSegment,
      maxRowsPerSegment,
    });
  }

  constructor(props: RangePartitionsSpecProps) {
    Object.assign(this, props);
    this.type = RangePartitionsSpec.TYPE;
  }

  async apply(page: playwright.Page): Promise<void> {
    await selectSuggestibleInput(page, PARTITIONING_TYPE, this.type);
    await setLabeledTextarea(
      page,
      RangePartitionsSpec.PARTITION_DIMENSIONS,
      this.partitionDimensions.join(', '),
    );
    if (this.targetRowsPerSegment) {
      await setLabeledInput(
        page,
        RangePartitionsSpec.TARGET_ROWS_PER_SEGMENT,
        String(this.targetRowsPerSegment),
      );
    }
    if (this.maxRowsPerSegment) {
      await setLabeledInput(
        page,
        RangePartitionsSpec.MAX_ROWS_PER_SEGMENT,
        String(this.maxRowsPerSegment),
      );
    }
  }
}

interface RangePartitionsSpecProps {
  readonly partitionDimensions: string[];
  readonly targetRowsPerSegment: number | null;
  readonly maxRowsPerSegment: number | null;
}

export interface RangePartitionsSpec extends RangePartitionsSpecProps {}

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
