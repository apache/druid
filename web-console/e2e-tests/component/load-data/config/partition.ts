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

import { selectSuggestibleInput } from '../../../util/playwright';
import { setLabeledInput } from '../../../util/playwright';

/* tslint:disable max-classes-per-file */

/**
 * Possible values for partition step segment granularity.
 */
export enum SegmentGranularity {
  HOUR = 'HOUR',
  DAY = 'DAY',
  MONTH = 'MONTH',
  YEAR = 'YEAR',
}

const PARTITIONING_TYPE = 'Partitioning type';

export interface PartitionsSpec {
  readonly type: string;
  apply(page: playwright.Page): Promise<void>;
}

export class HashedPartitionsSpec implements PartitionsSpec {
  readonly type: string;

  constructor(props: HashedPartitionsSpecProps) {
    Object.assign(this, props);
    this.type = 'hashed';
  }

  async apply(page: playwright.Page): Promise<void> {
    await setLabeledInput(page, PARTITIONING_TYPE, this.type);
    if (this.numShards != null) {
      await setLabeledInput(page, 'Num shards', String(this.numShards));
    }
  }
}

interface HashedPartitionsSpecProps {
  readonly numShards: number | null;
}

export interface HashedPartitionsSpec extends HashedPartitionsSpecProps {}

export class SingleDimPartitionsSpec implements PartitionsSpec {
  readonly type: string;

  constructor(props: SingleDimPartitionsSpecProps) {
    Object.assign(this, props);
    this.type = 'single_dim';
  }

  async apply(page: playwright.Page): Promise<void> {
    await selectSuggestibleInput(page, PARTITIONING_TYPE, this.type);
    await setLabeledInput(page, 'Partition dimension', this.partitionDimension);
    if (this.targetRowsPerSegment) {
      await setLabeledInput(page, 'Target rows per segment', String(this.targetRowsPerSegment));
    }
    if (this.maxRowsPerSegment) {
      await setLabeledInput(page, 'Max rows per segment', String(this.maxRowsPerSegment));
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
  readonly forceGuaranteedRollupText: string;

  constructor(props: PartitionConfigProps) {
    Object.assign(this, props);
    this.forceGuaranteedRollupText = this.forceGuaranteedRollup ? 'True' : 'False';
  }
}

interface PartitionConfigProps {
  readonly segmentGranularity: SegmentGranularity;
  readonly timeIntervals: string | null;
  readonly forceGuaranteedRollup: boolean | null;
  readonly partitionsSpec: PartitionsSpec | null;
}

export interface PartitionConfig extends PartitionConfigProps {}
