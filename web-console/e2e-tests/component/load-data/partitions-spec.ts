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

import {
  formGroup,
  getLabeledInput,
  getLabeledTextarea,
  selectSuggestibleInput,
  setLabeledInput,
  setLabeledTextarea,
} from '../../util/playwright';

/**
 * A partitions spec, as set in the form of the data loader's Partition step or of the compaction config dialog.
 */
export type PartitionsSpec =
  | { readonly type: 'hashed'; readonly numShards: number | null }
  | {
      readonly type: 'range';
      readonly partitionDimensions: string[];
      readonly targetRowsPerSegment: number | null;
      readonly maxRowsPerSegment: number | null;
    };

const PARTITIONING_TYPE = 'Partitioning type';
const NUM_SHARDS = 'Num shards';
const PARTITION_DIMENSIONS = 'Partition dimensions';
const TARGET_ROWS_PER_SEGMENT = 'Target rows per segment';
const MAX_ROWS_PER_SEGMENT = 'Max rows per segment';

export async function applyPartitionsSpec(
  page: Page,
  partitionsSpec: PartitionsSpec,
): Promise<void> {
  switch (partitionsSpec.type) {
    case 'hashed':
      await setLabeledInput(page, PARTITIONING_TYPE, partitionsSpec.type);
      if (partitionsSpec.numShards != null) {
        await setLabeledInput(page, NUM_SHARDS, String(partitionsSpec.numShards));
      }
      break;

    case 'range':
      await selectSuggestibleInput(page, PARTITIONING_TYPE, partitionsSpec.type);
      await setLabeledTextarea(
        page,
        PARTITION_DIMENSIONS,
        partitionsSpec.partitionDimensions.join(', '),
      );
      if (partitionsSpec.targetRowsPerSegment != null) {
        await setLabeledInput(
          page,
          TARGET_ROWS_PER_SEGMENT,
          String(partitionsSpec.targetRowsPerSegment),
        );
      }
      if (partitionsSpec.maxRowsPerSegment != null) {
        await setLabeledInput(page, MAX_ROWS_PER_SEGMENT, String(partitionsSpec.maxRowsPerSegment));
      }
      break;
  }
}

export async function readPartitionsSpec(page: Page): Promise<PartitionsSpec | undefined> {
  switch (await getLabeledInput(page, PARTITIONING_TYPE)) {
    case 'hashed':
      return {
        type: 'hashed',
        // The shards field is not always shown, then it is not set
        numShards: (await formGroup(page, NUM_SHARDS).count())
          ? await getLabeledNumber(page, NUM_SHARDS)
          : null,
      };

    case 'range': {
      const partitionDimensions = await getLabeledTextarea(page, PARTITION_DIMENSIONS);
      return {
        type: 'range',
        partitionDimensions: partitionDimensions
          ? partitionDimensions.split(',').map(d => d.trim())
          : [],
        targetRowsPerSegment: await getLabeledNumber(page, TARGET_ROWS_PER_SEGMENT),
        maxRowsPerSegment: await getLabeledNumber(page, MAX_ROWS_PER_SEGMENT),
      };
    }

    default:
      return;
  }
}

async function getLabeledNumber(page: Page, label: string): Promise<number | null> {
  const value = await getLabeledInput(page, label);
  return value === '' ? null : Number(value);
}
