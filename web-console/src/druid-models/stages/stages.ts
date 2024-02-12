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

import { max, sum } from 'd3-array';

import { deleteKeys, filterMap, oneOf, zeroDivide } from '../../utils';
import type { InputFormat } from '../input-format/input-format';
import type { InputSource } from '../input-source/input-source';

const SORT_WEIGHT = 0.5;
const READING_INPUT_WITH_SORT_WEIGHT = 1 - SORT_WEIGHT;

export type StageInput =
  | {
      type: 'stage';
      stage: number;
    }
  | {
      type: 'table';
      dataSource: string;
      intervals: string[];
      filter?: any;
      filterFields?: string[];
    }
  | {
      type: 'external';
      inputSource: InputSource;
      inputFormat: InputFormat;
      signature: any[];
    };

export interface StageDefinition {
  stageNumber: number;
  definition: {
    id: string;
    input: StageInput[];
    broadcast?: number[];
    processor: {
      type: string;
      [k: string]: any;
    };
    signature: any;
    shuffleSpec?: {
      type: string;
      clusterBy?: ClusterBy;
      targetSize?: number;
      partitions?: number;
      aggregate?: boolean;
    };
    maxWorkerCount: number;
    shuffleCheckHasMultipleValues?: boolean;
    maxInputBytesPerWorker?: number;
  };
  phase?: 'NEW' | 'READING_INPUT' | 'POST_READING' | 'RESULTS_READY' | 'FINISHED' | 'FAILED';
  workerCount?: number;
  partitionCount?: number;
  startTime?: string;
  duration?: number;
  sort?: boolean;
}

export interface ClusterBy {
  columns: {
    columnName: string;
    order?: 'ASCENDING' | 'DESCENDING';
  }[];
  bucketByCount?: number;
}

export function formatClusterBy(
  clusterBy: ClusterBy | undefined,
  part: 'all' | 'partition' | 'cluster' = 'all',
): string {
  if (!clusterBy) return '';

  let { columns, bucketByCount } = clusterBy;
  if (bucketByCount) {
    if (part === 'partition') {
      columns = columns.slice(0, bucketByCount);
    } else {
      columns = columns.slice(bucketByCount);
    }
  }

  return columns
    .map(part => part.columnName + (part.order === 'DESCENDING' ? ' DESC' : ''))
    .join(', ');
}

export interface StageWorkerCounter {
  [k: `input${number}`]: ChannelCounter | undefined;
  output?: ChannelCounter;
  shuffle?: ChannelCounter;
  sortProgress?: SortProgressCounter;
  segmentGenerationProgress?: SegmentGenerationProgressCounter;
  warnings?: WarningCounter;
}

export type ChannelCounterName = `input${number}` | 'output' | 'shuffle';

export type CounterName = keyof StageWorkerCounter;

function tallyWarningCount(warningCounter: WarningCounter): number {
  return sum(Object.values(warningCounter), v => (typeof v === 'number' ? v : 0));
}

function sumByKey(objs: Record<string, number>[]): Record<string, number> {
  const res: Record<string, number> = {};
  for (const obj of objs) {
    for (const k in obj) {
      if (Object.hasOwn(obj, k)) {
        res[k] = (res[k] || 0) + obj[k];
      }
    }
  }
  return res;
}

export interface ChannelCounter {
  type: 'channel';
  rows?: number[];
  bytes?: number[];
  frames?: number[];
  files?: number[];
  totalFiles?: number[];
}

export type ChannelFields = 'rows' | 'bytes' | 'frames' | 'files' | 'totalFiles';

export interface SortProgressCounter {
  type: 'sortProgress';
  totalMergingLevels: number;
  levelToTotalBatches: Record<number, number>;
  levelToMergedBatches: Record<number, number>;
  totalMergersForUltimateLevel: number;
  progressDigest?: number;
  triviallyComplete?: boolean;
}

export interface SegmentGenerationProgressCounter {
  type: 'segmentGenerationProgress';
  rowsProcessed: number;
  rowsPersisted: number;
  rowsMerged: number;
  rowsPushed: number;
}

export type SegmentGenerationProgressFields =
  | 'rowsProcessed'
  | 'rowsPersisted'
  | 'rowsMerged'
  | 'rowsPushed';

export interface WarningCounter {
  type: 'warning';
  CannotParseExternalData?: number;
  // More types of warnings might be added later
}

export interface SimpleWideCounter {
  index: number;
  [k: `input${number}`]: Record<ChannelFields, number> | undefined;
  output?: Record<ChannelFields, number>;
  shuffle?: Record<ChannelFields, number>;
  segmentGenerationProgress?: SegmentGenerationProgressCounter;
}

function zeroChannelFields(): Record<ChannelFields, number> {
  return {
    rows: 0,
    bytes: 0,
    frames: 0,
    files: 0,
    totalFiles: 0,
  };
}

export class Stages {
  static readonly QUERY_START_FACTOR = 0.05;
  static readonly QUERY_END_FACTOR = 0.05;

  static stageType(stage: StageDefinition): string {
    return stage.definition.processor.type;
  }

  static stageWeight(stage: StageDefinition): number {
    return Stages.stageType(stage) === 'limit' ? 0.1 : 1;
  }

  public readonly stages: StageDefinition[];
  private readonly counters?: Record<string, Record<string, StageWorkerCounter>>;

  constructor(
    stages: StageDefinition[],
    counters?: Record<string, Record<string, StageWorkerCounter>>,
  ) {
    this.stages = stages;
    this.counters = counters;
  }

  stageCount(): number {
    return this.stages.length;
  }

  getStage(stageNumber: number): StageDefinition {
    return this.stages[stageNumber];
  }

  getLastStage(): StageDefinition | undefined {
    return this.stages[this.stages.length - 1];
  }

  getAllCounters(): StageWorkerCounter[] {
    const { counters } = this;
    if (!counters) return [];
    return Object.values(counters).flatMap(wc => Object.values(wc));
  }

  getStageCounterTitle(stage: StageDefinition, counterName: CounterName): string {
    switch (counterName) {
      case 'output':
        return 'Processor output';

      case 'shuffle':
        return 'Shuffle output';

      case 'segmentGenerationProgress':
        return 'Segment generation';

      default:
        if (counterName.startsWith('input')) {
          const inputIndex = Number(counterName.replace('input', ''));
          return `Input${inputIndex} (${stage.definition.input[inputIndex].type})`;
        }
        return '';
    }
  }

  getCountersForStage(stage: StageDefinition): StageWorkerCounter[] {
    const { counters } = this;
    const c = counters?.[stage.stageNumber];
    return c ? Object.values(c) : [];
  }

  stageHasOutput(stage: StageDefinition): boolean {
    return Stages.stageType(stage) !== 'segmentGenerator';
  }

  stageHasSort(stage: StageDefinition): boolean {
    if (!this.stageHasOutput(stage)) return false;
    return Boolean(stage.sort);
  }

  stageOutputCounterName(stage: StageDefinition): ChannelCounterName {
    return this.stageHasSort(stage) ? 'shuffle' : 'output';
  }

  overallProgress(): number {
    const { stages } = this;
    let progress = 0;
    let total = Stages.QUERY_END_FACTOR;
    if (stages.length) {
      progress +=
        Stages.QUERY_START_FACTOR + sum(stages, s => this.stageProgress(s) * Stages.stageWeight(s));
      total += Stages.QUERY_START_FACTOR + sum(stages, Stages.stageWeight);
    }
    return zeroDivide(progress, total);
  }

  stageProgress(stage: StageDefinition): number {
    switch (stage.phase) {
      case 'READING_INPUT':
        return (
          (this.stageHasSort(stage) ? READING_INPUT_WITH_SORT_WEIGHT : 1) *
          this.readingInputPhaseProgress(stage)
        );

      case 'POST_READING':
        return READING_INPUT_WITH_SORT_WEIGHT + SORT_WEIGHT * this.postReadingPhaseProgress(stage);

      case 'RESULTS_READY':
      case 'FINISHED':
        return 1;

      default:
        return 0;
    }
  }

  readingInputPhaseProgress(stage: StageDefinition): number {
    const { stages } = this;
    const { input, broadcast } = stage.definition;

    const inputFileCount = this.getTotalInputForStage(stage, 'totalFiles');
    if (inputFileCount) {
      // If we know how many files there are base the progress on how many files were read
      return (
        sum(input, (input, i) =>
          input.type === 'external' ? this.getTotalCounterForStage(stage, `input${i}`, 'files') : 0,
        ) / inputFileCount
      );
    } else {
      // Otherwise, base it on the stage input divided by the output of all non-broadcast input stages,
      // use the segment generation counter in the special case of a segmentGenerator stage
      return zeroDivide(
        Stages.stageType(stage) === 'segmentGenerator'
          ? this.getTotalSegmentGenerationProgressForStage(stage, 'rowsPushed')
          : sum(input, (inputSource, i) =>
              inputSource.type === 'stage' && !broadcast?.includes(i)
                ? this.getTotalCounterForStage(stage, `input${i}`, 'rows')
                : 0,
            ),
        sum(input, (inputSource, i) =>
          inputSource.type === 'stage' && !broadcast?.includes(i)
            ? this.getTotalOutputForStage(stages[inputSource.stage], 'rows')
            : 0,
        ),
      );
    }
  }

  postReadingPhaseProgress(stage: StageDefinition): number {
    return this.getSortProgressForStage(stage);
  }

  currentStageIndex(): number {
    return this.stages.findIndex(({ phase }) => oneOf(phase, 'READING_INPUT', 'POST_READING'));
  }

  hasCounterForStage(stage: StageDefinition, counterName: CounterName): boolean {
    return this.getCountersForStage(stage).some(c => Boolean(c[counterName]));
  }

  hasSortProgressForStage(stage: StageDefinition): boolean {
    const { counters } = this;
    if (!counters) return false;
    const { phase } = stage;
    if (phase === 'READING_INPUT') return false;

    return this.getCountersForStage(stage).some(c => {
      const sortProgress = c.sortProgress;
      return Boolean(sortProgress?.progressDigest && !sortProgress.triviallyComplete);
    });
  }

  getWarningCount(): number {
    const { counters } = this;
    if (!counters) return 0;
    return sum(this.getAllCounters(), c => {
      const warningCounter = c.warnings;
      if (!warningCounter) return 0;
      return tallyWarningCount(warningCounter);
    });
  }

  getWarningCountForStage(stage: StageDefinition): number {
    const { counters } = this;
    if (!counters) return 0;
    return sum(this.getCountersForStage(stage), c => {
      const warningCounter = c.warnings;
      if (!warningCounter) return 0;
      return tallyWarningCount(warningCounter);
    });
  }

  getWarningBreakdownForStage(stage: StageDefinition): Record<string, number> {
    const { counters } = this;
    if (!counters) return {};
    return sumByKey(
      filterMap(this.getCountersForStage(stage), c => {
        const warningCounter = c.warnings as Record<string, number> | undefined;
        if (!warningCounter) return;
        return deleteKeys(warningCounter, ['type']);
      }),
    );
  }

  getTotalCounterForStage(
    stage: StageDefinition,
    counterName: CounterName,
    field: ChannelFields,
  ): number {
    const { counters } = this;
    if (!counters) return 0;
    return sum(this.getCountersForStage(stage), c => {
      const counter = c[counterName];
      if (counter?.type !== 'channel') return 0;
      return sum(counter[field] || []);
    });
  }

  getInputCountersForStage(stage: StageDefinition, field: ChannelFields): number[] {
    return stage.definition.input.map((_, i) =>
      this.getTotalCounterForStage(stage, `input${i}`, field),
    );
  }

  getTotalInputForStage(stage: StageDefinition, field: ChannelFields): number {
    return sum(this.getInputCountersForStage(stage, field));
  }

  getTotalOutputForStage(stage: StageDefinition, field: 'frames' | 'rows' | 'bytes'): number {
    return this.getTotalCounterForStage(stage, this.stageOutputCounterName(stage), field);
  }

  getSortProgressForStage(stage: StageDefinition): number {
    const { counters } = this;
    if (!counters) return 0;
    return zeroDivide(
      sum(this.getCountersForStage(stage), c => {
        const rowsToSort = c.output ? sum(c.output.rows || []) : 0;
        const progressDigest = c.sortProgress?.progressDigest || 0;
        return Math.floor(rowsToSort * progressDigest);
      }),
      this.getTotalCounterForStage(stage, 'output', 'rows'),
    );
  }

  getTotalSegmentGenerationProgressForStage(
    stage: StageDefinition,
    field: SegmentGenerationProgressFields,
  ): number {
    const { counters } = this;
    if (!counters) return 0;
    return sum(this.getCountersForStage(stage), c => c.segmentGenerationProgress?.[field] || 0);
  }

  getChannelCounterNamesForStage(stage: StageDefinition): ChannelCounterName[] {
    const { definition } = stage;

    const channelCounters = definition.input.map((_, i) => `input${i}` as ChannelCounterName);
    if (this.stageHasOutput(stage)) channelCounters.push('output');
    if (this.stageHasSort(stage)) channelCounters.push('shuffle');
    return channelCounters;
  }

  getByWorkerCountersForStage(stage: StageDefinition): SimpleWideCounter[] {
    const { counters } = this;
    const { stageNumber } = stage;

    const channelCounters = this.getChannelCounterNamesForStage(stage);

    const forStageCounters = counters?.[stageNumber] || {};
    return Object.entries(forStageCounters).map(([key, stageCounters]) => {
      const newWideCounter: SimpleWideCounter = {
        index: Number(key),
      };
      for (const channel of channelCounters) {
        const c = stageCounters[channel];
        newWideCounter[channel] = c
          ? {
              rows: sum(c.rows || []),
              bytes: sum(c.bytes || []),
              frames: sum(c.frames || []),
              files: sum(c.files || []),
              totalFiles: sum(c.totalFiles || []),
            }
          : zeroChannelFields();
      }
      newWideCounter.segmentGenerationProgress = stageCounters.segmentGenerationProgress;
      return newWideCounter;
    });
  }

  getPartitionChannelCounterNamesForStage(
    stage: StageDefinition,
    type: 'input' | 'output',
  ): ChannelCounterName[] {
    if (type === 'input') {
      const { input, broadcast } = stage.definition;
      return filterMap(input, (input, i) =>
        input.type === 'stage' && !broadcast?.includes(i)
          ? (`input${i}` as ChannelCounterName)
          : undefined,
      );
    } else {
      return [this.stageOutputCounterName(stage)];
    }
  }

  getByPartitionCountersForStage(
    stage: StageDefinition,
    type: 'input' | 'output',
  ): SimpleWideCounter[] {
    const counterNames = this.getPartitionChannelCounterNamesForStage(stage, type);
    if (!counterNames.length) return [];

    if (!this.hasCounterForStage(stage, counterNames[0])) return [];
    const stageCounters = this.getCountersForStage(stage);

    const { partitionCount } = stage;
    const partitionNumber =
      type === 'output'
        ? partitionCount
        : max(stageCounters, stageCounter =>
            max(counterNames, counterName => {
              const channelCounter = stageCounter[counterName];
              if (channelCounter?.type !== 'channel') return 0;
              return channelCounter.rows?.length || 0;
            }),
          );

    if (!partitionNumber) return [];

    const simpleCounters: SimpleWideCounter[] = [];

    // Initialize all portions and their counters to 0s
    for (let i = 0; i < partitionNumber; i++) {
      const newSimpleCounter: SimpleWideCounter = { index: i };
      for (const counterName of counterNames) newSimpleCounter[counterName] = zeroChannelFields();
      simpleCounters.push(newSimpleCounter);
    }

    for (const stageCounter of stageCounters) {
      for (const counterName of counterNames) {
        const channelCounter = stageCounter[counterName];
        if (channelCounter?.type !== 'channel') continue;
        const n = channelCounter.rows?.length || 0;
        if (!n) continue;

        for (let i = 0; i < n; i++) {
          const c = simpleCounters[i][counterName]!; // This must be defined as we initialized all the counters above
          c.rows += channelCounter.rows?.[i] || 0;
          c.bytes += channelCounter.bytes?.[i] || 0;
          c.frames += channelCounter.frames?.[i] || 0;
          c.files += channelCounter.files?.[i] || 0;
          c.totalFiles += channelCounter.totalFiles?.[i] || 0;
        }
      }
    }

    return simpleCounters;
  }

  getRateFromStage(stage: StageDefinition, field: ChannelFields): number | undefined {
    if (!stage.duration) return;
    if (field === 'bytes' && stage.definition.input.some(input => input.type !== 'stage')) {
      // If we have inputs that do not report bytes, don't show a rate
      return;
    }
    return Math.round(
      Math.max(
        this.getTotalInputForStage(stage, field),
        this.getTotalCounterForStage(stage, 'output', field),
      ) /
        (stage.duration / 1000),
    );
  }

  getPotentiallyStuckStageIndex(): number {
    const { stages } = this;
    const potentiallyStuckIndex = stages.findIndex(stage => typeof stage.phase === 'undefined');

    if (potentiallyStuckIndex > 0) {
      const prevStage = stages[potentiallyStuckIndex - 1];
      if (oneOf(prevStage.phase, 'NEW', 'READING_INPUT')) {
        // Previous stage is still working so this stage is not stuck, it is just waiting
        return -1;
      }
    }

    return potentiallyStuckIndex;
  }
}
