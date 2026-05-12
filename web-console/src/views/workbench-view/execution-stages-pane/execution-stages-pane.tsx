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

import { Button, Icon, Intent } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import classNames from 'classnames';
import React from 'react';
import type { Column } from 'react-table';
import ReactTable from 'react-table';

import { BracedText, TableClickableCell } from '../../../components';
import type {
  ChannelCounterName,
  ChannelFields,
  ClusterBy,
  CounterName,
  Execution,
  InOut,
  SegmentGenerationProgressFields,
  SimpleWideCounter,
  StageDefinition,
  StageInput,
} from '../../../druid-models';
import {
  CPUS_COUNTER_FIELDS,
  cpusCounterFieldTitle,
  formatClusterBy,
  Stages,
  summarizeInputSource,
} from '../../../druid-models';
import { DEFAULT_TABLE_CLASS_NAME } from '../../../react-table';
import {
  assemble,
  capitalizeFirst,
  clamp,
  deepGet,
  filterMap,
  formatByteRate,
  formatBytesCompact,
  formatDurationWithMs,
  formatDurationWithMsIfNeeded,
  formatInteger,
  formatPercent,
  oneOf,
  pluralIfNeeded,
  twoLines,
} from '../../../utils';

import './execution-stages-pane.scss';

const MAX_STAGE_ROWS = 20;
const MAX_DETAIL_ROWS = 20;
const NOT_SIZE_ON_DISK = '(does not represent size on disk)';
const NO_SIZE_INFO = 'no size info';

// For the graph
const LANE_WIDTH = 20;
const STAGE_OFFSET = '19px';

function summarizeTableInput(tableStageInput: StageInput): string {
  if (tableStageInput.type !== 'table') return '';
  return assemble(
    `Datasource: ${tableStageInput.dataSource}`,
    tableStageInput.intervals && `Interval: ${tableStageInput.intervals.join('; ')}`,
  ).join('\n');
}

function formatBreakdown(breakdown: Record<string, number>): string {
  return Object.keys(breakdown)
    .map(k => `${k}: ${formatInteger(breakdown[k])}`)
    .join('\n');
}

const formatRows = formatInteger;
const formatRowRate = formatInteger;
const formatFrames = formatInteger;

const formatFileOfTotal = (files: number, totalFiles: number) =>
  `(${formatInteger(files)} / ${formatInteger(totalFiles)})`;

const formatFileOfTotalForBrace = (files: number, totalFiles: number) =>
  `(${formatInteger(files)} /GB ${formatInteger(totalFiles)})`;

function formatLoadTooltip(
  loadFiles: number,
  loadBytes?: number,
  loadTime?: number,
  loadWait?: number,
): string {
  return assemble(
    `Loaded files: ${formatInteger(loadFiles)}`,
    loadBytes != null && `Loaded bytes: ${formatBytesCompact(loadBytes)}`,
    loadTime != null && loadTime > 0 && `Load time: ${formatDurationWithMs(loadTime)}`,
    loadTime && loadBytes
      ? `Load rate: ${formatByteRate(loadBytes / (loadTime / 1000))}`
      : undefined,
    loadWait != null && loadWait > 0 && `Load wait: ${formatDurationWithMs(loadWait)}`,
  ).join('\n');
}

function inputLabelContent(stage: StageDefinition, inputIndex: number) {
  const { input, broadcast } = stage.definition;
  const stageInput = input[inputIndex];
  return (
    <>
      Input{' '}
      {stageInput.type === 'stage' && <span className="stage">{`Stage${stageInput.stage}`}</span>}
      {stageInput.type === 'table' && (
        <span className="datasource" data-tooltip={summarizeTableInput(stageInput)}>
          {stageInput.dataSource}
        </span>
      )}
      {stageInput.type === 'external' && (
        <span
          className="external"
          data-tooltip={summarizeInputSource(stageInput.inputSource, true)}
        >
          {`${stageInput.inputSource.type} external`}
        </span>
      )}
      {broadcast?.includes(inputIndex) && (
        <Icon
          className="broadcast-tag"
          icon={IconNames.CELL_TOWER}
          data-tooltip="This input is being broadcast to all workers in this stage."
        />
      )}
    </>
  );
}

function formatInputLabel(stage: StageDefinition, inputIndex: number) {
  const { input, broadcast } = stage.definition;
  const stageInput = input[inputIndex];
  let ret = 'Input ';
  switch (stageInput.type) {
    case 'stage':
      ret += `Stage${stageInput.stage}`;
      break;

    case 'table':
      ret += stageInput.dataSource;
      break;

    case 'external':
      ret += `${stageInput.inputSource.type} external`;
      break;
  }

  if (broadcast?.includes(inputIndex)) {
    ret += ` (broadcast)`;
  }

  return ret;
}

export interface ExecutionStagesPaneProps {
  execution: Execution;
  onErrorClick?(): void;
  onWarningClick?(): void;
  goToTask?(taskId: string): void;
}

export const ExecutionStagesPane = React.memo(function ExecutionStagesPane(
  props: ExecutionStagesPaneProps,
) {
  const { execution, onErrorClick, onWarningClick, goToTask } = props;
  const stages = execution.stages || new Stages([]);
  const error = execution.error;
  const executionStartTime = execution.startTime;
  const executionDuration = execution.duration;

  const rowRateValues = stages.stages.map(s =>
    formatRowRate(stages.getRateFromStage(s, 'rows') || 0),
  );

  const rowsValues = stages.stages.flatMap(stage => [
    ...stages.getInputCountersForStage(stage, 'rows').map(formatRows),
    formatRows(stages.getTotalCounterForStage(stage, 'output', 'rows')),
    formatRows(stages.getTotalCounterForStage(stage, 'shuffle', 'rows')),
    formatRows(stages.getTotalSegmentGenerationProgressForStage(stage, 'rowsMerged')),
    formatRows(stages.getTotalSegmentGenerationProgressForStage(stage, 'rowsPushed')),
  ]);

  const filesValues = filterMap(stages.stages, stage => {
    const inputFileCount = stages.getTotalInputForStage(stage, 'totalFiles');
    if (!inputFileCount) return;
    return formatFileOfTotalForBrace(inputFileCount, inputFileCount);
  });

  function detailedStats(stage: StageDefinition) {
    const { phase } = stage;
    const phaseIsWorking = oneOf(phase, 'NEW', 'READING_INPUT', 'POST_READING');
    return (
      <div className="execution-stage-detail-pane">
        {detailedCountersForPartitions(stage, 'in', phase === 'READING_INPUT')}
        {detailedCountersForWorkers(stage)}
        {detailedCountersForPartitions(stage, 'out', phaseIsWorking)}
        {detailedCountersForSort(stage)}
      </div>
    );
  }

  function detailedCountersForWorkers(stage: StageDefinition) {
    const wideCounters = stages.getByWorkerCountersForStage(stage);
    if (!wideCounters.length) return;

    const counterNames: ChannelCounterName[] = stages.getChannelCounterNamesForStage(stage);

    const isSegmentGenerator = Stages.stageType(stage) === 'segmentGenerator';

    // Unified braces for the combined rows column
    const allBracesRows: string[] = counterNames.flatMap(counterName =>
      wideCounters.map(wideCounter => formatRows(wideCounter[counterName]!.rows)),
    );
    if (isSegmentGenerator) {
      allBracesRows.push(
        ...wideCounters.map(wc => formatRows(wc.segmentGenerationProgress?.rowsMerged || 0)),
        ...wideCounters.map(wc => formatRows(wc.segmentGenerationProgress?.rowsPushed || 0)),
      );
    }
    const allBracesFiles: string[] = filterMap(
      counterNames.flatMap(counterName =>
        wideCounters.map(wideCounter => wideCounter[counterName]!),
      ),
      c => (c.totalFiles ? formatFileOfTotalForBrace(c.totalFiles, c.totalFiles) : undefined),
    );
    const firstNonInputIndex = counterNames.findIndex(cn => !cn.startsWith('input'));

    return (
      <ReactTable
        className="detail-counters-for-workers"
        data={wideCounters}
        loading={false}
        sortable
        defaultSorted={[{ id: 'worker', desc: false }]}
        defaultPageSize={clamp(wideCounters.length, 1, MAX_DETAIL_ROWS)}
        showPagination={wideCounters.length > MAX_DETAIL_ROWS}
        columns={[
          {
            Header: 'Worker',
            id: 'worker',
            accessor: d => d.index,
            className: goToTask ? undefined : 'padded',
            width: 95,
            Cell({ value }) {
              const workerStates = execution.workers?.[String(value)];
              const workerState = workerStates?.[workerStates.length - 1];
              const label = `Worker${value}`;

              const workerRef = workerState?.workerDesc || workerState?.workerId;
              if (goToTask && workerRef && !/:\d+$/.test(workerRef)) {
                return (
                  <TableClickableCell
                    hoverIcon={IconNames.SHARE}
                    tooltip={`Go to task: ${workerRef}`}
                    onClick={() => {
                      goToTask(workerRef);
                    }}
                  >
                    {label}
                  </TableClickableCell>
                );
              }

              if (workerRef) {
                return <span data-tooltip={workerRef}>{label}</span>;
              }

              return label;
            },
          } as Column<SimpleWideCounter>,
          {
            Header: twoLines(
              'CPU utilization',
              <i>
                <span className="cpu-label">Counter</span>
                <span className="cpu-counter">Wall time</span>
              </i>,
            ),
            id: 'cpu',
            accessor: d => d.cpu?.main?.cpu || 0,
            className: 'padded',
            width: 240,
            show: stages.hasCounterForStage(stage, 'cpu'),
            Cell({ original }) {
              const cpuTotals = original.cpu || {};
              return (
                <>
                  {filterMap(CPUS_COUNTER_FIELDS, k => {
                    const v = cpuTotals[k];
                    if (!v) return;
                    const fieldTitle = cpusCounterFieldTitle(k);
                    return (
                      <div
                        key={k}
                        data-tooltip={`${fieldTitle}\nCPU time: ${formatDurationWithMs(
                          v.cpu / 1e6,
                        )}`}
                      >
                        <span className="cpu-label">{cpusCounterFieldTitle(k)}</span>
                        <span className="cpu-counter">{formatDurationWithMs(v.wall / 1e6)}</span>
                      </div>
                    );
                  })}
                </>
              );
            },
          } as Column<SimpleWideCounter>,
        ].concat([
          {
            Header: twoLines('Rows processed', <i>rows &nbsp; (input files)</i>),
            id: 'rows_processed',
            accessor: (d: SimpleWideCounter) =>
              counterNames.reduce((acc, cn) => acc + (d[cn]?.rows || 0), 0),
            className: 'padded',
            width: 300,
            Cell({ original }: { original: SimpleWideCounter }) {
              return (
                <>
                  {counterNames.map((counterName, idx) => {
                    const c = original[counterName]!;
                    const isInput = counterName.startsWith('input');
                    const inputIndex = isInput ? Number(counterName.replace('input', '')) : -1;
                    const showSpacer = idx === firstNonInputIndex && firstNonInputIndex > 0;
                    const label = isInput
                      ? formatInputLabel(stage, inputIndex)
                      : stages.getStageCounterTitle(stage, counterName);
                    const tooltipParts: string[] = [];
                    if (c.bytes) {
                      tooltipParts.push(
                        `Uncompressed size: ${formatBytesCompact(c.bytes)} ${NOT_SIZE_ON_DISK}`,
                      );
                    }
                    if (c.loadFiles) {
                      tooltipParts.push(
                        formatLoadTooltip(c.loadFiles, c.loadBytes, c.loadTime, c.loadWait),
                      );
                    }
                    if (c.queries || c.totalQueries) {
                      tooltipParts.push(
                        `Realtime queries: ${formatInteger(c.queries || 0)} / ${formatInteger(
                          c.totalQueries || 0,
                        )}`,
                      );
                    }
                    return (
                      <React.Fragment key={counterName}>
                        {showSpacer && <div className="counter-spacer extend-right" />}
                        <div
                          data-tooltip={
                            tooltipParts.length ? tooltipParts.join('\n') : NO_SIZE_INFO
                          }
                        >
                          <span className="rows-label">{label}</span>
                          <BracedText text={formatRows(c.rows)} braces={allBracesRows} />
                          {Boolean(c.totalFiles) && (
                            <>
                              {' '}
                              &nbsp;{' '}
                              <BracedText
                                text={formatFileOfTotal(c.files, c.totalFiles)}
                                braces={allBracesFiles}
                              />
                            </>
                          )}
                        </div>
                      </React.Fragment>
                    );
                  })}
                  {isSegmentGenerator && (
                    <>
                      <div className="counter-spacer extend-right" />
                      <div>
                        <span className="rows-label">Merged</span>
                        <BracedText
                          text={formatRows(original.segmentGenerationProgress?.rowsMerged || 0)}
                          braces={allBracesRows}
                        />
                      </div>
                      <div>
                        <span className="rows-label">Pushed</span>
                        <BracedText
                          text={formatRows(original.segmentGenerationProgress?.rowsPushed || 0)}
                          braces={allBracesRows}
                        />
                      </div>
                    </>
                  )}
                </>
              );
            },
          } as Column<SimpleWideCounter>,
          {
            Header: 'Storage utilization',
            id: 'storage',
            accessor: (d: SimpleWideCounter) => {
              const s = d.storage;
              if (!s) return 0;
              return s.localBytesWritten + s.durableBytesWritten;
            },
            className: 'padded',
            width: 250,
            show: stages.hasCounterForStage(stage, 'storage'),
            Cell({ original }: { original: SimpleWideCounter }) {
              const s = original.storage;
              if (!s) return <i>none</i>;

              const hasLocal = s.localBytesWritten > 0 || s.localBytesReserved > 0;
              const hasDurable = s.durableBytesWritten > 0;

              if (!hasLocal && !hasDurable) return <i>none</i>;

              const tooltipParts: string[] = [];
              if (hasLocal) {
                const usedPart =
                  s.localBytesMax != null
                    ? `(${formatBytesCompact(s.localBytesReserved)} / ${formatBytesCompact(
                        s.localBytesMax,
                      )} used)`
                    : `(${formatBytesCompact(s.localBytesReserved)} used)`;
                tooltipParts.push(
                  `Local: ${formatBytesCompact(s.localBytesWritten)} written in ${pluralIfNeeded(
                    s.localFilesWritten,
                    'file',
                  )} ${usedPart}`,
                );
              }
              if (hasDurable) {
                tooltipParts.push(
                  `Durable: ${formatBytesCompact(
                    s.durableBytesWritten,
                  )} written in ${pluralIfNeeded(s.durableFileCount, 'file')}`,
                );
              }

              return (
                <div data-tooltip={tooltipParts.join('\n')}>
                  {hasLocal && (
                    <div>
                      <span className="storage-label">Local</span>
                      {formatBytesCompact(s.localBytesWritten)}
                      {s.localBytesReserved > 0 && (
                        <span className="storage-used">
                          {` (${formatBytesCompact(s.localBytesReserved)} used)`}
                        </span>
                      )}
                    </div>
                  )}
                  {hasDurable && (
                    <div>
                      <span className="storage-label">Durable</span>
                      {formatBytesCompact(s.durableBytesWritten)}
                    </div>
                  )}
                </div>
              );
            },
          } as Column<SimpleWideCounter>,
        ])}
      />
    );
  }

  function detailedCountersForSort(stage: StageDefinition) {
    const aggregatedSortProgress = stages.getAggregatedSortProgressForStage(stage);
    if (!aggregatedSortProgress) return;

    const data = [
      { name: 'levelToBatches', counts: aggregatedSortProgress.totalMergingLevels },
      ...Object.entries(aggregatedSortProgress.levelToBatches).map(([level, counts]) => ({
        name: `Level ${level}`,
        counts,
      })),
    ];

    return (
      <ReactTable
        className="detail-counters-for-sort"
        data={data}
        loading={false}
        defaultPageSize={clamp(data.length, 1, MAX_DETAIL_ROWS)}
        showPagination={data.length > MAX_DETAIL_ROWS}
        columns={[
          {
            Header: `Sort stat`,
            accessor: 'name',
            className: 'padded',
            width: 120,
          },
          {
            Header: `Counts`,
            accessor: 'counts',
            className: 'padded wrapped',
            width: 300,
            Cell({ value }) {
              const entries = Object.entries(value);
              if (!entries.length) return '-';
              return (
                <>
                  {entries.map(([n, v], i) => (
                    <>
                      <span
                        key={n}
                        data-tooltip={`${pluralIfNeeded(Number(v), 'worker')} reporting: ${n}`}
                      >
                        {n}
                        {Number(v) > 1 && <span className="count">{` (${v})`}</span>}
                      </span>
                      {i < entries.length - 1 && <span key={`${n}_sep`}>, </span>}
                    </>
                  ))}
                </>
              );
            },
          },
        ]}
      />
    );
  }

  function detailedCountersForPartitions(
    stage: StageDefinition,
    inOut: InOut,
    inProgress: boolean,
  ) {
    const wideCounters = stages.getByPartitionCountersForStage(stage, inOut);
    if (!wideCounters.length) return;

    const counterNames: ChannelCounterName[] = stages.getPartitionChannelCounterNamesForStage(
      stage,
      inOut,
    );

    const bracesRows: Record<ChannelCounterName, string[]> = {} as any;
    for (const counterName of counterNames) {
      bracesRows[counterName] = wideCounters.map(wideCounter =>
        formatRows(wideCounter[counterName]!.rows),
      );
    }

    return (
      <ReactTable
        className="detail-counters-for-partitions"
        data={wideCounters}
        loading={false}
        sortable
        defaultSorted={[{ id: 'partition', desc: false }]}
        defaultPageSize={clamp(wideCounters.length, 1, MAX_DETAIL_ROWS)}
        showPagination={wideCounters.length > MAX_DETAIL_ROWS}
        columns={[
          {
            Header: `${capitalizeFirst(inOut)} partitions` + (inProgress ? '*' : ''),
            id: 'partition',
            accessor: d => d.index,
            className: 'padded',
            width: 120,
            Cell({ value }) {
              return `Partition${value}`;
            },
          } as Column<SimpleWideCounter>,
        ].concat(
          counterNames.map(counterName => {
            return {
              Header: twoLines(
                stages.getStageCounterTitle(stage, counterName),
                <i>rows &nbsp; (size)</i>,
              ),
              id: counterName,
              accessor: d => d[counterName]!.rows,
              className: 'padded',
              width: 180,
              Cell({ value, original }) {
                const c: Record<ChannelFields, number> = original[counterName];
                return (
                  <BracedText
                    text={formatRows(value)}
                    braces={bracesRows[counterName]}
                    data-tooltip={
                      c.bytes
                        ? `Uncompressed size: ${formatBytesCompact(c.bytes)} ${NOT_SIZE_ON_DISK}`
                        : NO_SIZE_INFO
                    }
                  />
                );
              },
            };
          }),
        )}
      />
    );
  }

  function dataProcessedInput(stage: StageDefinition, inputNumber: number) {
    const inputCounter: CounterName = `input${inputNumber}`;
    const hasCounter = stages.hasCounterForStage(stage, inputCounter);
    const bytes = stages.getTotalCounterForStage(stage, inputCounter, 'bytes');
    const inputFileCount = stages.getTotalCounterForStage(stage, inputCounter, 'totalFiles');
    const loadFiles = stages.getTotalCounterForStage(stage, inputCounter, 'loadFiles');
    const loadBytes = stages.getTotalCounterForStage(stage, inputCounter, 'loadBytes');
    const loadTime = stages.getTotalCounterForStage(stage, inputCounter, 'loadTime');
    const loadWait = stages.getTotalCounterForStage(stage, inputCounter, 'loadWait');
    const queries = stages.getTotalCounterForStage(stage, inputCounter, 'queries');
    const totalQueries = stages.getTotalCounterForStage(stage, inputCounter, 'totalQueries');
    const inputLabel = `${formatInputLabel(stage, inputNumber)} (input${inputNumber})`;
    return (
      <div
        className="data-transfer"
        key={inputNumber}
        data-tooltip={
          bytes
            ? `${inputLabel} uncompressed size: ${formatBytesCompact(bytes)} ${NOT_SIZE_ON_DISK}`
            : `${inputLabel}: ${NO_SIZE_INFO}`
        }
      >
        <BracedText
          text={
            hasCounter
              ? formatRows(stages.getTotalCounterForStage(stage, inputCounter, 'rows'))
              : ''
          }
          braces={rowsValues}
        />
        {Boolean(inputFileCount) && (
          <>
            {' '}
            &nbsp;{' '}
            <BracedText
              text={formatFileOfTotal(
                stages.getTotalCounterForStage(stage, inputCounter, 'files'),
                inputFileCount,
              )}
              braces={filesValues}
            />
          </>
        )}
        {Boolean(loadFiles) && (
          <>
            {' '}
            &nbsp;{' '}
            <Icon
              className="load-indicator"
              icon={IconNames.IMPORT}
              data-tooltip={formatLoadTooltip(loadFiles, loadBytes, loadTime, loadWait)}
            />
          </>
        )}
        {Boolean(queries || totalQueries) && (
          <>
            {' '}
            &nbsp;{' '}
            <Icon
              icon={IconNames.ARROW_BOTTOM_LEFT}
              data-tooltip={`Realtime queries (${formatInteger(queries || 0)} / ${formatInteger(
                totalQueries || 0,
              )})`}
            />
          </>
        )}
      </div>
    );
  }

  function dataProcessedInputBroadcast(stage: StageDefinition, inputNumber: number) {
    const inputCounter: CounterName = `input${inputNumber}`;
    if (!stages.hasCounterForStage(stage, inputCounter)) return;
    const stageInput = stage.definition.input[inputNumber];
    if (stageInput.type !== 'stage') return;
    const sourceStage = stages.getStage(stageInput.stage);
    const timesRead =
      stages.getTotalCounterForStage(stage, inputCounter, 'rows') /
      stages.getTotalOutputForStage(sourceStage, 'rows');

    let msg = timesRead.toFixed(2).replace(/\.00$/, '');
    msg += msg === '1' ? ' time' : ' times';

    return (
      <div className="data-transfer" key={inputNumber}>
        {`Read ${msg}`}
      </div>
    );
  }

  function dataProcessedOutput(stage: StageDefinition) {
    if (!stages.hasCounterForStage(stage, 'output')) return;

    const title = stages.getStageCounterTitle(stage, 'output');
    return (
      <div
        className="data-transfer"
        data-tooltip={`${title} frames: ${formatFrames(
          stages.getTotalCounterForStage(stage, 'output', 'frames'),
        )}
${title} uncompressed size: ${formatBytesCompact(
          stages.getTotalCounterForStage(stage, 'output', 'bytes'),
        )} ${NOT_SIZE_ON_DISK}`}
      >
        <BracedText
          text={formatRows(stages.getTotalCounterForStage(stage, 'output', 'rows'))}
          braces={rowsValues}
        />
      </div>
    );
  }

  function dataProcessedShuffle(stage: StageDefinition) {
    const hasCounter = stages.hasCounterForStage(stage, 'shuffle');
    const hasProgress = stages.hasSortProgressForStage(stage);
    if (!hasCounter && !hasProgress) return;

    const shuffleRows = stages.getTotalCounterForStage(stage, 'shuffle', 'rows');
    const sortProgress = stages.getSortProgressForStage(stage);
    const showSortedPercent = 0 < sortProgress && sortProgress < 1;
    const title = stages.getStageCounterTitle(stage, 'shuffle');
    return (
      <div
        className="data-transfer"
        data-tooltip={`${title} frames: ${formatFrames(
          stages.getTotalCounterForStage(stage, 'shuffle', 'frames'),
        )}
${title} uncompressed size: ${formatBytesCompact(
          stages.getTotalCounterForStage(stage, 'shuffle', 'bytes'),
        )} ${NOT_SIZE_ON_DISK}`}
      >
        {Boolean(shuffleRows) && <BracedText text={formatRows(shuffleRows)} braces={rowsValues} />}
        {Boolean(shuffleRows && showSortedPercent) && <>&nbsp; : &nbsp;</>}
        {showSortedPercent && `${formatPercent(sortProgress)} sorted`}
      </div>
    );
  }

  function dataProcessedSegmentGeneration(
    stage: StageDefinition,
    field: SegmentGenerationProgressFields,
  ) {
    if (!stages.hasCounterForStage(stage, 'segmentGenerationProgress')) return;

    return (
      <div className="data-transfer" data-tooltip={NO_SIZE_INFO}>
        <BracedText
          text={formatRows(stages.getTotalSegmentGenerationProgressForStage(stage, field))}
          braces={rowsValues}
        />
      </div>
    );
  }

  const graphInfos = stages.getGraphInfos();
  const maxLanes = Math.max(...graphInfos.map(graphInfo => graphInfo.length));

  function laneX(laneNumber: number) {
    return `${((laneNumber + 0.5) / maxLanes) * 100}%`;
  }

  return (
    <ReactTable
      className={classNames('execution-stages-pane', DEFAULT_TABLE_CLASS_NAME)}
      data={stages.stages}
      loading={false}
      noDataText="No stages"
      sortable={false}
      collapseOnDataChange={false}
      defaultPageSize={clamp(stages.stageCount(), 7, MAX_STAGE_ROWS)}
      showPagination={stages.stageCount() > MAX_STAGE_ROWS}
      SubComponent={({ original }) => detailedStats(original)}
      columns={[
        {
          id: 'graph',
          className: 'graph-cell',
          accessor: 'stageNumber',
          show: maxLanes > 1,
          width: LANE_WIDTH * maxLanes,
          minWidth: LANE_WIDTH,
          Cell({ value }) {
            const graphInfo = graphInfos[value];

            return (
              <svg xmlns="http://www.w3.org/2000/svg">
                {graphInfo.flatMap((lane, i) => {
                  switch (lane.type) {
                    case 'line':
                      return (
                        <line
                          key={`line${i}`}
                          x1={laneX(lane.fromLane)}
                          y1="0%"
                          x2={laneX(i)}
                          y2="100%"
                        />
                      );

                    case 'stage':
                      return [
                        ...lane.fromLanes.map(fromLane => (
                          <line
                            key={`stage${i}_from${fromLane}`}
                            x1={laneX(fromLane)}
                            y1="0%"
                            x2={laneX(i)}
                            y2={STAGE_OFFSET}
                          />
                        )),
                        ...(lane.hasOut
                          ? [
                              <line
                                key={`stage${i}_out`}
                                x1={laneX(i)}
                                y1={STAGE_OFFSET}
                                x2={laneX(i)}
                                y2="100%"
                              />,
                            ]
                          : []),
                        <circle key={`stage${i}_stage`} cx={laneX(i)} cy={STAGE_OFFSET} r={5} />,
                      ];
                  }
                })}
              </svg>
            );
          },
        },
        {
          Header: twoLines('Stage', <i>processorType</i>),
          id: 'stage',
          accessor: 'stageNumber',
          className: 'padded',
          width: 160,
          Cell(props) {
            const stage = props.original as StageDefinition;
            const myError = error && error.stageNumber === stage.stageNumber;
            const warnings = stages.getWarningCountForStage(stage);
            return (
              <>
                <div>
                  <span className="stage">{`Stage${stage.stageNumber}`}</span>
                </div>
                <div>{stage.definition.processor.type}</div>
                {stage.sort && <div className="detail-line">(with sort)</div>}
                {(myError || warnings > 0) && (
                  <div className="error-warning">
                    {myError && (
                      <Button
                        minimal
                        small
                        icon={IconNames.ERROR}
                        intent={Intent.DANGER}
                        onClick={onErrorClick}
                        data-tooltip={
                          (error.error.errorCode ? `${error.error.errorCode}: ` : '') +
                          error.error.errorMessage
                        }
                      />
                    )}
                    {myError && warnings > 0 && ' '}
                    {warnings > 0 && (
                      <Button
                        minimal
                        small
                        icon={IconNames.WARNING_SIGN}
                        text={warnings > 1 ? `${warnings}` : undefined}
                        intent={Intent.WARNING}
                        onClick={onWarningClick}
                        data-tooltip={formatBreakdown(stages.getWarningBreakdownForStage(stage))}
                      />
                    )}
                  </div>
                )}
              </>
            );
          },
        },
        {
          Header: 'Counter',
          id: 'counter',
          accessor: 'stageNumber',
          className: 'padded',
          width: 150,
          Cell(props) {
            const stage = props.original as StageDefinition;
            const { input } = stage.definition;
            return (
              <>
                {input.map((_, i) => (
                  <div key={i}>{inputLabelContent(stage, i)}</div>
                ))}
                {stages.hasCounterForStage(stage, 'output') && (
                  <>
                    <div className="counter-spacer extend-right" />
                    <div>{stages.getStageCounterTitle(stage, 'output')}</div>
                    {stages.hasCounterForStage(stage, 'shuffle') && (
                      <div>{stages.getStageCounterTitle(stage, 'shuffle')}</div>
                    )}
                  </>
                )}
                {stages.hasCounterForStage(stage, 'segmentGenerationProgress') && (
                  <>
                    <div className="counter-spacer extend-right" />
                    <div>Merged</div>
                    <div>Pushed</div>
                  </>
                )}
              </>
            );
          },
        },
        {
          Header: twoLines('Rows processed', <i>rows &nbsp; (input files)</i>),
          id: 'rows_processed',
          accessor: () => null,
          className: 'padded',
          width: 200,
          Cell({ original }) {
            const stage = original as StageDefinition;
            const { input, broadcast } = stage.definition;
            return (
              <>
                {input.map((_, i) =>
                  broadcast?.includes(i)
                    ? dataProcessedInputBroadcast(stage, i)
                    : dataProcessedInput(stage, i),
                )}
                {stages.hasCounterForStage(stage, 'output') && (
                  <>
                    <div className="counter-spacer extend-left" />
                    {dataProcessedOutput(stage)}
                    {dataProcessedShuffle(stage)}
                  </>
                )}
                {stages.hasCounterForStage(stage, 'segmentGenerationProgress') &&
                  stages.getTotalSegmentGenerationProgressForStage(stage, 'rowsMerged') > 0 && (
                    <>
                      <div className="counter-spacer extend-left" />
                      {dataProcessedSegmentGeneration(stage, 'rowsMerged')}
                      {dataProcessedSegmentGeneration(stage, 'rowsPushed')}
                    </>
                  )}
              </>
            );
          },
        },
        {
          Header: twoLines('Processing rate', <i>rows/s</i>),
          id: 'processing_rate',
          accessor: s => stages.getRateFromStage(s, 'rows'),
          className: 'padded',
          width: 150,
          Cell({ value, original }) {
            const stage = original as StageDefinition;
            if (typeof value !== 'number') return null;

            const byteRate = stages.getRateFromStage(stage, 'bytes');
            return (
              <BracedText
                className="moon"
                text={formatRowRate(value)}
                braces={rowRateValues}
                data-tooltip={byteRate ? `${formatBytesCompact(byteRate)}/s` : 'Unknown byte rate'}
              />
            );
          },
        },
        {
          Header: 'Timing',
          id: 'timing',
          accessor: row => row.startTime,
          width: 170,
          Cell({ value, original }) {
            const { duration, phase } = original;
            if (!value) return null;

            const sinceQueryStart =
              new Date(value).valueOf() - (executionStartTime?.valueOf() || 0);

            return (
              <div
                className="timing-value"
                data-tooltip={assemble(
                  `Start: T+${formatDurationWithMs(sinceQueryStart)} (${value})`,
                  duration ? `Duration: ${formatDurationWithMs(duration)}` : undefined,
                ).join('\n')}
              >
                {!!executionDuration && executionDuration > 0 && (
                  <div
                    className="timing-bar"
                    style={{
                      left: `${(sinceQueryStart / executionDuration) * 100}%`,
                      width: `max(${(duration / executionDuration) * 100}%, 1px)`,
                    }}
                  />
                )}
                <div>{`Start: T+${formatDurationWithMsIfNeeded(sinceQueryStart)}`}</div>
                <div>Duration: {duration ? formatDurationWithMsIfNeeded(duration) : ''}</div>
                {phase && (
                  <div className="detail-line">{capitalizeFirst(phase.replace(/_/g, ' '))}</div>
                )}
              </div>
            );
          },
        },
        {
          Header: twoLines(
            'CPU utilization',
            <i>
              <span className="cpu-label">Counter</span>
              <span className="cpu-counter">Wall time</span>
            </i>,
          ),
          id: 'cpu',
          accessor: () => null,
          className: 'padded',
          width: 240,
          show: stages.hasCounter('cpu'),
          Cell({ original }) {
            const cpuTotals = stages.getCpuTotalsForStage(original);
            return (
              <>
                {filterMap(CPUS_COUNTER_FIELDS, k => {
                  const v = cpuTotals[k];
                  if (!v) return;
                  const fieldTitle = cpusCounterFieldTitle(k);
                  return (
                    <div
                      key={k}
                      data-tooltip={`${fieldTitle}\nCPU time: ${formatDurationWithMs(v.cpu / 1e6)}`}
                    >
                      <span className="cpu-label">{fieldTitle}</span>
                      <span className="cpu-counter">{formatDurationWithMs(v.wall / 1e6)}</span>
                    </div>
                  );
                })}
              </>
            );
          },
        },
        {
          Header: twoLines('Num', 'workers'),
          accessor: 'workerCount',
          className: 'padded',
          width: 75,
          Cell({ value, original }) {
            const inactiveWorkers = stages.getInactiveWorkerCount(original);
            if (inactiveWorkers) {
              return (
                <div>
                  <div>{formatInteger(value)}</div>
                  <div
                    className="detail-line"
                    data-tooltip="Workers are counted as active once they report any activity."
                  >{`${formatInteger(inactiveWorkers)} inactive`}</div>
                </div>
              );
            } else {
              return formatInteger(value);
            }
          },
        },
        {
          Header: 'Output',
          accessor: 'partitionCount',
          className: 'padded',
          width: 120,
          Cell({ value, original }) {
            const { shuffle } = original;
            const clusterBy: ClusterBy | undefined = deepGet(
              original,
              'definition.shuffleSpec.clusterBy',
            );
            return (
              <div data-tooltip={clusterBy ? formatClusterBy(clusterBy) : 'No clusterBy'}>
                <div>{shuffle}</div>
                <div className="detail-line">{original.output}</div>
                {typeof value === 'number' ? (
                  <div className="detail-line">{pluralIfNeeded(value, 'partition')}</div>
                ) : undefined}
              </div>
            );
          },
        },
      ]}
    />
  );
});
