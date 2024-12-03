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
import * as JSONBig from 'json-bigint-native';
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
    `Interval: ${tableStageInput.intervals.join('; ')}`,
    tableStageInput.filter && `Filter: ${JSONBig.stringify(tableStageInput.filter)}`,
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
  goToTask(taskId: string): void;
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

    const bracesRows: Record<ChannelCounterName, string[]> = {} as any;
    const bracesExtra: Record<ChannelCounterName, string[]> = {} as any;
    for (const counterName of counterNames) {
      bracesRows[counterName] = wideCounters.map(wideCounter =>
        formatRows(wideCounter[counterName]!.rows),
      );
      bracesExtra[counterName] = filterMap(wideCounters, wideCounter => {
        const totalFiles = wideCounter[counterName]!.totalFiles;
        if (!totalFiles) return;
        return formatFileOfTotalForBrace(totalFiles, totalFiles);
      });
    }

    const isSegmentGenerator = Stages.stageType(stage) === 'segmentGenerator';
    let bracesSegmentRowsMerged: string[] = [];
    let bracesSegmentRowsPushed: string[] = [];
    if (isSegmentGenerator) {
      bracesSegmentRowsMerged = wideCounters.map(wideCounter =>
        formatRows(wideCounter.segmentGenerationProgress?.rowsMerged || 0),
      );
      bracesSegmentRowsPushed = wideCounters.map(wideCounter =>
        formatRows(wideCounter.segmentGenerationProgress?.rowsPushed || 0),
      );
    }

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
            width: 95,
            Cell({ value }) {
              const taskId = `${execution.id}-worker${value}_0`;
              return (
                <TableClickableCell
                  hoverIcon={IconNames.SHARE}
                  tooltip={`Go to task: ${taskId}`}
                  onClick={() => {
                    goToTask(taskId);
                  }}
                >{`Worker${value}`}</TableClickableCell>
              );
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
        ].concat(
          counterNames.map((counterName, i) => {
            const isInput = counterName.startsWith('input');
            return {
              Header: twoLines(
                isInput ? (
                  <span>{inputLabelContent(stage, i)}</span>
                ) : (
                  stages.getStageCounterTitle(stage, counterName)
                ),
                isInput ? <i>rows &nbsp; (input files)</i> : <i>rows</i>,
              ),
              id: counterName,
              accessor: d => d[counterName]!.rows,
              className: 'padded',
              width: 200,
              Cell({ value, original }) {
                const c = (original as SimpleWideCounter)[counterName]!;
                return (
                  <>
                    <BracedText
                      text={formatRows(value)}
                      braces={bracesRows[counterName]}
                      data-tooltip={
                        c.bytes
                          ? `Uncompressed size: ${formatBytesCompact(c.bytes)} ${NOT_SIZE_ON_DISK}`
                          : NO_SIZE_INFO
                      }
                    />
                    {Boolean(c.totalFiles) && (
                      <>
                        {' '}
                        &nbsp;{' '}
                        <BracedText
                          text={formatFileOfTotal(c.files, c.totalFiles)}
                          braces={bracesExtra[counterName]}
                        />
                      </>
                    )}
                  </>
                );
              },
            };
          }),
          Stages.stageType(stage) === 'segmentGenerator'
            ? [
                {
                  Header: twoLines('Merged', <i>rows</i>),
                  id: 'segmentGeneration_rowsMerged',
                  accessor: d => d.segmentGenerationProgress?.rowsMerged || 0,
                  className: 'padded',
                  width: 180,
                  Cell({ value }) {
                    return <BracedText text={formatRows(value)} braces={bracesSegmentRowsMerged} />;
                  },
                },
                {
                  Header: twoLines('Pushed', <i>rows</i>),
                  id: 'segmentGeneration_rowsPushed',
                  accessor: d => d.segmentGenerationProgress?.rowsPushed || 0,
                  className: 'padded',
                  width: 180,
                  Cell({ value }) {
                    return <BracedText text={formatRows(value)} braces={bracesSegmentRowsPushed} />;
                  },
                },
              ]
            : [],
        )}
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
        {inputFileCount ? (
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
        ) : undefined}
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
