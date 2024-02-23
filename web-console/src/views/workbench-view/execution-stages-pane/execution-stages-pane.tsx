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
import { Tooltip2 } from '@blueprintjs/popover2';
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
  SegmentGenerationProgressFields,
  SimpleWideCounter,
  StageDefinition,
  StageInput,
} from '../../../druid-models';
import { formatClusterBy, Stages, summarizeInputSource } from '../../../druid-models';
import { DEFAULT_TABLE_CLASS_NAME } from '../../../react-table';
import type { NumberLike } from '../../../utils';
import {
  assemble,
  capitalizeFirst,
  clamp,
  deepGet,
  filterMap,
  formatBytesCompact,
  formatDuration,
  formatDurationWithMs,
  formatInteger,
  formatPercent,
  oneOf,
  prettyFormatIsoDate,
  twoLines,
} from '../../../utils';

import './execution-stages-pane.scss';

const MAX_STAGE_ROWS = 20;
const MAX_DETAIL_ROWS = 20;
const NOT_SIZE_ON_DISK = '(does not represent size on disk)';

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
const formatDurationDynamic = (n: NumberLike) =>
  n < 1000 ? formatDurationWithMs(n) : formatDuration(n);

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
        <span className="datasource" title={summarizeTableInput(stageInput)}>
          {stageInput.dataSource}
        </span>
      )}
      {stageInput.type === 'external' && (
        <span className="external" title={summarizeInputSource(stageInput.inputSource, true)}>
          {`${stageInput.inputSource.type} external`}
        </span>
      )}
      {broadcast?.includes(inputIndex) && (
        <Icon
          className="broadcast-tag"
          icon={IconNames.CELL_TOWER}
          title="This input is being broadcast to all workers in this stage."
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
        {detailedCountersForPartitions(stage, 'input', phase === 'READING_INPUT')}
        {detailedCountersForWorkers(stage)}
        {detailedCountersForPartitions(stage, 'output', phaseIsWorking)}
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
            width: 100,
            Cell({ value }) {
              const taskId = `${execution.id}-worker${value}_0`;
              return (
                <TableClickableCell
                  hoverIcon={IconNames.SHARE}
                  title={`Go to task: ${taskId}`}
                  onClick={() => {
                    goToTask(taskId);
                  }}
                >{`Worker${value}`}</TableClickableCell>
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
              width: 180,
              Cell({ value, original }) {
                const c = (original as SimpleWideCounter)[counterName]!;
                return (
                  <>
                    <BracedText
                      text={formatRows(value)}
                      braces={bracesRows[counterName]}
                      title={
                        c.bytes
                          ? `Uncompressed size: ${formatBytesCompact(c.bytes)} ${NOT_SIZE_ON_DISK}`
                          : undefined
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

  function detailedCountersForPartitions(
    stage: StageDefinition,
    type: 'input' | 'output',
    inProgress: boolean,
  ) {
    const wideCounters = stages.getByPartitionCountersForStage(stage, type);
    if (!wideCounters.length) return;

    const counterNames: ChannelCounterName[] = stages.getPartitionChannelCounterNamesForStage(
      stage,
      type,
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
            Header: `${capitalizeFirst(type)} partitions` + (inProgress ? '*' : ''),
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
                    title={
                      c.bytes
                        ? `Uncompressed size: ${formatBytesCompact(c.bytes)} ${NOT_SIZE_ON_DISK}`
                        : undefined
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
    return (
      <div
        className="data-transfer"
        key={inputNumber}
        title={
          bytes
            ? `${formatInputLabel(
                stage,
                inputNumber,
              )} (input${inputNumber}) uncompressed size: ${formatBytesCompact(
                bytes,
              )} ${NOT_SIZE_ON_DISK}`
            : undefined
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
        title={`${title} frames: ${formatFrames(
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
        title={`${title} frames: ${formatFrames(
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
      <div className="data-transfer">
        <BracedText
          text={formatRows(stages.getTotalSegmentGenerationProgressForStage(stage, field))}
          braces={rowsValues}
        />
      </div>
    );
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
          Header: twoLines('Stage', <i>processorType</i>),
          id: 'stage',
          accessor: 'stageNumber',
          className: 'padded',
          width: 140,
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
                {stage.sort && <div className="sort-marker">(with sort)</div>}
                {(myError || warnings > 0) && (
                  <div className="error-warning">
                    {myError && (
                      <Tooltip2
                        content={
                          <div>
                            {(error.error.errorCode ? `${error.error.errorCode}: ` : '') +
                              error.error.errorMessage}
                          </div>
                        }
                      >
                        <Button
                          minimal
                          small
                          icon={IconNames.ERROR}
                          intent={Intent.DANGER}
                          onClick={onErrorClick}
                        />
                      </Tooltip2>
                    )}
                    {myError && warnings > 0 && ' '}
                    {warnings > 0 && (
                      <Tooltip2
                        content={
                          <pre>{formatBreakdown(stages.getWarningBreakdownForStage(stage))}</pre>
                        }
                      >
                        <Button
                          minimal
                          small
                          icon={IconNames.WARNING_SIGN}
                          text={warnings > 1 ? `${warnings}` : undefined}
                          intent={Intent.WARNING}
                          onClick={onWarningClick}
                        />
                      </Tooltip2>
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
          width: 160,
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
                text={formatRowRate(value)}
                braces={rowRateValues}
                title={byteRate ? `${formatBytesCompact(byteRate)}/s` : undefined}
              />
            );
          },
        },
        {
          Header: 'Phase',
          id: 'phase',
          accessor: row => (row.phase ? capitalizeFirst(row.phase.replace(/_/g, ' ')) : ''),
          className: 'padded',
          width: 130,
        },
        {
          Header: 'Timing',
          id: 'timing',
          accessor: row => row.startTime,
          className: 'padded',
          width: 170,
          Cell({ value, original }) {
            const duration: number | undefined = original.duration;
            if (!value) return null;
            return (
              <div title={value + (duration ? `/${formatDurationWithMs(duration)}` : '')}>
                <div>{prettyFormatIsoDate(value)}</div>
                <div>{duration ? formatDurationDynamic(duration) : ''}</div>
              </div>
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
          Header: twoLines('Output', 'partitions'),
          accessor: 'partitionCount',
          className: 'padded',
          width: 75,
        },
        {
          Header: 'Cluster by',
          id: 'clusterBy',
          className: 'padded',
          minWidth: 400,
          accessor: row => formatClusterBy(deepGet(row, 'definition.shuffleSpec.clusterBy')),
          Cell({ value, original }) {
            const clusterBy: ClusterBy | undefined = deepGet(
              original,
              'definition.shuffleSpec.clusterBy',
            );
            if (!clusterBy) return null;
            if (clusterBy.bucketByCount) {
              return (
                <>
                  <div>{`Partition by: ${formatClusterBy(clusterBy, 'partition')}`}</div>
                  <div>{`Cluster by: ${formatClusterBy(clusterBy, 'cluster')}`}</div>
                </>
              );
            } else {
              return <div title={value}>{value}</div>;
            }
          },
        },
      ]}
    />
  );
});
