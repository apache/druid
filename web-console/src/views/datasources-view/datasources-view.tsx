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

import { Button, FormGroup, InputGroup, Intent, MenuItem, Switch, Tag } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import { sum } from 'd3-array';
import { SqlQuery, T } from 'druid-query-toolkit';
import React from 'react';
import type { Filter } from 'react-table';
import ReactTable from 'react-table';

import {
  ACTION_COLUMN_ID,
  ACTION_COLUMN_LABEL,
  ACTION_COLUMN_WIDTH,
  ActionCell,
  BracedText,
  MoreButton,
  RefreshButton,
  SegmentTimeline,
  SplitterLayout,
  TableClickableCell,
  TableColumnSelector,
  type TableColumnSelectorColumn,
  ViewControlBar,
} from '../../components';
import {
  AsyncActionDialog,
  CompactionConfigDialog,
  KillDatasourceDialog,
  RetentionDialog,
} from '../../dialogs';
import { DatasourceTableActionDialog } from '../../dialogs/datasource-table-action-dialog/datasource-table-action-dialog';
import type {
  CompactionConfig,
  CompactionConfigs,
  CompactionInfo,
  CompactionStatus,
  QueryWithContext,
  Rule,
} from '../../druid-models';
import {
  END_OF_TIME_DATE,
  formatCompactionInfo,
  getDatasourceColor,
  RuleUtil,
  START_OF_TIME_DATE,
  zeroCompactionStatus,
} from '../../druid-models';
import type { Capabilities, CapabilitiesMode } from '../../helpers';
import { STANDARD_TABLE_PAGE_SIZE, STANDARD_TABLE_PAGE_SIZE_OPTIONS } from '../../react-table';
import { Api, AppToaster } from '../../singletons';
import type { AuxiliaryQueryFn, NumberLike } from '../../utils';
import {
  assemble,
  compact,
  countBy,
  deepGet,
  findMap,
  formatBytes,
  formatInteger,
  formatMillions,
  formatPercent,
  getApiArray,
  getDruidErrorMessage,
  groupByAsMap,
  hasOverlayOpen,
  isNumberLikeNaN,
  LocalStorageBackedVisibility,
  LocalStorageKeys,
  lookupBy,
  moveToEnd,
  pluralIfNeeded,
  queryDruidSql,
  QueryManager,
  QueryState,
  ResultWithAuxiliaryWork,
  twoLines,
} from '../../utils';
import type { BasicAction } from '../../utils/basic-action';

import './datasources-view.scss';

const TABLE_COLUMNS_BY_MODE: Record<CapabilitiesMode, TableColumnSelectorColumn[]> = {
  'full': [
    'Datasource name',
    'Availability',
    'Historical load/drop queues',
    'Total data size',
    { text: 'Running tasks', label: 'sys.tasks' },
    'Segment rows',
    'Segment size',
    { text: 'Segment granularity', label: '𝑓(sys.segments)' },
    'Total rows',
    'Avg. row size',
    'Replicated size',
    { text: 'Compaction', label: 'compaction API' },
    { text: '% Compacted', label: 'compaction API' },
    { text: 'Left to be compacted', label: 'compaction API' },
    { text: 'Retention', label: 'rules API' },
  ],
  'no-sql': [
    'Datasource name',
    'Availability',
    'Historical load/drop queues',
    'Total data size',
    { text: 'Running tasks', label: 'tasks API' },
    { text: 'Compaction', label: 'compaction API' },
    { text: '% Compacted', label: 'compaction API' },
    { text: 'Left to be compacted', label: 'compaction API' },
    { text: 'Retention', label: 'rules API' },
  ],
  'no-proxy': [
    'Datasource name',
    'Availability',
    'Historical load/drop queues',
    'Total data size',
    { text: 'Running tasks', label: 'sys.tasks' },
    'Segment rows',
    'Segment size',
    { text: 'Segment granularity', label: '𝑓(sys.segments)' },
    'Total rows',
    'Avg. row size',
    'Replicated size',
  ],
};

function formatLoadDrop(segmentsToLoad: NumberLike, segmentsToDrop: NumberLike): string {
  const loadDrop: string[] = [];
  if (segmentsToLoad) {
    loadDrop.push(`${pluralIfNeeded(segmentsToLoad, 'segment')} to load`);
  }
  if (segmentsToDrop) {
    loadDrop.push(`${pluralIfNeeded(segmentsToDrop, 'segment')} to drop`);
  }
  return loadDrop.join(', ') || 'No segments to load/drop';
}

const formatTotalDataSize = formatBytes;
const formatSegmentRows = formatMillions;
const formatSegmentSize = formatBytes;
const formatTotalRows = formatInteger;
const formatAvgRowSize = formatInteger;
const formatReplicatedSize = formatBytes;
const formatLeftToBeCompacted = formatBytes;

function progress(done: number, awaiting: number): number {
  const d = done + awaiting;
  if (!d) return 0;
  return done / d;
}

const PERCENT_BRACES = [formatPercent(1)];

interface DatasourceQueryResultRow {
  readonly datasource: string;
  readonly num_segments: number;
  readonly num_zero_replica_segments: number;
  readonly num_segments_to_load: number;
  readonly num_segments_to_drop: number;
  readonly minute_aligned_segments: number;
  readonly hour_aligned_segments: number;
  readonly day_aligned_segments: number;
  readonly month_aligned_segments: number;
  readonly year_aligned_segments: number;
  readonly all_granularity_segments: number;
  readonly total_data_size: NumberLike;
  readonly replicated_size: NumberLike;
  readonly min_segment_rows: NumberLike;
  readonly avg_segment_rows: NumberLike;
  readonly max_segment_rows: NumberLike;
  readonly min_segment_size: NumberLike;
  readonly avg_segment_size: NumberLike;
  readonly max_segment_size: NumberLike;
  readonly total_rows: NumberLike;
  readonly avg_row_size: NumberLike;
}

function makeEmptyDatasourceQueryResultRow(datasource: string): DatasourceQueryResultRow {
  return {
    datasource,
    num_segments: 0,
    num_zero_replica_segments: 0,
    num_segments_to_load: 0,
    num_segments_to_drop: 0,
    minute_aligned_segments: 0,
    hour_aligned_segments: 0,
    day_aligned_segments: 0,
    month_aligned_segments: 0,
    year_aligned_segments: 0,
    all_granularity_segments: 0,
    total_data_size: 0,
    replicated_size: 0,
    min_segment_rows: 0,
    avg_segment_rows: 0,
    max_segment_rows: 0,
    min_segment_size: 0,
    avg_segment_size: 0,
    max_segment_size: 0,
    total_rows: 0,
    avg_row_size: 0,
  };
}

function segmentGranularityCountsToRank(row: DatasourceQueryResultRow): number {
  if (row.all_granularity_segments) return 7;
  if (row.year_aligned_segments) return 6;
  if (row.month_aligned_segments) return 5;
  if (row.day_aligned_segments) return 4;
  if (row.hour_aligned_segments) return 3;
  if (row.minute_aligned_segments) return 2;
  if (row.num_segments) return 1;
  return 0;
}

interface Datasource extends DatasourceQueryResultRow {
  readonly runningTasks?: Record<string, number>;
  readonly rules?: Rule[];
  readonly compaction?: CompactionInfo;
  readonly unused?: boolean;
}

function makeUnusedDatasource(datasource: string): Datasource {
  return { ...makeEmptyDatasourceQueryResultRow(datasource), rules: [], unused: true };
}

interface DatasourcesAndDefaultRules {
  readonly datasources: Datasource[];
  readonly defaultRules?: Rule[];
}

interface RetentionDialogOpenOn {
  readonly datasource: string;
  readonly rules: Rule[];
  readonly defaultRules: Rule[];
}

interface CompactionConfigDialogOpenOn {
  readonly datasource: string;
  readonly compactionConfig?: CompactionConfig;
}

interface DatasourceQuery {
  capabilities: Capabilities;
  visibleColumns: LocalStorageBackedVisibility;
  showUnused: boolean;
}

interface RunningTaskRow {
  datasource: string;
  type: string;
  num_running_tasks: number;
}

function countRunningTasks(runningTasks: Record<string, number> | undefined): number {
  if (!runningTasks) return -1;
  return sum(Object.values(runningTasks));
}

function formatRunningTasks(runningTasks: Record<string, number>): string {
  const runningTaskEntries = Object.entries(runningTasks);
  if (!runningTaskEntries.length) return 'No running tasks';
  return moveToEnd(
    runningTaskEntries.sort(([t1, c1], [t2, c2]) => {
      const dc = c2 - c1;
      if (dc) return dc;
      return t1.localeCompare(t2);
    }),
    ([t]) => t === 'other',
  )
    .map(kv => kv.join(': '))
    .join(', ');
}

function normalizeTaskType(taskType: string): string {
  switch (taskType) {
    case 'index_parallel':
    case 'index_hadoop':
    case 'index_kafka':
    case 'index_kinesis':
    case 'compact':
    case 'kill':
      return taskType;

    default:
      return 'other';
  }
}

export interface DatasourcesViewProps {
  filters: Filter[];
  onFiltersChange(filters: Filter[]): void;
  goToQuery(queryWithContext: QueryWithContext): void;
  goToTasks(datasource?: string): void;
  goToSegments(options: {
    start?: Date;
    end?: Date;
    datasource?: string;
    realtime?: boolean;
  }): void;
  capabilities: Capabilities;
}

export interface DatasourcesViewState {
  datasourcesAndDefaultRulesState: QueryState<DatasourcesAndDefaultRules>;

  showUnused: boolean;
  retentionDialogOpenOn?: RetentionDialogOpenOn;
  compactionDialogOpenOn?: CompactionConfigDialogOpenOn;
  datasourceToMarkAsUnusedAllSegmentsIn?: string;
  datasourceToMarkAllNonOvershadowedSegmentsAsUsedIn?: string;
  killDatasource?: string;
  datasourceToMarkSegmentsByIntervalIn?: string;
  useUnuseAction: 'use' | 'unuse';
  useUnuseInterval: string;
  showForceCompact: boolean;
  visibleColumns: LocalStorageBackedVisibility;
  showSegmentTimeline?: { capabilities: Capabilities; datasource?: string };

  datasourceTableActionDialogId?: string;
  actions: BasicAction[];
}

export class DatasourcesView extends React.PureComponent<
  DatasourcesViewProps,
  DatasourcesViewState
> {
  static UNUSED_COLOR = '#0a1500';
  static EMPTY_COLOR = '#868686';
  static FULLY_AVAILABLE_COLOR = '#57d500';
  static PARTIALLY_AVAILABLE_COLOR = '#ffbf00';

  static query(visibleColumns: LocalStorageBackedVisibility) {
    const columns = compact(
      [
        visibleColumns.shown('Datasource name') && `datasource`,
        visibleColumns.shown('Availability', 'Segment granularity') && [
          `COUNT(*) FILTER (WHERE is_active = 1) AS num_segments`,
          `COUNT(*) FILTER (WHERE is_published = 1 AND is_overshadowed = 0 AND replication_factor = 0) AS num_zero_replica_segments`,
        ],
        visibleColumns.shown('Availability', 'Historical load/drop queues') && [
          `COUNT(*) FILTER (WHERE is_published = 1 AND is_overshadowed = 0 AND is_available = 0 AND replication_factor > 0) AS num_segments_to_load`,
          `COUNT(*) FILTER (WHERE is_available = 1 AND is_active = 0) AS num_segments_to_drop`,
        ],
        visibleColumns.shown('Total data size') &&
          `SUM("size") FILTER (WHERE is_active = 1) AS total_data_size`,
        visibleColumns.shown('Segment rows') && [
          `MIN("num_rows") FILTER (WHERE is_available = 1 AND is_realtime = 0) AS min_segment_rows`,
          `AVG("num_rows") FILTER (WHERE is_available = 1 AND is_realtime = 0) AS avg_segment_rows`,
          `MAX("num_rows") FILTER (WHERE is_available = 1 AND is_realtime = 0) AS max_segment_rows`,
        ],
        visibleColumns.shown('Segment size') && [
          `MIN("size") FILTER (WHERE is_active = 1 AND is_realtime = 0) AS min_segment_size`,
          `AVG("size") FILTER (WHERE is_active = 1 AND is_realtime = 0) AS avg_segment_size`,
          `MAX("size") FILTER (WHERE is_active = 1 AND is_realtime = 0) AS max_segment_size`,
        ],
        visibleColumns.shown('Segment granularity') && [
          `COUNT(*) FILTER (WHERE is_active = 1 AND "start" LIKE '%:00.000Z' AND "end" LIKE '%:00.000Z') AS minute_aligned_segments`,
          `COUNT(*) FILTER (WHERE is_active = 1 AND "start" LIKE '%:00:00.000Z' AND "end" LIKE '%:00:00.000Z') AS hour_aligned_segments`,
          `COUNT(*) FILTER (WHERE is_active = 1 AND "start" LIKE '%T00:00:00.000Z' AND "end" LIKE '%T00:00:00.000Z') AS day_aligned_segments`,
          `COUNT(*) FILTER (WHERE is_active = 1 AND "start" LIKE '%-01T00:00:00.000Z' AND "end" LIKE '%-01T00:00:00.000Z') AS month_aligned_segments`,
          `COUNT(*) FILTER (WHERE is_active = 1 AND "start" LIKE '%-01-01T00:00:00.000Z' AND "end" LIKE '%-01-01T00:00:00.000Z') AS year_aligned_segments`,
          `COUNT(*) FILTER (WHERE is_active = 1 AND "start" = '${START_OF_TIME_DATE}' AND "end" = '${END_OF_TIME_DATE}') AS all_granularity_segments`,
        ],
        visibleColumns.shown('Total rows') &&
          `SUM("num_rows") FILTER (WHERE is_active = 1) AS total_rows`,
        visibleColumns.shown('Avg. row size') &&
          `CASE WHEN SUM("num_rows") FILTER (WHERE is_available = 1) <> 0 THEN (SUM("size") FILTER (WHERE is_available = 1) / SUM("num_rows") FILTER (WHERE is_available = 1)) ELSE 0 END AS avg_row_size`,
        visibleColumns.shown('Replicated size') &&
          `SUM("size" * "num_replicas") FILTER (WHERE is_active = 1) AS replicated_size`,
      ].flat(),
    );

    if (!columns.length) {
      columns.push(`datasource`);
    }

    return `SELECT
${columns.join(',\n')}
FROM sys.segments
GROUP BY datasource
ORDER BY datasource`;
  }

  static RUNNING_TASK_SQL = `SELECT
  "datasource", "type", COUNT(*) AS "num_running_tasks"
FROM sys.tasks WHERE "status" = 'RUNNING' AND "runner_status" = 'RUNNING'
GROUP BY 1, 2`;

  static formatRules(rules: Rule[]): string {
    if (rules.length === 0) {
      return 'No rules';
    } else if (rules.length <= 2) {
      return rules.map(RuleUtil.ruleToString).join(', ');
    } else {
      return `${RuleUtil.ruleToString(rules[0])} +${rules.length - 1} more rules`;
    }
  }

  private readonly datasourceQueryManager: QueryManager<
    DatasourceQuery,
    DatasourcesAndDefaultRules
  >;

  constructor(props: DatasourcesViewProps) {
    super(props);

    this.state = {
      datasourcesAndDefaultRulesState: QueryState.INIT,

      showUnused: false,
      useUnuseAction: 'unuse',
      useUnuseInterval: '',
      showForceCompact: false,
      visibleColumns: new LocalStorageBackedVisibility(
        LocalStorageKeys.DATASOURCE_TABLE_COLUMN_SELECTION,
        ['Segment size', 'Segment granularity'],
      ),

      actions: [],
    };

    this.datasourceQueryManager = new QueryManager<DatasourceQuery, DatasourcesAndDefaultRules>({
      processQuery: async (
        { capabilities, visibleColumns, showUnused },
        cancelToken,
        setIntermediateQuery,
      ) => {
        let datasources: DatasourceQueryResultRow[];
        if (capabilities.hasSql()) {
          const query = DatasourcesView.query(visibleColumns);
          setIntermediateQuery(query);
          datasources = await queryDruidSql({ query }, cancelToken);
        } else if (capabilities.hasCoordinatorAccess()) {
          const datasourcesResp = await getApiArray(
            '/druid/coordinator/v1/datasources?simple',
            cancelToken,
          );
          const loadstatusResp = await Api.instance.get('/druid/coordinator/v1/loadstatus?simple', {
            cancelToken,
          });
          const loadstatus = loadstatusResp.data;
          datasources = datasourcesResp.map((d: any): DatasourceQueryResultRow => {
            const totalDataSize = deepGet(d, 'properties.segments.size') || -1;
            const segmentsToLoad = Number(loadstatus[d.name] || 0);
            const availableSegments = Number(deepGet(d, 'properties.segments.count'));
            const numSegments = availableSegments + segmentsToLoad;
            return {
              datasource: d.name,
              num_segments: numSegments,
              num_zero_replica_segments: 0,
              num_segments_to_load: segmentsToLoad,
              num_segments_to_drop: 0,
              minute_aligned_segments: -1,
              hour_aligned_segments: -1,
              day_aligned_segments: -1,
              month_aligned_segments: -1,
              year_aligned_segments: -1,
              all_granularity_segments: -1,
              replicated_size: -1,
              total_data_size: totalDataSize,
              min_segment_rows: -1,
              avg_segment_rows: -1,
              max_segment_rows: -1,
              min_segment_size: -1,
              avg_segment_size: -1,
              max_segment_size: -1,
              total_rows: -1,
              avg_row_size: -1,
            };
          });
        } else {
          throw new Error(`must have SQL or coordinator access`);
        }

        const auxiliaryQueries: AuxiliaryQueryFn<DatasourcesAndDefaultRules>[] = [];

        if (visibleColumns.shown('Running tasks')) {
          if (capabilities.hasSql()) {
            auxiliaryQueries.push(async (datasourcesAndDefaultRules, cancelToken) => {
              try {
                const runningTasks = await queryDruidSql<RunningTaskRow>(
                  {
                    query: DatasourcesView.RUNNING_TASK_SQL,
                  },
                  cancelToken,
                );

                const runningTasksByDatasource = groupByAsMap(
                  runningTasks,
                  x => x.datasource,
                  xs =>
                    groupByAsMap(
                      xs,
                      x => normalizeTaskType(x.type),
                      ys => sum(ys, y => y.num_running_tasks),
                    ),
                );

                return {
                  ...datasourcesAndDefaultRules,
                  datasources: datasourcesAndDefaultRules.datasources.map(ds => ({
                    ...ds,
                    runningTasks: runningTasksByDatasource[ds.datasource] || {},
                  })),
                };
              } catch {
                AppToaster.show({
                  icon: IconNames.ERROR,
                  intent: Intent.DANGER,
                  message: 'Could not get running task counts',
                });
                return datasourcesAndDefaultRules;
              }
            });
          } else if (capabilities.hasOverlordAccess()) {
            auxiliaryQueries.push(async (datasourcesAndDefaultRules, cancelToken) => {
              try {
                const taskList = await getApiArray(
                  `/druid/indexer/v1/tasks?state=running`,
                  cancelToken,
                );

                const runningTasksByDatasource = groupByAsMap(
                  taskList,
                  (t: any) => t.dataSource,
                  xs =>
                    groupByAsMap(
                      xs,
                      x => normalizeTaskType(x.type),
                      ys => ys.length,
                    ),
                );

                return {
                  ...datasourcesAndDefaultRules,
                  datasources: datasourcesAndDefaultRules.datasources.map(ds => ({
                    ...ds,
                    runningTasks: runningTasksByDatasource[ds.datasource] || {},
                  })),
                };
              } catch {
                AppToaster.show({
                  icon: IconNames.ERROR,
                  intent: Intent.DANGER,
                  message: 'Could not get running task counts',
                });
                return datasourcesAndDefaultRules;
              }
            });
          }
        }

        let unused: string[] = [];
        if (capabilities.hasCoordinatorAccess()) {
          // Unused
          const seen = countBy(datasources, x => x.datasource);
          if (showUnused) {
            try {
              unused = (
                await getApiArray<string>(
                  '/druid/coordinator/v1/metadata/datasources?includeUnused',
                )
              ).filter(d => !seen[d]);
            } catch {
              AppToaster.show({
                icon: IconNames.ERROR,
                intent: Intent.DANGER,
                message: 'Could not get the list of unused datasources',
              });
            }
          }

          // Rules
          auxiliaryQueries.push(async (datasourcesAndDefaultRules, cancelToken) => {
            try {
              const rules = (
                await Api.instance.get<Record<string, Rule[]>>('/druid/coordinator/v1/rules', {
                  cancelToken,
                })
              ).data;

              return {
                datasources: datasourcesAndDefaultRules.datasources.map(ds => ({
                  ...ds,
                  rules: rules[ds.datasource] || [],
                })),
                defaultRules: rules[RuleUtil.DEFAULT_RULES_KEY],
              };
            } catch {
              AppToaster.show({
                icon: IconNames.ERROR,
                intent: Intent.DANGER,
                message: 'Could not get load rules',
              });
              return datasourcesAndDefaultRules;
            }
          });

          // Compaction
          auxiliaryQueries.push(async (datasourcesAndDefaultRules, cancelToken) => {
            try {
              const compactionConfigsAndMore = (
                await Api.instance.get<CompactionConfigs>(
                  '/druid/indexer/v1/compaction/config/datasources',
                  { cancelToken },
                )
              ).data;
              const compactionConfigs = lookupBy(
                compactionConfigsAndMore.compactionConfigs || [],
                c => c.dataSource,
              );

              const compactionStatusesResp = await Api.instance.get<{
                latestStatus: CompactionStatus[];
              }>('/druid/indexer/v1/compaction/status/datasources', { cancelToken });
              const compactionStatuses = lookupBy(
                compactionStatusesResp.data.latestStatus || [],
                c => c.dataSource,
              );

              return {
                ...datasourcesAndDefaultRules,
                datasources: datasourcesAndDefaultRules.datasources.map(ds => ({
                  ...ds,
                  compaction: {
                    config: compactionConfigs[ds.datasource],
                    status: compactionStatuses[ds.datasource],
                  },
                })),
              };
            } catch {
              AppToaster.show({
                icon: IconNames.ERROR,
                intent: Intent.DANGER,
                message: 'Could not get compaction information',
              });
              return datasourcesAndDefaultRules;
            }
          });
        }

        return new ResultWithAuxiliaryWork(
          {
            datasources: datasources.concat(unused.map(makeUnusedDatasource)),
          },
          auxiliaryQueries,
        );
      },
      onStateChange: datasourcesAndDefaultRulesState => {
        this.setState({
          datasourcesAndDefaultRulesState,
        });
      },
    });
  }

  private readonly refresh = (auto: boolean): void => {
    if (auto && hasOverlayOpen()) return;
    this.datasourceQueryManager.rerunLastQuery(auto);

    const { showSegmentTimeline } = this.state;
    if (showSegmentTimeline) {
      // Create a new capabilities object to force the segment timeline to re-render
      this.setState(({ showSegmentTimeline }) => ({
        showSegmentTimeline: {
          ...showSegmentTimeline,
          capabilities: this.props.capabilities.clone(),
        },
      }));
    }
  };

  private readonly fetchData = () => {
    const { capabilities } = this.props;
    const { visibleColumns, showUnused } = this.state;
    this.datasourceQueryManager.runQuery({ capabilities, visibleColumns, showUnused });
  };

  componentDidMount(): void {
    this.fetchData();
  }

  componentWillUnmount(): void {
    this.datasourceQueryManager.terminate();
  }

  renderUnuseAction() {
    const { datasourceToMarkAsUnusedAllSegmentsIn } = this.state;
    if (!datasourceToMarkAsUnusedAllSegmentsIn) return;

    return (
      <AsyncActionDialog
        action={async () => {
          const resp = await Api.instance.delete(
            `/druid/indexer/v1/datasources/${Api.encodePath(
              datasourceToMarkAsUnusedAllSegmentsIn,
            )}`,
            {},
          );
          return resp.data;
        }}
        confirmButtonText="Mark as unused all segments"
        successText={
          <>
            All segments in datasource <Tag minimal>{datasourceToMarkAsUnusedAllSegmentsIn}</Tag>{' '}
            have been marked as unused
          </>
        }
        failText={
          <>
            Failed to mark as unused all segments in datasource{' '}
            <Tag minimal>{datasourceToMarkAsUnusedAllSegmentsIn}</Tag>
          </>
        }
        intent={Intent.DANGER}
        onClose={() => {
          this.setState({ datasourceToMarkAsUnusedAllSegmentsIn: undefined });
        }}
        onSuccess={this.fetchData}
      >
        <p>
          {`Are you sure you want to mark as unused all segments in '${datasourceToMarkAsUnusedAllSegmentsIn}'?`}
        </p>
      </AsyncActionDialog>
    );
  }

  renderUseAction() {
    const { datasourceToMarkAllNonOvershadowedSegmentsAsUsedIn } = this.state;
    if (!datasourceToMarkAllNonOvershadowedSegmentsAsUsedIn) return;

    return (
      <AsyncActionDialog
        action={async () => {
          const resp = await Api.instance.post(
            `/druid/indexer/v1/datasources/${Api.encodePath(
              datasourceToMarkAllNonOvershadowedSegmentsAsUsedIn,
            )}`,
            {},
          );
          return resp.data;
        }}
        confirmButtonText="Mark as used all segments"
        successText={
          <>
            All non-overshadowed segments in datasource{' '}
            <Tag minimal>{datasourceToMarkAllNonOvershadowedSegmentsAsUsedIn}</Tag> have been marked
            as used
          </>
        }
        failText={
          <>
            Failed to mark as used all non-overshadowed segments in datasource{' '}
            <Tag minimal>{datasourceToMarkAllNonOvershadowedSegmentsAsUsedIn}</Tag>
          </>
        }
        intent={Intent.PRIMARY}
        onClose={() => {
          this.setState({ datasourceToMarkAllNonOvershadowedSegmentsAsUsedIn: undefined });
        }}
        onSuccess={this.fetchData}
      >
        <p>{`Are you sure you want to mark as used all non-overshadowed segments in '${datasourceToMarkAllNonOvershadowedSegmentsAsUsedIn}'?`}</p>
      </AsyncActionDialog>
    );
  }

  renderUseUnuseActionByInterval() {
    const { datasourceToMarkSegmentsByIntervalIn, useUnuseAction, useUnuseInterval } = this.state;
    if (!datasourceToMarkSegmentsByIntervalIn) return;
    const isUse = useUnuseAction === 'use';
    const usedWord = isUse ? 'used' : 'unused';
    return (
      <AsyncActionDialog
        action={async () => {
          if (!useUnuseInterval) return;
          const param = isUse ? 'markUsed' : 'markUnused';
          const resp = await Api.instance.post(
            `/druid/indexer/v1/datasources/${Api.encodePath(
              datasourceToMarkSegmentsByIntervalIn,
            )}/${Api.encodePath(param)}`,
            {
              interval: useUnuseInterval,
            },
          );
          return resp.data;
        }}
        confirmButtonText={`Mark as ${usedWord} segments in the interval`}
        confirmButtonDisabled={!/.\/./.test(useUnuseInterval)}
        successText={`Segments in the interval in datasource have been marked as ${usedWord}`}
        failText={`Failed to mark as ${usedWord} segments in the interval in datasource`}
        intent={Intent.PRIMARY}
        onClose={() => {
          this.setState({ datasourceToMarkSegmentsByIntervalIn: undefined });
        }}
        onSuccess={this.fetchData}
      >
        <p>{`Please select the interval in which you want to mark segments as ${usedWord} in '${datasourceToMarkSegmentsByIntervalIn}'?`}</p>
        <FormGroup>
          <InputGroup
            value={useUnuseInterval}
            onChange={(e: any) => {
              const v = e.target.value;
              this.setState({ useUnuseInterval: v.toUpperCase() });
            }}
            placeholder="2018-01-01T00:00:00/2018-01-03T00:00:00"
          />
        </FormGroup>
      </AsyncActionDialog>
    );
  }

  renderKillAction() {
    const { killDatasource } = this.state;
    if (!killDatasource) return;

    return (
      <KillDatasourceDialog
        datasource={killDatasource}
        onClose={() => {
          this.setState({ killDatasource: undefined });
        }}
        onSuccess={this.fetchData}
      />
    );
  }

  renderBulkDatasourceActions() {
    const { goToQuery, capabilities } = this.props;
    const lastDatasourcesQuery = this.datasourceQueryManager.getLastIntermediateQuery();

    return (
      <MoreButton
        altExtra={
          <MenuItem
            icon={IconNames.COMPRESSED}
            text="Force compaction run"
            label="(debug)"
            intent={Intent.DANGER}
            onClick={() => {
              this.setState({ showForceCompact: true });
            }}
          />
        }
      >
        {capabilities.hasSql() && (
          <MenuItem
            icon={IconNames.APPLICATION}
            text="View SQL query for table"
            disabled={typeof lastDatasourcesQuery !== 'string'}
            onClick={() => {
              if (typeof lastDatasourcesQuery !== 'string') return;
              goToQuery({ queryString: lastDatasourcesQuery });
            }}
          />
        )}
        <MenuItem
          icon={IconNames.EDIT}
          text="Edit default retention rules"
          onClick={this.editDefaultRules}
        />
      </MoreButton>
    );
  }

  renderForceCompactAction() {
    const { showForceCompact } = this.state;
    if (!showForceCompact) return;

    return (
      <AsyncActionDialog
        action={async () => {
          const resp = await Api.instance.post(`/druid/coordinator/v1/compaction/compact`, {});
          return resp.data;
        }}
        confirmButtonText="Force compaction run"
        successText="Out of band compaction run has been initiated"
        failText="Could not force compaction"
        intent={Intent.DANGER}
        onClose={() => {
          this.setState({ showForceCompact: false });
        }}
      >
        <p>Are you sure you want to force a compaction run?</p>
        <p>This functionality only exists for debugging and testing reasons.</p>
        <p>If you are running it in production you are doing something wrong.</p>
      </AsyncActionDialog>
    );
  }

  private readonly saveRules = async (datasource: string, rules: Rule[], comment: string) => {
    try {
      await Api.instance.post(`/druid/coordinator/v1/rules/${Api.encodePath(datasource)}`, rules, {
        headers: {
          'X-Druid-Author': 'console',
          'X-Druid-Comment': comment,
        },
      });
    } catch (e) {
      AppToaster.show({
        message: `Failed to submit retention rules: ${getDruidErrorMessage(e)}`,
        intent: Intent.DANGER,
      });
      return;
    }

    AppToaster.show({
      message: 'Retention rules submitted successfully',
      intent: Intent.SUCCESS,
    });
    this.fetchData();
  };

  private readonly editDefaultRules = () => {
    this.setState({ retentionDialogOpenOn: undefined });
    setTimeout(() => {
      this.setState(state => {
        const defaultRules = state.datasourcesAndDefaultRulesState.data?.defaultRules;
        if (!defaultRules) return {};

        return {
          retentionDialogOpenOn: {
            datasource: '_default',
            rules: defaultRules,
            defaultRules,
          },
        };
      });
    }, 50);
  };

  private readonly saveCompaction = async (compactionConfig: CompactionConfig) => {
    if (!compactionConfig) return;
    try {
      await Api.instance.post(
        `/druid/indexer/v1/compaction/config/datasources/${Api.encodePath(
          compactionConfig.dataSource,
        )}`,
        compactionConfig,
      );
      this.setState({ compactionDialogOpenOn: undefined });
      this.fetchData();
    } catch (e) {
      AppToaster.show({
        message: getDruidErrorMessage(e),
        intent: Intent.DANGER,
      });
    }
  };

  private readonly deleteCompaction = () => {
    const { compactionDialogOpenOn } = this.state;
    if (!compactionDialogOpenOn) return;
    const datasource = compactionDialogOpenOn.datasource;
    AppToaster.show({
      message: `Are you sure you want to delete ${datasource}'s compaction?`,
      intent: Intent.DANGER,
      action: {
        text: 'Confirm',
        // eslint-disable-next-line @typescript-eslint/no-misused-promises
        onClick: async () => {
          try {
            await Api.instance.delete(
              `/druid/indexer/v1/compaction/config/datasources/${Api.encodePath(datasource)}`,
            );
            this.setState({ compactionDialogOpenOn: undefined }, () => this.fetchData());
          } catch (e) {
            AppToaster.show({
              message: getDruidErrorMessage(e),
              intent: Intent.DANGER,
            });
          }
        },
      },
    });
  };

  private toggleUnused(showUnused: boolean) {
    this.setState({ showUnused: !showUnused }, () => {
      if (showUnused) return;
      this.fetchData();
    });
  }

  getDatasourceActions(
    datasource: string,
    unused: boolean | undefined,
    rules: Rule[] | undefined,
    compactionInfo: CompactionInfo | undefined,
  ): BasicAction[] {
    const { goToQuery, goToSegments, capabilities } = this.props;

    if (unused) {
      if (!capabilities.hasOverlordAccess()) return [];
      return [
        {
          icon: IconNames.EXPORT,
          title: 'Mark as used all segments',
          onAction: () =>
            this.setState({
              datasourceToMarkAllNonOvershadowedSegmentsAsUsedIn: datasource,
            }),
        },
        {
          icon: IconNames.EXPORT,
          title: 'Mark as used segments by interval',
          onAction: () =>
            this.setState({
              datasourceToMarkSegmentsByIntervalIn: datasource,
              useUnuseAction: 'use',
            }),
        },
        {
          icon: IconNames.TRASH,
          title: 'Delete segments (issue kill task)',
          intent: Intent.DANGER,
          onAction: () => this.setState({ killDatasource: datasource }),
        },
      ];
    } else {
      return compact([
        capabilities.hasSql()
          ? {
              icon: IconNames.APPLICATION,
              title: 'Query with SQL',
              onAction: () => goToQuery({ queryString: SqlQuery.create(T(datasource)).toString() }),
            }
          : undefined,
        {
          icon: IconNames.STACKED_CHART,
          title: 'Go to segments',
          onAction: () => {
            goToSegments({ datasource });
          },
        },
        capabilities.hasCoordinatorAccess()
          ? {
              icon: IconNames.AUTOMATIC_UPDATES,
              title: 'Edit retention rules',
              onAction: () => {
                const defaultRules = this.state.datasourcesAndDefaultRulesState.data?.defaultRules;
                if (!defaultRules) return;
                this.setState({
                  retentionDialogOpenOn: {
                    datasource,
                    rules: rules || [],
                    defaultRules,
                  },
                });
              },
            }
          : undefined,
        capabilities.hasOverlordAccess()
          ? {
              icon: IconNames.REFRESH,
              title: 'Mark as used all segments (will lead to reapplying retention rules)',
              onAction: () =>
                this.setState({
                  datasourceToMarkAllNonOvershadowedSegmentsAsUsedIn: datasource,
                }),
            }
          : undefined,
        capabilities.hasCoordinatorAccess() && compactionInfo
          ? {
              icon: IconNames.COMPRESSED,
              title: 'Edit compaction configuration',
              onAction: () => {
                this.setState({
                  compactionDialogOpenOn: {
                    datasource,
                    compactionConfig: compactionInfo.config,
                  },
                });
              },
            }
          : undefined,
        capabilities.hasOverlordAccess()
          ? {
              icon: IconNames.EXPORT,
              title: 'Mark as used segments by interval',
              onAction: () =>
                this.setState({
                  datasourceToMarkSegmentsByIntervalIn: datasource,
                  useUnuseAction: 'use',
                }),
            }
          : undefined,
        capabilities.hasOverlordAccess()
          ? {
              icon: IconNames.IMPORT,
              title: 'Mark as unused segments by interval',
              onAction: () =>
                this.setState({
                  datasourceToMarkSegmentsByIntervalIn: datasource,
                  useUnuseAction: 'unuse',
                }),
            }
          : undefined,
        capabilities.hasOverlordAccess()
          ? {
              icon: IconNames.IMPORT,
              title: 'Mark as unused all segments',
              intent: Intent.DANGER,
              onAction: () => this.setState({ datasourceToMarkAsUnusedAllSegmentsIn: datasource }),
            }
          : undefined,
        capabilities.hasOverlordAccess()
          ? {
              icon: IconNames.TRASH,
              title: 'Delete unused segments (issue kill task)',
              intent: Intent.DANGER,
              onAction: () => this.setState({ killDatasource: datasource }),
            }
          : undefined,
      ]);
    }
  }

  private renderRetentionDialog() {
    const { capabilities } = this.props;
    const { retentionDialogOpenOn } = this.state;
    if (!retentionDialogOpenOn) return;

    return (
      <RetentionDialog
        datasource={retentionDialogOpenOn.datasource}
        rules={retentionDialogOpenOn.rules}
        capabilities={capabilities}
        onEditDefaults={this.editDefaultRules}
        defaultRules={retentionDialogOpenOn.defaultRules}
        onCancel={() => this.setState({ retentionDialogOpenOn: undefined })}
        onSave={this.saveRules}
      />
    );
  }

  private renderCompactionConfigDialog() {
    const { datasourcesAndDefaultRulesState, compactionDialogOpenOn } = this.state;
    if (!compactionDialogOpenOn || !datasourcesAndDefaultRulesState.data) return;

    return (
      <CompactionConfigDialog
        datasource={compactionDialogOpenOn.datasource}
        compactionConfig={compactionDialogOpenOn.compactionConfig}
        onClose={() => this.setState({ compactionDialogOpenOn: undefined })}
        onSave={this.saveCompaction}
        onDelete={this.deleteCompaction}
      />
    );
  }

  private onDetail(datasource: Datasource): void {
    const { unused, rules, compaction } = datasource;

    this.setState({
      datasourceTableActionDialogId: datasource.datasource,
      actions: this.getDatasourceActions(datasource.datasource, unused, rules, compaction),
    });
  }

  private renderDatasourcesTable() {
    const { goToTasks, capabilities, filters, onFiltersChange } = this.props;
    const { datasourcesAndDefaultRulesState, showUnused, visibleColumns, showSegmentTimeline } =
      this.state;

    let { datasources, defaultRules } = datasourcesAndDefaultRulesState.data || { datasources: [] };

    if (!showUnused) {
      datasources = datasources.filter(d => !d.unused);
    }

    // Calculate column values for bracing

    const totalDataSizeValues = datasources.map(d => formatTotalDataSize(d.total_data_size));

    const minSegmentRowsValues = datasources.map(d => formatSegmentRows(d.min_segment_rows));
    const avgSegmentRowsValues = datasources.map(d => formatSegmentRows(d.avg_segment_rows));
    const maxSegmentRowsValues = datasources.map(d => formatSegmentRows(d.max_segment_rows));

    const minSegmentSizeValues = datasources.map(d => formatSegmentSize(d.min_segment_size));
    const avgSegmentSizeValues = datasources.map(d => formatSegmentSize(d.avg_segment_size));
    const maxSegmentSizeValues = datasources.map(d => formatSegmentSize(d.max_segment_size));

    const totalRowsValues = datasources.map(d => formatTotalRows(d.total_rows));

    const avgRowSizeValues = datasources.map(d => formatAvgRowSize(d.avg_row_size));

    const replicatedSizeValues = datasources.map(d => formatReplicatedSize(d.replicated_size));

    const leftToBeCompactedValues = datasources.map(d =>
      d.compaction?.status
        ? formatLeftToBeCompacted(d.compaction?.status.bytesAwaitingCompaction)
        : '-',
    );

    return (
      <ReactTable
        data={datasources}
        loading={datasourcesAndDefaultRulesState.loading}
        noDataText={
          datasourcesAndDefaultRulesState.getErrorMessage() ||
          (!datasourcesAndDefaultRulesState.loading && datasources && !datasources.length
            ? 'No datasources'
            : '')
        }
        filterable
        filtered={filters}
        onFilteredChange={onFiltersChange}
        defaultPageSize={STANDARD_TABLE_PAGE_SIZE}
        pageSizeOptions={STANDARD_TABLE_PAGE_SIZE_OPTIONS}
        showPagination={datasources.length > STANDARD_TABLE_PAGE_SIZE}
        columns={[
          {
            Header: twoLines('Datasource', 'name'),
            show: visibleColumns.shown('Datasource name'),
            accessor: 'datasource',
            width: 150,
            Cell: ({ value, original }) => (
              <TableClickableCell
                onClick={() => this.onDetail(original)}
                hoverIcon={IconNames.SEARCH_TEMPLATE}
                tooltip="Show detail"
              >
                {showSegmentTimeline ? (
                  <>
                    <span style={{ color: getDatasourceColor(value) }}>&#9632;</span> {value}
                  </>
                ) : (
                  value
                )}
              </TableClickableCell>
            ),
          },
          {
            Header: 'Availability',
            show: visibleColumns.shown('Availability'),
            filterable: false,
            width: 220,
            accessor: 'num_segments',
            className: 'padded',
            Cell: ({ value: num_segments, original }) => {
              const { datasource, unused, num_segments_to_load, num_zero_replica_segments, rules } =
                original as Datasource;
              if (unused) {
                return (
                  <span>
                    <span style={{ color: DatasourcesView.UNUSED_COLOR }}>&#x25cf;&nbsp;</span>
                    Unused
                  </span>
                );
              }

              const hasZeroReplicationRule = RuleUtil.hasZeroReplicaRule(rules, defaultRules);
              const descriptor = hasZeroReplicationRule ? 'pre-cached' : 'available';
              const segmentsEl = (
                <a
                  onClick={() =>
                    this.setState({ showSegmentTimeline: { capabilities, datasource } })
                  }
                  data-tooltip="Show in segment timeline"
                >
                  {pluralIfNeeded(num_segments, 'segment')}
                </a>
              );
              const percentZeroReplica = (
                Math.floor((num_zero_replica_segments / num_segments) * 1000) / 10
              ).toFixed(1);

              if (typeof num_segments_to_load !== 'number' || typeof num_segments !== 'number') {
                return '-';
              } else if (num_segments === 0) {
                return (
                  <span>
                    <span style={{ color: DatasourcesView.EMPTY_COLOR }}>&#x25cf;&nbsp;</span>
                    Empty
                  </span>
                );
              } else if (num_segments_to_load === 0) {
                return (
                  <span>
                    <span style={{ color: DatasourcesView.FULLY_AVAILABLE_COLOR }}>
                      &#x25cf;&nbsp;
                    </span>
                    {assemble(
                      num_segments !== num_zero_replica_segments
                        ? `Fully ${descriptor}`
                        : undefined,
                      hasZeroReplicationRule ? `${percentZeroReplica}% deep storage only` : '',
                    ).join(', ')}{' '}
                    ({segmentsEl})
                  </span>
                );
              } else {
                const numAvailableSegments = num_segments - num_segments_to_load;
                const percentAvailable = (
                  Math.floor((numAvailableSegments / num_segments) * 1000) / 10
                ).toFixed(1);
                return (
                  <span>
                    <span style={{ color: DatasourcesView.PARTIALLY_AVAILABLE_COLOR }}>
                      {numAvailableSegments ? '\u25cf' : '\u25cb'}&nbsp;
                    </span>
                    {`${percentAvailable}% ${descriptor}${
                      hasZeroReplicationRule ? `, ${percentZeroReplica}% deep storage only` : ''
                    }`}{' '}
                    ({segmentsEl})
                  </span>
                );
              }
            },
            sortMethod: (d1, d2) => {
              const percentAvailable1 = d1.num_available / d1.num_total;
              const percentAvailable2 = d2.num_available / d2.num_total;
              return percentAvailable1 - percentAvailable2 || d1.num_total - d2.num_total;
            },
          },
          {
            Header: twoLines('Historical', 'load/drop queues'),
            show: visibleColumns.shown('Historical load/drop queues'),
            accessor: 'num_segments_to_load',
            filterable: false,
            width: 180,
            className: 'padded',
            Cell: ({ original }) => {
              const { num_segments_to_load, num_segments_to_drop } = original as Datasource;
              return formatLoadDrop(num_segments_to_load, num_segments_to_drop);
            },
          },
          {
            Header: twoLines('Total', 'data size'),
            show: visibleColumns.shown('Total data size'),
            accessor: 'total_data_size',
            filterable: false,
            width: 100,
            className: 'padded',
            Cell: ({ value }) => (
              <BracedText text={formatTotalDataSize(value)} braces={totalDataSizeValues} />
            ),
          },
          {
            Header: 'Running tasks',
            show: visibleColumns.shown('Running tasks'),
            id: 'running_tasks',
            accessor: d => countRunningTasks(d.runningTasks),
            filterable: false,
            width: 200,
            Cell: ({ original }) => {
              const { runningTasks } = original;
              if (!runningTasks) return;
              return (
                <TableClickableCell
                  onClick={() => goToTasks(original.datasource)}
                  hoverIcon={IconNames.ARROW_TOP_RIGHT}
                  tooltip="Go to tasks"
                >
                  {formatRunningTasks(runningTasks)}
                </TableClickableCell>
              );
            },
          },
          {
            Header: twoLines('Segment rows', 'minimum / average / maximum'),
            show: capabilities.hasSql() && visibleColumns.shown('Segment rows'),
            accessor: 'avg_segment_rows',
            filterable: false,
            width: 230,
            className: 'padded',
            Cell: ({ value, original }) => {
              const { min_segment_rows, max_segment_rows } = original as Datasource;
              if (
                isNumberLikeNaN(value) ||
                isNumberLikeNaN(min_segment_rows) ||
                isNumberLikeNaN(max_segment_rows)
              )
                return '-';
              return (
                <>
                  <BracedText
                    text={formatSegmentRows(min_segment_rows)}
                    braces={minSegmentRowsValues}
                  />{' '}
                  &nbsp;{' '}
                  <BracedText text={formatSegmentRows(value)} braces={avgSegmentRowsValues} />{' '}
                  &nbsp;{' '}
                  <BracedText
                    text={formatSegmentRows(max_segment_rows)}
                    braces={maxSegmentRowsValues}
                  />
                </>
              );
            },
          },
          {
            Header: twoLines('Segment size', 'minimum / average / maximum'),
            show: capabilities.hasSql() && visibleColumns.shown('Segment size'),
            accessor: 'avg_segment_size',
            filterable: false,
            width: 270,
            className: 'padded',
            Cell: ({ value, original }) => {
              const { min_segment_size, max_segment_size } = original as Datasource;
              if (
                isNumberLikeNaN(value) ||
                isNumberLikeNaN(min_segment_size) ||
                isNumberLikeNaN(max_segment_size)
              )
                return '-';
              return (
                <>
                  <BracedText
                    text={formatSegmentSize(min_segment_size)}
                    braces={minSegmentSizeValues}
                  />{' '}
                  &nbsp;{' '}
                  <BracedText text={formatSegmentSize(value)} braces={avgSegmentSizeValues} />{' '}
                  &nbsp;{' '}
                  <BracedText
                    text={formatSegmentSize(max_segment_size)}
                    braces={maxSegmentSizeValues}
                  />
                </>
              );
            },
          },
          {
            Header: twoLines('Segment', 'granularity'),
            show: capabilities.hasSql() && visibleColumns.shown('Segment granularity'),
            id: 'segment_granularity',
            accessor: segmentGranularityCountsToRank,
            filterable: false,
            width: 100,
            className: 'padded',
            Cell: ({ original }) => {
              const {
                num_segments,
                minute_aligned_segments,
                hour_aligned_segments,
                day_aligned_segments,
                month_aligned_segments,
                year_aligned_segments,
                all_granularity_segments,
              } = original as Datasource;
              const segmentGranularities: string[] = [];
              if (!num_segments || isNumberLikeNaN(year_aligned_segments)) return '-';
              if (all_granularity_segments) {
                segmentGranularities.push('All');
              }
              if (year_aligned_segments) {
                segmentGranularities.push('Year');
              }
              if (month_aligned_segments !== year_aligned_segments) {
                segmentGranularities.push('Month');
              }
              if (day_aligned_segments !== month_aligned_segments) {
                segmentGranularities.push('Day');
              }
              if (hour_aligned_segments !== day_aligned_segments) {
                segmentGranularities.push('Hour');
              }
              if (minute_aligned_segments !== hour_aligned_segments) {
                segmentGranularities.push('Minute');
              }
              if (
                Number(num_segments) - Number(all_granularity_segments) !==
                Number(minute_aligned_segments)
              ) {
                segmentGranularities.push('Sub minute');
              }
              return segmentGranularities.join(', ');
            },
          },
          {
            Header: twoLines('Total', 'rows'),
            show: capabilities.hasSql() && visibleColumns.shown('Total rows'),
            accessor: 'total_rows',
            filterable: false,
            width: 110,
            className: 'padded',
            Cell: ({ value }) => {
              if (isNumberLikeNaN(value)) return '-';
              return (
                <BracedText
                  text={formatTotalRows(value)}
                  braces={totalRowsValues}
                  unselectableThousandsSeparator
                />
              );
            },
          },
          {
            Header: twoLines('Avg. row size', '(bytes)'),
            show: capabilities.hasSql() && visibleColumns.shown('Avg. row size'),
            accessor: 'avg_row_size',
            filterable: false,
            width: 100,
            className: 'padded',
            Cell: ({ value }) => {
              if (isNumberLikeNaN(value)) return '-';
              return (
                <BracedText
                  text={formatAvgRowSize(value)}
                  braces={avgRowSizeValues}
                  unselectableThousandsSeparator
                />
              );
            },
          },
          {
            Header: twoLines('Replicated', 'size'),
            show: capabilities.hasSql() && visibleColumns.shown('Replicated size'),
            accessor: 'replicated_size',
            filterable: false,
            width: 100,
            className: 'padded',
            Cell: ({ value }) => {
              if (isNumberLikeNaN(value)) return '-';
              return (
                <BracedText text={formatReplicatedSize(value)} braces={replicatedSizeValues} />
              );
            },
          },
          {
            Header: 'Compaction',
            show: capabilities.hasCoordinatorAccess() && visibleColumns.shown('Compaction'),
            id: 'compactionStatus',
            accessor: row => Boolean(row.compaction?.status),
            filterable: false,
            width: 180,
            Cell: ({ original }) => {
              const { datasource, compaction } = original as Datasource;
              if (!compaction) return;
              return (
                <TableClickableCell
                  tooltip="Open compaction configuration"
                  disabled={!compaction}
                  onClick={() => {
                    if (!compaction) return;
                    this.setState({
                      compactionDialogOpenOn: {
                        datasource,
                        compactionConfig: compaction.config,
                      },
                    });
                  }}
                  hoverIcon={IconNames.EDIT}
                >
                  {formatCompactionInfo(compaction)}
                </TableClickableCell>
              );
            },
          },
          {
            Header: twoLines('% Compacted', 'bytes / segments / intervals'),
            show: capabilities.hasCoordinatorAccess() && visibleColumns.shown('% Compacted'),
            id: 'percentCompacted',
            width: 200,
            accessor: ({ compaction }) => {
              const status = compaction?.status;
              return status?.bytesCompacted
                ? status.bytesCompacted / (status.bytesAwaitingCompaction + status.bytesCompacted)
                : 0;
            },
            filterable: false,
            className: 'padded',
            Cell: ({ original }) => {
              const { compaction } = original as Datasource;
              if (!compaction) return;

              const { status } = compaction;
              if (!status || zeroCompactionStatus(status)) {
                return (
                  <>
                    <BracedText text="-" braces={PERCENT_BRACES} /> &nbsp;{' '}
                    <BracedText text="-" braces={PERCENT_BRACES} /> &nbsp;{' '}
                    <BracedText text="-" braces={PERCENT_BRACES} />
                  </>
                );
              }

              return (
                <>
                  <BracedText
                    text={formatPercent(
                      progress(status.bytesCompacted, status.bytesAwaitingCompaction),
                    )}
                    braces={PERCENT_BRACES}
                  />{' '}
                  &nbsp;{' '}
                  <BracedText
                    text={formatPercent(
                      progress(status.segmentCountCompacted, status.segmentCountAwaitingCompaction),
                    )}
                    braces={PERCENT_BRACES}
                  />{' '}
                  &nbsp;{' '}
                  <BracedText
                    text={formatPercent(
                      progress(
                        status.intervalCountCompacted,
                        status.intervalCountAwaitingCompaction,
                      ),
                    )}
                    braces={PERCENT_BRACES}
                  />
                </>
              );
            },
          },
          {
            Header: twoLines('Left to be', 'compacted'),
            show:
              capabilities.hasCoordinatorAccess() && visibleColumns.shown('Left to be compacted'),
            id: 'leftToBeCompacted',
            width: 100,
            accessor: ({ compaction }) => {
              const status = compaction?.status;
              return status?.bytesAwaitingCompaction || 0;
            },
            filterable: false,
            className: 'padded',
            Cell: ({ original }) => {
              const { compaction } = original as Datasource;
              if (!compaction) return;

              const { status } = compaction;
              if (!status) {
                return <BracedText text="-" braces={leftToBeCompactedValues} />;
              }

              return (
                <BracedText
                  text={formatLeftToBeCompacted(status.bytesAwaitingCompaction)}
                  braces={leftToBeCompactedValues}
                />
              );
            },
          },
          {
            Header: 'Retention',
            show: capabilities.hasCoordinatorAccess() && visibleColumns.shown('Retention'),
            id: 'retention',
            accessor: row => row.rules?.length || 0,
            filterable: false,
            width: 200,
            Cell: ({ original }) => {
              const { datasource, rules } = original as Datasource;
              if (!rules) return;

              return (
                <TableClickableCell
                  tooltip="Open retention (load rule) configuration"
                  disabled={!defaultRules}
                  onClick={() => {
                    if (!defaultRules) return;
                    this.setState({
                      retentionDialogOpenOn: {
                        datasource,
                        rules,
                        defaultRules,
                      },
                    });
                  }}
                  hoverIcon={IconNames.EDIT}
                >
                  {rules.length
                    ? DatasourcesView.formatRules(rules)
                    : defaultRules
                    ? `Cluster default: ${DatasourcesView.formatRules(defaultRules)}`
                    : ''}
                </TableClickableCell>
              );
            },
          },
          {
            Header: ACTION_COLUMN_LABEL,
            accessor: 'datasource',
            id: ACTION_COLUMN_ID,
            width: ACTION_COLUMN_WIDTH,
            filterable: false,
            sortable: false,
            Cell: ({ value: datasource, original }) => {
              const { unused, rules, compaction } = original as Datasource;
              const datasourceActions = this.getDatasourceActions(
                datasource,
                unused,
                rules,
                compaction,
              );
              return (
                <ActionCell
                  onDetail={() => {
                    this.onDetail(original);
                  }}
                  disableDetail={unused}
                  actions={datasourceActions}
                  menuTitle={datasource}
                />
              );
            },
          },
        ]}
      />
    );
  }

  render() {
    const { capabilities, filters, goToSegments } = this.props;
    const {
      showUnused,
      visibleColumns,
      showSegmentTimeline,
      datasourceTableActionDialogId,
      actions,
    } = this.state;

    return (
      <div className="datasources-view app-view">
        <ViewControlBar label="Datasources">
          <RefreshButton
            onRefresh={this.refresh}
            localStorageKey={LocalStorageKeys.DATASOURCES_REFRESH_RATE}
          />
          {this.renderBulkDatasourceActions()}
          <Switch
            checked={showUnused}
            label="Show unused"
            onChange={() => this.toggleUnused(showUnused)}
            disabled={!capabilities.hasCoordinatorAccess()}
          />
          <Switch
            checked={Boolean(showSegmentTimeline)}
            label="Show segment timeline"
            onChange={() =>
              this.setState({
                showSegmentTimeline: showSegmentTimeline
                  ? undefined
                  : {
                      capabilities,
                      datasource: findMap(filters, filter =>
                        filter.id === 'datasource' && /^=[^=|]+$/.exec(String(filter.value))
                          ? filter.value.slice(1)
                          : undefined,
                      ),
                    },
              })
            }
            disabled={!capabilities.hasSqlOrCoordinatorAccess()}
          />
          <TableColumnSelector
            columns={TABLE_COLUMNS_BY_MODE[capabilities.getMode()]}
            onChange={column =>
              this.setState(prevState => ({
                visibleColumns: prevState.visibleColumns.toggle(column),
              }))
            }
            onClose={this.fetchData}
            tableColumnsHidden={visibleColumns.getHiddenColumns()}
          />
        </ViewControlBar>
        <SplitterLayout
          className="timeline-datasources-splitter"
          vertical
          percentage
          secondaryInitialSize={35}
          primaryIndex={1}
          primaryMinSize={20}
          secondaryMinSize={10}
        >
          {showSegmentTimeline && (
            <SegmentTimeline
              capabilities={showSegmentTimeline.capabilities}
              datasource={showSegmentTimeline.datasource}
              getIntervalActionButton={(start, end, datasource, realtime) => {
                return (
                  <Button
                    text="Open in segments view"
                    small
                    rightIcon={IconNames.ARROW_TOP_RIGHT}
                    onClick={() => goToSegments({ start, end, datasource, realtime })}
                  />
                );
              }}
            />
          )}
          {this.renderDatasourcesTable()}
        </SplitterLayout>
        {datasourceTableActionDialogId && (
          <DatasourceTableActionDialog
            datasource={datasourceTableActionDialogId}
            actions={actions}
            onClose={() => this.setState({ datasourceTableActionDialogId: undefined })}
          />
        )}
        {this.renderUnuseAction()}
        {this.renderUseAction()}
        {this.renderUseUnuseActionByInterval()}
        {this.renderKillAction()}
        {this.renderRetentionDialog()}
        {this.renderCompactionConfigDialog()}
        {this.renderForceCompactAction()}
      </div>
    );
  }
}
