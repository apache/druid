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

import { FormGroup, InputGroup, Intent, MenuItem, Switch } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import axios from 'axios';
import classNames from 'classnames';
import { SqlQuery, SqlRef } from 'druid-query-toolkit';
import React from 'react';
import ReactTable, { Filter } from 'react-table';

import {
  ACTION_COLUMN_ID,
  ACTION_COLUMN_LABEL,
  ACTION_COLUMN_WIDTH,
  ActionCell,
  ActionIcon,
  BracedText,
  MoreButton,
  RefreshButton,
  SegmentTimeline,
  TableColumnSelector,
  ViewControlBar,
} from '../../components';
import { AsyncActionDialog, CompactionDialog, RetentionDialog } from '../../dialogs';
import { DatasourceTableActionDialog } from '../../dialogs/datasource-table-action-dialog/datasource-table-action-dialog';
import { AppToaster } from '../../singletons/toaster';
import {
  addFilter,
  CompactionConfig,
  CompactionStatus,
  countBy,
  formatBytes,
  formatCompactionConfigAndStatus,
  formatInteger,
  formatMillions,
  formatPercent,
  getDruidErrorMessage,
  LocalStorageKeys,
  lookupBy,
  pluralIfNeeded,
  queryDruidSql,
  QueryManager,
  QueryState,
  zeroCompactionStatus,
} from '../../utils';
import { BasicAction } from '../../utils/basic-action';
import { Capabilities, CapabilitiesMode } from '../../utils/capabilities';
import { Rule, RuleUtil } from '../../utils/load-rule';
import { LocalStorageBackedArray } from '../../utils/local-storage-backed-array';
import { deepGet } from '../../utils/object-change';

import './datasource-view.scss';

const tableColumns: Record<CapabilitiesMode, string[]> = {
  full: [
    'Datasource name',
    'Availability',
    'Segment load/drop queues',
    'Total data size',
    'Segment size',
    'Total rows',
    'Avg. row size',
    'Replicated size',
    'Compaction',
    '% Compacted',
    'Left to be compacted',
    'Retention',
    ACTION_COLUMN_LABEL,
  ],
  'no-sql': [
    'Datasource name',
    'Availability',
    'Segment load/drop queues',
    'Total data size',
    'Compaction',
    '% Compacted',
    'Left to be compacted',
    'Retention',
    ACTION_COLUMN_LABEL,
  ],
  'no-proxy': [
    'Datasource name',
    'Availability',
    'Segment load/drop queues',
    'Total data size',
    'Segment size',
    'Total rows',
    'Avg. row size',
    'Replicated size',
    ACTION_COLUMN_LABEL,
  ],
};

function formatLoadDrop(segmentsToLoad: number, segmentsToDrop: number): string {
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
const formatTotalRows = formatInteger;
const formatAvgRowSize = formatInteger;
const formatReplicatedSize = formatBytes;
const formatLeftToBeCompacted = formatBytes;

function twoLines(line1: string, line2: string) {
  return (
    <>
      {line1}
      <br />
      {line2}
    </>
  );
}

function progress(done: number, awaiting: number): number {
  const d = done + awaiting;
  if (!d) return 0;
  return done / d;
}

const PERCENT_BRACES = [formatPercent(1)];

interface DatasourceQueryResultRow {
  readonly datasource: string;
  readonly num_segments: number;
  readonly num_available_segments: number;
  readonly num_segments_to_load: number;
  readonly num_segments_to_drop: number;
  readonly total_data_size: number;
  readonly replicated_size: number;
  readonly min_segment_rows: number;
  readonly avg_segment_rows: number;
  readonly max_segment_rows: number;
  readonly total_rows: number;
  readonly avg_row_size: number;
}

interface Datasource extends DatasourceQueryResultRow {
  readonly rules: Rule[];
  readonly compactionConfig?: CompactionConfig;
  readonly compactionStatus?: CompactionStatus;
  readonly unused?: boolean;
}

interface DatasourcesAndDefaultRules {
  readonly datasources: Datasource[];
  readonly defaultRules: Rule[];
}

interface RetentionDialogOpenOn {
  readonly datasource: string;
  readonly rules: Rule[];
}

interface CompactionDialogOpenOn {
  readonly datasource: string;
  readonly compactionConfig: CompactionConfig;
}

export interface DatasourcesViewProps {
  goToQuery: (initSql: string) => void;
  goToTask: (datasource?: string, openDialog?: string) => void;
  goToSegments: (datasource: string, onlyUnavailable?: boolean) => void;
  capabilities: Capabilities;
  initDatasource?: string;
}

export interface DatasourcesViewState {
  datasourceFilter: Filter[];
  datasourcesAndDefaultRulesState: QueryState<DatasourcesAndDefaultRules>;

  tiersState: QueryState<string[]>;

  showUnused: boolean;
  retentionDialogOpenOn?: RetentionDialogOpenOn;
  compactionDialogOpenOn?: CompactionDialogOpenOn;
  datasourceToMarkAsUnusedAllSegmentsIn?: string;
  datasourceToMarkAllNonOvershadowedSegmentsAsUsedIn?: string;
  killDatasource?: string;
  datasourceToMarkSegmentsByIntervalIn?: string;
  useUnuseAction: 'use' | 'unuse';
  useUnuseInterval: string;
  showForceCompact: boolean;
  hiddenColumns: LocalStorageBackedArray<string>;
  showChart: boolean;
  chartWidth: number;
  chartHeight: number;

  datasourceTableActionDialogId?: string;
  actions: BasicAction[];
}

export class DatasourcesView extends React.PureComponent<
  DatasourcesViewProps,
  DatasourcesViewState
> {
  static UNUSED_COLOR = '#0a1500';
  static FULLY_AVAILABLE_COLOR = '#57d500';
  static PARTIALLY_AVAILABLE_COLOR = '#ffbf00';

  static DATASOURCE_SQL = `SELECT
  datasource,
  COUNT(*) FILTER (WHERE (is_published = 1 AND is_overshadowed = 0) OR is_realtime = 1) AS num_segments,
  COUNT(*) FILTER (WHERE is_available = 1 AND ((is_published = 1 AND is_overshadowed = 0) OR is_realtime = 1)) AS num_available_segments,
  COUNT(*) FILTER (WHERE is_published = 1 AND is_overshadowed = 0 AND is_available = 0) AS num_segments_to_load,
  COUNT(*) FILTER (WHERE is_available = 1 AND NOT ((is_published = 1 AND is_overshadowed = 0) OR is_realtime = 1)) AS num_segments_to_drop,
  SUM("size") FILTER (WHERE is_published = 1 AND is_overshadowed = 0) AS total_data_size,
  SUM("size" * "num_replicas") FILTER (WHERE is_published = 1 AND is_overshadowed = 0) AS replicated_size,
  MIN("num_rows") FILTER (WHERE is_published = 1 AND is_overshadowed = 0) AS min_segment_rows,
  AVG("num_rows") FILTER (WHERE is_published = 1 AND is_overshadowed = 0) AS avg_segment_rows,
  MAX("num_rows") FILTER (WHERE is_published = 1 AND is_overshadowed = 0) AS max_segment_rows,
  SUM("num_rows") FILTER (WHERE (is_published = 1 AND is_overshadowed = 0) OR is_realtime = 1) AS total_rows,
  CASE
    WHEN SUM("num_rows") FILTER (WHERE is_published = 1 AND is_overshadowed = 0) <> 0
    THEN (
      SUM("size") FILTER (WHERE is_published = 1 AND is_overshadowed = 0) /
      SUM("num_rows") FILTER (WHERE is_published = 1 AND is_overshadowed = 0)
    )
    ELSE 0
  END AS avg_row_size
FROM sys.segments
GROUP BY 1`;

  static formatRules(rules: Rule[]): string {
    if (rules.length === 0) {
      return 'No rules';
    } else if (rules.length <= 2) {
      return rules.map(RuleUtil.ruleToString).join(', ');
    } else {
      return `${RuleUtil.ruleToString(rules[0])} +${rules.length - 1} more rules`;
    }
  }

  private datasourceQueryManager: QueryManager<Capabilities, DatasourcesAndDefaultRules>;
  private tiersQueryManager: QueryManager<Capabilities, string[]>;

  constructor(props: DatasourcesViewProps, context: any) {
    super(props, context);

    const datasourceFilter: Filter[] = [];
    if (props.initDatasource) {
      datasourceFilter.push({ id: 'datasource', value: `"${props.initDatasource}"` });
    }

    this.state = {
      datasourceFilter,
      datasourcesAndDefaultRulesState: QueryState.INIT,

      tiersState: QueryState.INIT,

      showUnused: false,
      useUnuseAction: 'unuse',
      useUnuseInterval: '',
      showForceCompact: false,
      hiddenColumns: new LocalStorageBackedArray<string>(
        LocalStorageKeys.DATASOURCE_TABLE_COLUMN_SELECTION,
      ),
      showChart: false,
      chartWidth: window.innerWidth * 0.85,
      chartHeight: window.innerHeight * 0.4,

      actions: [],
    };

    this.datasourceQueryManager = new QueryManager({
      processQuery: async capabilities => {
        let datasources: DatasourceQueryResultRow[];
        if (capabilities.hasSql()) {
          datasources = await queryDruidSql({ query: DatasourcesView.DATASOURCE_SQL });
        } else if (capabilities.hasCoordinatorAccess()) {
          const datasourcesResp = await axios.get('/druid/coordinator/v1/datasources?simple');
          const loadstatusResp = await axios.get('/druid/coordinator/v1/loadstatus?simple');
          const loadstatus = loadstatusResp.data;
          datasources = datasourcesResp.data.map(
            (d: any): DatasourceQueryResultRow => {
              const totalDataSize = deepGet(d, 'properties.segments.size') || -1;
              const segmentsToLoad = Number(loadstatus[d.name] || 0);
              const availableSegments = Number(deepGet(d, 'properties.segments.count'));
              const numSegments = availableSegments + segmentsToLoad;
              return {
                datasource: d.name,
                num_available_segments: availableSegments,
                num_segments: numSegments,
                num_segments_to_load: segmentsToLoad,
                num_segments_to_drop: 0,
                replicated_size: -1,
                total_data_size: totalDataSize,
                min_segment_rows: -1,
                avg_segment_rows: -1,
                max_segment_rows: -1,
                total_rows: -1,
                avg_row_size: -1,
              };
            },
          );
        } else {
          throw new Error(`must have SQL or coordinator access`);
        }

        if (!capabilities.hasCoordinatorAccess()) {
          datasources.forEach((ds: any) => {
            ds.rules = [];
          });
          return {
            datasources,
            defaultRules: [],
          };
        }

        const seen = countBy(datasources, x => x.datasource);

        let unused: string[] = [];
        if (this.state.showUnused) {
          // Using 'includeDisabled' parameter for compatibility.
          // Should be changed to 'includeUnused' in Druid 0.17
          const unusedResp = await axios.get(
            '/druid/coordinator/v1/metadata/datasources?includeDisabled',
          );
          unused = unusedResp.data.filter((d: string) => !seen[d]);
        }

        const rulesResp = await axios.get('/druid/coordinator/v1/rules');
        const rules = rulesResp.data;

        const compactionConfigsResp = await axios.get('/druid/coordinator/v1/config/compaction');
        const compactionConfigs = lookupBy(
          compactionConfigsResp.data.compactionConfigs || [],
          (c: CompactionConfig) => c.dataSource,
        );

        const compactionStatusesResp = await axios.get('/druid/coordinator/v1/compaction/status');
        const compactionStatuses = lookupBy(
          compactionStatusesResp.data.latestStatus || [],
          (c: CompactionStatus) => c.dataSource,
        );

        const allDatasources = (datasources as any).concat(
          unused.map(d => ({ datasource: d, unused: true })),
        );
        allDatasources.forEach((ds: any) => {
          ds.rules = rules[ds.datasource] || [];
          ds.compactionConfig = compactionConfigs[ds.datasource];
          ds.compactionStatus = compactionStatuses[ds.datasource];
        });

        return {
          datasources: allDatasources,
          defaultRules: rules['_default'],
        };
      },
      onStateChange: datasourcesAndDefaultRulesState => {
        this.setState({
          datasourcesAndDefaultRulesState,
        });
      },
    });

    this.tiersQueryManager = new QueryManager({
      processQuery: async capabilities => {
        if (capabilities.hasCoordinatorAccess()) {
          const tiersResp = await axios.get('/druid/coordinator/v1/tiers');
          return tiersResp.data;
        } else {
          throw new Error(`must have coordinator access`);
        }
      },
      onStateChange: tiersState => {
        this.setState({ tiersState });
      },
    });
  }

  private handleResize = () => {
    this.setState({
      chartWidth: window.innerWidth * 0.85,
      chartHeight: window.innerHeight * 0.4,
    });
  };

  private refresh = (auto: any): void => {
    this.datasourceQueryManager.rerunLastQuery(auto);
    this.tiersQueryManager.rerunLastQuery(auto);
  };

  componentDidMount(): void {
    const { capabilities } = this.props;
    this.datasourceQueryManager.runQuery(capabilities);
    this.tiersQueryManager.runQuery(capabilities);
    window.addEventListener('resize', this.handleResize);
  }

  componentWillUnmount(): void {
    this.datasourceQueryManager.terminate();
    this.tiersQueryManager.terminate();
  }

  renderUnuseAction() {
    const { datasourceToMarkAsUnusedAllSegmentsIn } = this.state;
    if (!datasourceToMarkAsUnusedAllSegmentsIn) return;

    return (
      <AsyncActionDialog
        action={async () => {
          const resp = await axios.delete(
            `/druid/coordinator/v1/datasources/${datasourceToMarkAsUnusedAllSegmentsIn}`,
            {},
          );
          return resp.data;
        }}
        confirmButtonText="Mark as unused all segments"
        successText="All segments in datasource have been marked as unused"
        failText="Failed to mark as unused all segments in datasource"
        intent={Intent.DANGER}
        onClose={() => {
          this.setState({ datasourceToMarkAsUnusedAllSegmentsIn: undefined });
        }}
        onSuccess={() => {
          this.datasourceQueryManager.rerunLastQuery();
        }}
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
          const resp = await axios.post(
            `/druid/coordinator/v1/datasources/${datasourceToMarkAllNonOvershadowedSegmentsAsUsedIn}`,
            {},
          );
          return resp.data;
        }}
        confirmButtonText="Mark as used all segments"
        successText="All non-overshadowed segments in datasource have been marked as used"
        failText="Failed to mark as used all non-overshadowed segments in datasource"
        intent={Intent.PRIMARY}
        onClose={() => {
          this.setState({ datasourceToMarkAllNonOvershadowedSegmentsAsUsedIn: undefined });
        }}
        onSuccess={() => {
          this.datasourceQueryManager.rerunLastQuery();
        }}
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
          const resp = await axios.post(
            `/druid/coordinator/v1/datasources/${datasourceToMarkSegmentsByIntervalIn}/${param}`,
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
        onSuccess={() => {
          this.datasourceQueryManager.rerunLastQuery();
        }}
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
      <AsyncActionDialog
        action={async () => {
          const resp = await axios.delete(
            `/druid/coordinator/v1/datasources/${killDatasource}?kill=true&interval=1000/3000`,
            {},
          );
          return resp.data;
        }}
        confirmButtonText="Permanently delete unused segments"
        successText="Kill task was issued. Unused segments in datasource will be deleted"
        failText="Failed submit kill task"
        intent={Intent.DANGER}
        onClose={() => {
          this.setState({ killDatasource: undefined });
        }}
        onSuccess={() => {
          this.datasourceQueryManager.rerunLastQuery();
        }}
      >
        <p>
          {`Are you sure you want to permanently delete unused segments in '${killDatasource}'?`}
        </p>
        <p>This action is not reversible and the data deleted will be lost.</p>
      </AsyncActionDialog>
    );
  }

  renderBulkDatasourceActions() {
    const { goToQuery, capabilities } = this.props;

    return (
      <MoreButton
        altExtra={
          <MenuItem
            icon={IconNames.COMPRESSED}
            text="Force compaction run (debug)"
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
            onClick={() => goToQuery(DatasourcesView.DATASOURCE_SQL)}
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
          const resp = await axios.post(`/druid/coordinator/v1/compaction/compact`, {});
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

  private saveRules = async (datasource: string, rules: Rule[], comment: string) => {
    try {
      await axios.post(`/druid/coordinator/v1/rules/${datasource}`, rules, {
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
    this.datasourceQueryManager.rerunLastQuery();
  };

  private editDefaultRules = () => {
    this.setState({ retentionDialogOpenOn: undefined });
    setTimeout(() => {
      this.setState(state => {
        const datasourcesAndDefaultRules = state.datasourcesAndDefaultRulesState.data;
        if (!datasourcesAndDefaultRules) return {};

        return {
          retentionDialogOpenOn: {
            datasource: '_default',
            rules: datasourcesAndDefaultRules.defaultRules,
          },
        };
      });
    }, 50);
  };

  private saveCompaction = async (compactionConfig: any) => {
    if (!compactionConfig) return;
    try {
      await axios.post(`/druid/coordinator/v1/config/compaction`, compactionConfig);
      this.setState({ compactionDialogOpenOn: undefined });
      this.datasourceQueryManager.rerunLastQuery();
    } catch (e) {
      AppToaster.show({
        message: getDruidErrorMessage(e),
        intent: Intent.DANGER,
      });
    }
  };

  private deleteCompaction = async () => {
    const { compactionDialogOpenOn } = this.state;
    if (!compactionDialogOpenOn) return;
    const datasource = compactionDialogOpenOn.datasource;
    AppToaster.show({
      message: `Are you sure you want to delete ${datasource}'s compaction?`,
      intent: Intent.DANGER,
      action: {
        text: 'Confirm',
        onClick: async () => {
          try {
            await axios.delete(`/druid/coordinator/v1/config/compaction/${datasource}`);
            this.setState({ compactionDialogOpenOn: undefined }, () =>
              this.datasourceQueryManager.rerunLastQuery(),
            );
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
    if (!showUnused) {
      this.datasourceQueryManager.rerunLastQuery();
    }
    this.setState({ showUnused: !showUnused });
  }

  getDatasourceActions(
    datasource: string,
    unused: boolean,
    rules: Rule[],
    compactionConfig: CompactionConfig,
  ): BasicAction[] {
    const { goToQuery, goToTask, capabilities } = this.props;

    const goToActions: BasicAction[] = [
      {
        icon: IconNames.APPLICATION,
        title: 'Query with SQL',
        onAction: () => goToQuery(SqlQuery.create(SqlRef.table(datasource)).toString()),
      },
      {
        icon: IconNames.GANTT_CHART,
        title: 'Go to tasks',
        onAction: () => goToTask(datasource),
      },
    ];

    if (!capabilities.hasCoordinatorAccess()) {
      return goToActions;
    }

    if (unused) {
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
          icon: IconNames.TRASH,
          title: 'Delete segments (issue kill task)',
          intent: Intent.DANGER,
          onAction: () => this.setState({ killDatasource: datasource }),
        },
      ];
    } else {
      return goToActions.concat([
        {
          icon: IconNames.AUTOMATIC_UPDATES,
          title: 'Edit retention rules',
          onAction: () => {
            this.setState({
              retentionDialogOpenOn: {
                datasource,
                rules,
              },
            });
          },
        },
        {
          icon: IconNames.REFRESH,
          title: 'Mark as used all segments (will lead to reapplying retention rules)',
          onAction: () =>
            this.setState({
              datasourceToMarkAllNonOvershadowedSegmentsAsUsedIn: datasource,
            }),
        },
        {
          icon: IconNames.COMPRESSED,
          title: 'Edit compaction configuration',
          onAction: () => {
            this.setState({
              compactionDialogOpenOn: {
                datasource,
                compactionConfig,
              },
            });
          },
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
          icon: IconNames.IMPORT,
          title: 'Mark as unused segments by interval',

          onAction: () =>
            this.setState({
              datasourceToMarkSegmentsByIntervalIn: datasource,
              useUnuseAction: 'unuse',
            }),
        },
        {
          icon: IconNames.IMPORT,
          title: 'Mark as unused all segments',
          intent: Intent.DANGER,
          onAction: () => this.setState({ datasourceToMarkAsUnusedAllSegmentsIn: datasource }),
        },
        {
          icon: IconNames.TRASH,
          title: 'Delete unused segments (issue kill task)',
          intent: Intent.DANGER,
          onAction: () => this.setState({ killDatasource: datasource }),
        },
      ]);
    }
  }

  renderRetentionDialog(): JSX.Element | undefined {
    const { retentionDialogOpenOn, tiersState, datasourcesAndDefaultRulesState } = this.state;
    const { defaultRules } = datasourcesAndDefaultRulesState.data || {
      datasources: [],
      defaultRules: [],
    };
    if (!retentionDialogOpenOn) return;

    return (
      <RetentionDialog
        datasource={retentionDialogOpenOn.datasource}
        rules={retentionDialogOpenOn.rules}
        tiers={tiersState.data || []}
        onEditDefaults={this.editDefaultRules}
        defaultRules={defaultRules}
        onCancel={() => this.setState({ retentionDialogOpenOn: undefined })}
        onSave={this.saveRules}
      />
    );
  }

  renderCompactionDialog() {
    const { datasourcesAndDefaultRulesState, compactionDialogOpenOn } = this.state;
    if (!compactionDialogOpenOn || !datasourcesAndDefaultRulesState.data) return;

    return (
      <CompactionDialog
        datasource={compactionDialogOpenOn.datasource}
        compactionConfig={compactionDialogOpenOn.compactionConfig}
        onClose={() => this.setState({ compactionDialogOpenOn: undefined })}
        onSave={this.saveCompaction}
        onDelete={this.deleteCompaction}
      />
    );
  }

  renderDatasourceTable() {
    const { goToSegments, capabilities } = this.props;
    const {
      datasourcesAndDefaultRulesState,
      datasourceFilter,
      showUnused,
      hiddenColumns,
    } = this.state;

    let { datasources, defaultRules } = datasourcesAndDefaultRulesState.data
      ? datasourcesAndDefaultRulesState.data
      : { datasources: [], defaultRules: [] };

    if (!showUnused) {
      datasources = datasources.filter(d => !d.unused);
    }

    // Calculate column values for bracing

    const totalDataSizeValues = datasources.map(d => formatTotalDataSize(d.total_data_size));

    const minSegmentRowsValues = datasources.map(d => formatSegmentRows(d.min_segment_rows));

    const avgSegmentRowsValues = datasources.map(d => formatSegmentRows(d.avg_segment_rows));

    const maxSegmentRowsValues = datasources.map(d => formatSegmentRows(d.max_segment_rows));

    const totalRowsValues = datasources.map(d => formatTotalRows(d.total_rows));

    const avgRowSizeValues = datasources.map(d => formatAvgRowSize(d.avg_row_size));

    const replicatedSizeValues = datasources.map(d => formatReplicatedSize(d.replicated_size));

    const leftToBeCompactedValues = datasources.map(d =>
      d.compactionStatus
        ? formatLeftToBeCompacted(d.compactionStatus.bytesAwaitingCompaction)
        : '-',
    );

    return (
      <>
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
          filtered={datasourceFilter}
          onFilteredChange={filtered => {
            this.setState({ datasourceFilter: filtered });
          }}
          columns={[
            {
              Header: twoLines('Datasource', 'name'),
              show: hiddenColumns.exists('Datasource name'),
              accessor: 'datasource',
              width: 150,
              Cell: ({ value }) => {
                return (
                  <a
                    onClick={() => {
                      this.setState({
                        datasourceFilter: addFilter(datasourceFilter, 'datasource', value),
                      });
                    }}
                  >
                    {value}
                  </a>
                );
              },
            },
            {
              Header: 'Availability',
              show: hiddenColumns.exists('Availability'),
              id: 'availability',
              filterable: false,
              minWidth: 200,
              accessor: row => {
                return {
                  num_available: row.num_available_segments,
                  num_total: row.num_segments,
                };
              },
              Cell: ({ original }) => {
                const { datasource, num_available_segments, num_segments, unused } = original;

                if (unused) {
                  return (
                    <span>
                      <span style={{ color: DatasourcesView.UNUSED_COLOR }}>&#x25cf;&nbsp;</span>
                      Unused
                    </span>
                  );
                }

                const segmentsEl = (
                  <a onClick={() => goToSegments(datasource)}>
                    {pluralIfNeeded(num_segments, 'segment')}
                  </a>
                );
                if (num_available_segments === num_segments) {
                  return (
                    <span>
                      <span style={{ color: DatasourcesView.FULLY_AVAILABLE_COLOR }}>
                        &#x25cf;&nbsp;
                      </span>
                      Fully available ({segmentsEl})
                    </span>
                  );
                } else {
                  const percentAvailable = (
                    Math.floor((num_available_segments / num_segments) * 1000) / 10
                  ).toFixed(1);
                  const missing = num_segments - num_available_segments;
                  const segmentsMissingEl = (
                    <a onClick={() => goToSegments(datasource, true)}>{`${pluralIfNeeded(
                      missing,
                      'segment',
                    )} unavailable`}</a>
                  );
                  return (
                    <span>
                      <span style={{ color: DatasourcesView.PARTIALLY_AVAILABLE_COLOR }}>
                        {num_available_segments ? '\u25cf' : '\u25cb'}&nbsp;
                      </span>
                      {percentAvailable}% available ({segmentsEl}, {segmentsMissingEl})
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
              Header: twoLines('Segment load/drop', 'queues'),
              show: hiddenColumns.exists('Segment load/drop queues'),
              id: 'load-drop',
              accessor: 'num_segments_to_load',
              filterable: false,
              minWidth: 100,
              Cell: ({ original }) => {
                const { num_segments_to_load, num_segments_to_drop } = original;
                return formatLoadDrop(num_segments_to_load, num_segments_to_drop);
              },
            },
            {
              Header: twoLines('Total', 'data size'),
              show: hiddenColumns.exists('Total data size'),
              accessor: 'total_data_size',
              filterable: false,
              width: 100,
              Cell: ({ value }) => (
                <BracedText text={formatTotalDataSize(value)} braces={totalDataSizeValues} />
              ),
            },
            {
              Header: twoLines('Segment size (rows)', 'minimum / average / maximum'),
              show: capabilities.hasSql() && hiddenColumns.exists('Segment size'),
              accessor: 'avg_segment_rows',
              filterable: false,
              width: 220,
              Cell: ({ value, original }) => (
                <>
                  <BracedText
                    text={formatSegmentRows(original.min_segment_rows)}
                    braces={minSegmentRowsValues}
                  />{' '}
                  &nbsp;{' '}
                  <BracedText text={formatSegmentRows(value)} braces={avgSegmentRowsValues} />{' '}
                  &nbsp;{' '}
                  <BracedText
                    text={formatSegmentRows(original.max_segment_rows)}
                    braces={maxSegmentRowsValues}
                  />
                </>
              ),
            },
            {
              Header: twoLines('Total', 'rows'),
              show: capabilities.hasSql() && hiddenColumns.exists('Total rows'),
              accessor: 'total_rows',
              filterable: false,
              width: 100,
              Cell: ({ value }) => (
                <BracedText text={formatTotalRows(value)} braces={totalRowsValues} />
              ),
            },
            {
              Header: twoLines('Avg. row size', '(bytes)'),
              show: capabilities.hasSql() && hiddenColumns.exists('Avg. row size'),
              accessor: 'avg_row_size',
              filterable: false,
              width: 100,
              Cell: ({ value }) => (
                <BracedText text={formatAvgRowSize(value)} braces={avgRowSizeValues} />
              ),
            },
            {
              Header: twoLines('Replicated', 'size'),
              show: capabilities.hasSql() && hiddenColumns.exists('Replicated size'),
              accessor: 'replicated_size',
              filterable: false,
              width: 100,
              Cell: ({ value }) => (
                <BracedText text={formatReplicatedSize(value)} braces={replicatedSizeValues} />
              ),
            },
            {
              Header: 'Compaction',
              show: capabilities.hasCoordinatorAccess() && hiddenColumns.exists('Compaction'),
              id: 'compactionStatus',
              accessor: row => Boolean(row.compactionStatus),
              filterable: false,
              width: 150,
              Cell: ({ original }) => {
                const { datasource, compactionConfig, compactionStatus } = original;
                return (
                  <span
                    className="clickable-cell"
                    onClick={() =>
                      this.setState({
                        compactionDialogOpenOn: {
                          datasource,
                          compactionConfig,
                        },
                      })
                    }
                  >
                    {formatCompactionConfigAndStatus(compactionConfig, compactionStatus)}&nbsp;
                    <ActionIcon icon={IconNames.EDIT} />
                  </span>
                );
              },
            },
            {
              Header: twoLines('% Compacted', 'bytes / segments / intervals'),
              show: capabilities.hasCoordinatorAccess() && hiddenColumns.exists('% Compacted'),
              id: 'percentCompacted',
              width: 200,
              accessor: ({ compactionStatus }) =>
                compactionStatus && compactionStatus.bytesCompacted
                  ? compactionStatus.bytesCompacted /
                    (compactionStatus.bytesAwaitingCompaction + compactionStatus.bytesCompacted)
                  : 0,
              filterable: false,
              Cell: ({ original }) => {
                const { compactionStatus } = original;

                if (!compactionStatus || zeroCompactionStatus(compactionStatus)) {
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
                        progress(
                          compactionStatus.bytesCompacted,
                          compactionStatus.bytesAwaitingCompaction,
                        ),
                      )}
                      braces={PERCENT_BRACES}
                    />{' '}
                    &nbsp;{' '}
                    <BracedText
                      text={formatPercent(
                        progress(
                          compactionStatus.segmentCountCompacted,
                          compactionStatus.segmentCountAwaitingCompaction,
                        ),
                      )}
                      braces={PERCENT_BRACES}
                    />{' '}
                    &nbsp;{' '}
                    <BracedText
                      text={formatPercent(
                        progress(
                          compactionStatus.intervalCountCompacted,
                          compactionStatus.intervalCountAwaitingCompaction,
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
                capabilities.hasCoordinatorAccess() && hiddenColumns.exists('Left to be compacted'),
              id: 'leftToBeCompacted',
              width: 100,
              accessor: ({ compactionStatus }) =>
                (compactionStatus && compactionStatus.bytesAwaitingCompaction) || 0,
              filterable: false,
              Cell: ({ original }) => {
                const { compactionStatus } = original;

                if (!compactionStatus) {
                  return <BracedText text="-" braces={leftToBeCompactedValues} />;
                }

                return (
                  <BracedText
                    text={formatLeftToBeCompacted(compactionStatus.bytesAwaitingCompaction)}
                    braces={leftToBeCompactedValues}
                  />
                );
              },
            },
            {
              Header: 'Retention',
              show: capabilities.hasCoordinatorAccess() && hiddenColumns.exists('Retention'),
              id: 'retention',
              accessor: row => row.rules.length,
              filterable: false,
              minWidth: 100,
              Cell: ({ original }) => {
                const { datasource, rules } = original;
                return (
                  <span
                    onClick={() =>
                      this.setState({
                        retentionDialogOpenOn: {
                          datasource,
                          rules,
                        },
                      })
                    }
                    className="clickable-cell"
                  >
                    {rules.length
                      ? DatasourcesView.formatRules(rules)
                      : `Cluster default: ${DatasourcesView.formatRules(defaultRules)}`}
                    &nbsp;
                    <ActionIcon icon={IconNames.EDIT} />
                  </span>
                );
              },
            },
            {
              Header: ACTION_COLUMN_LABEL,
              show: hiddenColumns.exists(ACTION_COLUMN_LABEL),
              accessor: 'datasource',
              id: ACTION_COLUMN_ID,
              width: ACTION_COLUMN_WIDTH,
              filterable: false,
              Cell: ({ value: datasource, original }) => {
                const { unused, rules, compactionConfig } = original;
                const datasourceActions = this.getDatasourceActions(
                  datasource,
                  unused,
                  rules,
                  compactionConfig,
                );
                return (
                  <ActionCell
                    onDetail={() => {
                      this.setState({
                        datasourceTableActionDialogId: datasource,
                        actions: datasourceActions,
                      });
                    }}
                    actions={datasourceActions}
                  />
                );
              },
            },
          ]}
          defaultPageSize={50}
        />
        {this.renderUnuseAction()}
        {this.renderUseAction()}
        {this.renderUseUnuseActionByInterval()}
        {this.renderKillAction()}
        {this.renderRetentionDialog()}
        {this.renderCompactionDialog()}
        {this.renderForceCompactAction()}
      </>
    );
  }

  render(): JSX.Element {
    const { capabilities } = this.props;
    const {
      showUnused,
      hiddenColumns,
      showChart,
      chartHeight,
      chartWidth,
      datasourceTableActionDialogId,
      actions,
    } = this.state;

    return (
      <div
        className={classNames('datasource-view app-view', showChart ? 'show-chart' : 'no-chart')}
      >
        <ViewControlBar label="Datasources">
          <RefreshButton
            onRefresh={auto => {
              this.refresh(auto);
            }}
            localStorageKey={LocalStorageKeys.DATASOURCES_REFRESH_RATE}
          />
          {this.renderBulkDatasourceActions()}
          <Switch
            checked={showChart}
            label="Show segment timeline"
            onChange={() => this.setState({ showChart: !showChart })}
            disabled={!capabilities.hasSqlOrCoordinatorAccess()}
          />
          <Switch
            checked={showUnused}
            label="Show unused"
            onChange={() => this.toggleUnused(showUnused)}
            disabled={!capabilities.hasCoordinatorAccess()}
          />
          <TableColumnSelector
            columns={tableColumns[capabilities.getMode()]}
            onChange={column =>
              this.setState(prevState => ({
                hiddenColumns: prevState.hiddenColumns.toggle(column),
              }))
            }
            tableColumnsHidden={hiddenColumns.storedArray}
          />
        </ViewControlBar>
        {showChart && (
          <div className={'chart-container'}>
            <SegmentTimeline
              capabilities={capabilities}
              chartHeight={chartHeight}
              chartWidth={chartWidth}
            />
          </div>
        )}
        {this.renderDatasourceTable()}
        {datasourceTableActionDialogId && (
          <DatasourceTableActionDialog
            datasourceId={datasourceTableActionDialogId}
            actions={actions}
            onClose={() => this.setState({ datasourceTableActionDialogId: undefined })}
          />
        )}
      </div>
    );
  }
}
