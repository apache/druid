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
import React from 'react';
import ReactTable, { Filter } from 'react-table';

import {
  ACTION_COLUMN_ID,
  ACTION_COLUMN_LABEL,
  ACTION_COLUMN_WIDTH,
  ActionCell,
  ActionIcon,
  MoreButton,
  RefreshButton,
  TableColumnSelector,
  ViewControlBar,
} from '../../components';
import { SegmentTimeline } from '../../components/segment-timeline/segment-timeline';
import { AsyncActionDialog, CompactionDialog, RetentionDialog } from '../../dialogs';
import { DatasourceTableActionDialog } from '../../dialogs/datasource-table-action-dialog/datasource-table-action-dialog';
import { AppToaster } from '../../singletons/toaster';
import {
  addFilter,
  countBy,
  escapeSqlIdentifier,
  formatBytes,
  formatNumber,
  getDruidErrorMessage,
  LocalStorageKeys,
  lookupBy,
  pluralIfNeeded,
  queryDruidSql,
  QueryManager,
} from '../../utils';
import { BasicAction } from '../../utils/basic-action';
import { Capabilities, CapabilitiesMode } from '../../utils/capabilities';
import { RuleUtil } from '../../utils/load-rule';
import { LocalStorageBackedArray } from '../../utils/local-storage-backed-array';
import { deepGet } from '../../utils/object-change';

import './datasource-view.scss';

const tableColumns: Record<CapabilitiesMode, string[]> = {
  full: [
    'Datasource',
    'Availability',
    'Segment load/drop',
    'Retention',
    'Replicated size',
    'Size',
    'Compaction',
    'Avg. segment size',
    'Num rows',
    ACTION_COLUMN_LABEL,
  ],
  'no-sql': [
    'Datasource',
    'Availability',
    'Segment load/drop',
    'Retention',
    'Size',
    'Compaction',
    'Avg. segment size',
    ACTION_COLUMN_LABEL,
  ],
  'no-proxy': [
    'Datasource',
    'Availability',
    'Segment load/drop',
    'Replicated size',
    'Size',
    'Avg. segment size',
    'Num rows',
    ACTION_COLUMN_LABEL,
  ],
};

function formatLoadDrop(segmentsToLoad: number, segmentsToDrop: number): string {
  const loadDrop: string[] = [];
  if (segmentsToLoad) {
    loadDrop.push(`${segmentsToLoad} segments to load`);
  }
  if (segmentsToDrop) {
    loadDrop.push(`${segmentsToDrop} segments to drop`);
  }
  return loadDrop.join(', ') || 'No segments to load/drop';
}

interface Datasource {
  datasource: string;
  rules: any[];
  [key: string]: any;
}

interface DatasourceQueryResultRow {
  datasource: string;
  num_segments: number;
  num_available_segments: number;
  num_segments_to_load: number;
  num_segments_to_drop: number;
  replicated_size: number;
  size: number;
  avg_segment_size: number;
  num_rows: number;
}

interface RetentionDialogOpenOn {
  datasource: string;
  rules: any[];
}

interface CompactionDialogOpenOn {
  datasource: string;
  compactionConfig: Record<string, any>;
}

export interface DatasourcesViewProps {
  goToQuery: (initSql: string) => void;
  goToTask: (datasource?: string, openDialog?: string) => void;
  goToSegments: (datasource: string, onlyUnavailable?: boolean) => void;
  capabilities: Capabilities;
  initDatasource?: string;
}

export interface DatasourcesViewState {
  datasourcesLoading: boolean;
  datasources: Datasource[] | null;
  tiers: string[];
  defaultRules: any[];
  datasourcesError?: string;
  datasourceFilter: Filter[];

  showUnused: boolean;
  retentionDialogOpenOn?: RetentionDialogOpenOn;
  compactionDialogOpenOn?: CompactionDialogOpenOn;
  datasourceToMarkAsUnusedAllSegmentsIn?: string;
  datasourceToMarkAllNonOvershadowedSegmentsAsUsedIn?: string;
  killDatasource?: string;
  datasourceToMarkSegmentsByIntervalIn?: string;
  useUnuseAction: 'use' | 'unuse';
  useUnuseInterval: string;
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
  SUM("size" * "num_replicas") FILTER (WHERE (is_published = 1 AND is_overshadowed = 0) OR is_realtime = 1) AS replicated_size,
  SUM("size") FILTER (WHERE (is_published = 1 AND is_overshadowed = 0) OR is_realtime = 1) AS size,
  (
    SUM("size") FILTER (WHERE (is_published = 1 AND is_overshadowed = 0) OR is_realtime = 1) /
    COUNT(*) FILTER (WHERE (is_published = 1 AND is_overshadowed = 0) OR is_realtime = 1)
  ) AS avg_segment_size,
  SUM("num_rows") FILTER (WHERE (is_published = 1 AND is_overshadowed = 0) OR is_realtime = 1) AS num_rows
FROM sys.segments
GROUP BY 1`;

  static formatRules(rules: any[]): string {
    if (rules.length === 0) {
      return 'No rules';
    } else if (rules.length <= 2) {
      return rules.map(RuleUtil.ruleToString).join(', ');
    } else {
      return `${RuleUtil.ruleToString(rules[0])} +${rules.length - 1} more rules`;
    }
  }

  private datasourceQueryManager: QueryManager<
    Capabilities,
    { tiers: string[]; defaultRules: any[]; datasources: Datasource[] }
  >;

  constructor(props: DatasourcesViewProps, context: any) {
    super(props, context);

    const datasourceFilter: Filter[] = [];
    if (props.initDatasource) {
      datasourceFilter.push({ id: 'datasource', value: `"${props.initDatasource}"` });
    }

    this.state = {
      datasourcesLoading: true,
      datasources: null,
      tiers: [],
      defaultRules: [],
      datasourceFilter,

      showUnused: false,
      useUnuseAction: 'unuse',
      useUnuseInterval: '',
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
              const size = deepGet(d, 'properties.segments.size') || -1;
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
                size,
                avg_segment_size: size / numSegments,
                num_rows: -1,
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
            tiers: [],
            defaultRules: [],
          };
        }

        const seen = countBy(datasources, (x: any) => x.datasource);

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

        const compactionResp = await axios.get('/druid/coordinator/v1/config/compaction');
        const compaction = lookupBy(
          compactionResp.data.compactionConfigs,
          (c: any) => c.dataSource,
        );

        const tiersResp = await axios.get('/druid/coordinator/v1/tiers');
        const tiers = tiersResp.data;

        const allDatasources = (datasources as any).concat(
          unused.map(d => ({ datasource: d, unused: true })),
        );
        allDatasources.forEach((ds: any) => {
          ds.rules = rules[ds.datasource] || [];
          ds.compaction = compaction[ds.datasource];
        });

        return {
          datasources: allDatasources,
          tiers,
          defaultRules: rules['_default'],
        };
      },
      onStateChange: ({ result, loading, error }) => {
        this.setState({
          datasourcesLoading: loading,
          datasources: result ? result.datasources : null,
          tiers: result ? result.tiers : [],
          defaultRules: result ? result.defaultRules : [],
          datasourcesError: error || undefined,
        });
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
  };

  componentDidMount(): void {
    const { capabilities } = this.props;
    this.datasourceQueryManager.runQuery(capabilities);
    window.addEventListener('resize', this.handleResize);
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
          const resp = await axios.delete(
            `/druid/coordinator/v1/datasources/${datasourceToMarkAsUnusedAllSegmentsIn}`,
            {},
          );
          return resp.data;
        }}
        confirmButtonText="Mark as unused all segments"
        successText="All segments in data source have been marked as unused"
        failText="Failed to mark as unused all segments in data source"
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
        successText="All non-overshadowed segments in data source have been marked as used"
        failText="Failed to mark as used all non-overshadowed segments in data source"
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
        successText={`Segments in the interval in data source have been marked as ${usedWord}`}
        failText={`Failed to mark as ${usedWord} segments in the interval in data source`}
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
        successText="Kill task was issued. Unused segments in data source will be deleted"
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
      <MoreButton>
        {capabilities.hasSql() && (
          <MenuItem
            icon={IconNames.APPLICATION}
            text="View SQL query for table"
            onClick={() => goToQuery(DatasourcesView.DATASOURCE_SQL)}
          />
        )}
      </MoreButton>
    );
  }

  private saveRules = async (datasource: string, rules: any[], comment: string) => {
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
    const { datasources, defaultRules } = this.state;
    if (!datasources) return;

    this.setState({ retentionDialogOpenOn: undefined });
    setTimeout(() => {
      this.setState({
        retentionDialogOpenOn: {
          datasource: '_default',
          rules: defaultRules,
        },
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
    rules: any[],
    compactionConfig: Record<string, any>,
  ): BasicAction[] {
    const { goToQuery, goToTask, capabilities } = this.props;

    const goToActions: BasicAction[] = [
      {
        icon: IconNames.APPLICATION,
        title: 'Query with SQL',
        onAction: () => goToQuery(`SELECT * FROM ${escapeSqlIdentifier(datasource)}`),
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

  renderRetentionDialog() {
    const { retentionDialogOpenOn, tiers } = this.state;
    if (!retentionDialogOpenOn) return null;

    return (
      <RetentionDialog
        datasource={retentionDialogOpenOn.datasource}
        rules={retentionDialogOpenOn.rules}
        tiers={tiers}
        onEditDefaults={this.editDefaultRules}
        onCancel={() => this.setState({ retentionDialogOpenOn: undefined })}
        onSave={this.saveRules}
      />
    );
  }

  renderCompactionDialog() {
    const { datasources, compactionDialogOpenOn } = this.state;

    if (!compactionDialogOpenOn || !datasources) return;

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
      datasources,
      defaultRules,
      datasourcesLoading,
      datasourcesError,
      datasourceFilter,
      showUnused,
      hiddenColumns,
    } = this.state;
    let data = datasources || [];
    if (!showUnused) {
      data = data.filter(d => !d.unused);
    }
    return (
      <>
        <ReactTable
          data={data}
          loading={datasourcesLoading}
          noDataText={
            !datasourcesLoading && datasources && !datasources.length
              ? 'No datasources'
              : datasourcesError || ''
          }
          filterable
          filtered={datasourceFilter}
          onFilteredChange={filtered => {
            this.setState({ datasourceFilter: filtered });
          }}
          columns={[
            {
              Header: 'Datasource',
              accessor: 'datasource',
              width: 150,
              Cell: row => {
                const value = row.value;
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
              show: hiddenColumns.exists('Datasource'),
            },
            {
              Header: 'Availability',
              id: 'availability',
              filterable: false,
              accessor: row => {
                return {
                  num_available: row.num_available_segments,
                  num_total: row.num_segments,
                };
              },
              Cell: row => {
                const { datasource, num_available_segments, num_segments, unused } = row.original;

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
              show: hiddenColumns.exists('Availability'),
            },
            {
              Header: 'Segment load/drop',
              id: 'load-drop',
              accessor: 'num_segments_to_load',
              filterable: false,
              Cell: row => {
                const { num_segments_to_load, num_segments_to_drop } = row.original;
                return formatLoadDrop(num_segments_to_load, num_segments_to_drop);
              },
              show: hiddenColumns.exists('Segment load/drop'),
            },
            {
              Header: 'Retention',
              id: 'retention',
              accessor: row => row.rules.length,
              filterable: false,
              Cell: row => {
                const { rules } = row.original;
                let text: string;
                if (rules.length === 0) {
                  text = 'Cluster default: ' + DatasourcesView.formatRules(defaultRules);
                } else {
                  text = DatasourcesView.formatRules(rules);
                }

                return (
                  <span
                    onClick={() =>
                      this.setState({
                        retentionDialogOpenOn: {
                          datasource: row.original.datasource,
                          rules: row.original.rules,
                        },
                      })
                    }
                    className="clickable-cell"
                  >
                    {text}&nbsp;
                    <ActionIcon icon={IconNames.EDIT} />
                  </span>
                );
              },
              show: capabilities.hasCoordinatorAccess() && hiddenColumns.exists('Retention'),
            },
            {
              Header: 'Replicated size',
              accessor: 'replicated_size',
              filterable: false,
              width: 100,
              Cell: row => formatBytes(row.value),
              show: capabilities.hasSql() && hiddenColumns.exists('Replicated size'),
            },
            {
              Header: 'Size',
              accessor: 'size',
              filterable: false,
              width: 100,
              Cell: row => formatBytes(row.value),
              show: hiddenColumns.exists('Size'),
            },
            {
              Header: 'Compaction',
              id: 'compaction',
              accessor: row => Boolean(row.compaction),
              filterable: false,
              Cell: row => {
                const { compaction } = row.original;
                let text: string;
                if (compaction) {
                  if (compaction.maxRowsPerSegment == null) {
                    text = `Target: Default (${formatNumber(
                      CompactionDialog.DEFAULT_MAX_ROWS_PER_SEGMENT,
                    )})`;
                  } else {
                    text = `Target: ${formatNumber(compaction.maxRowsPerSegment)}`;
                  }
                } else {
                  text = 'None';
                }
                return (
                  <span
                    className="clickable-cell"
                    onClick={() =>
                      this.setState({
                        compactionDialogOpenOn: {
                          datasource: row.original.datasource,
                          compactionConfig: compaction,
                        },
                      })
                    }
                  >
                    {text}&nbsp;
                    <ActionIcon icon={IconNames.EDIT} />
                  </span>
                );
              },
              show: capabilities.hasCoordinatorAccess() && hiddenColumns.exists('Compaction'),
            },
            {
              Header: 'Avg. segment size',
              accessor: 'avg_segment_size',
              filterable: false,
              width: 100,
              Cell: row => formatBytes(row.value),
              show: hiddenColumns.exists('Avg. segment size'),
            },
            {
              Header: 'Num rows',
              accessor: 'num_rows',
              filterable: false,
              width: 100,
              Cell: row => formatNumber(row.value),
              show: capabilities.hasSql() && hiddenColumns.exists('Num rows'),
            },
            {
              Header: ACTION_COLUMN_LABEL,
              accessor: 'datasource',
              id: ACTION_COLUMN_ID,
              width: ACTION_COLUMN_WIDTH,
              filterable: false,
              Cell: row => {
                const datasource = row.value;
                const { unused, rules, compaction } = row.original;
                const datasourceActions = this.getDatasourceActions(
                  datasource,
                  unused,
                  rules,
                  compaction,
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
              show: hiddenColumns.exists(ACTION_COLUMN_LABEL),
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
