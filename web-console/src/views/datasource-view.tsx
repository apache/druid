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

import { Button, Icon, InputGroup, Intent, Popover, Position, Switch } from '@blueprintjs/core';
import { FormGroup } from '@blueprintjs/core/lib/esnext';
import { IconNames } from '@blueprintjs/icons';
import axios from 'axios';
import * as React from 'react';
import ReactTable, { Filter } from 'react-table';

import { ActionCell } from '../components/action-cell';
import { RuleEditor } from '../components/rule-editor';
import { TableColumnSelection } from '../components/table-column-selection';
import { ViewControlBar } from '../components/view-control-bar';
import { AsyncActionDialog } from '../dialogs/async-action-dialog';
import { CompactionDialog } from '../dialogs/compaction-dialog';
import { RetentionDialog } from '../dialogs/retention-dialog';
import { AppToaster } from '../singletons/toaster';
import {
  addFilter,
  countBy,
  formatBytes,
  formatNumber,
  getDruidErrorMessage, LocalStorageKeys,
  lookupBy,
  pluralIfNeeded,
  queryDruidSql,
  QueryManager, TableColumnSelectionHandler
} from '../utils';
import { BasicAction, basicActionsToMenu } from '../utils/basic-action';

import './datasource-view.scss';

const tableColumns: string[] = ['Datasource', 'Availability', 'Retention', 'Compaction', 'Size', 'Num rows', 'Actions'];
const tableColumnsNoSql: string[] = ['Datasource', 'Availability', 'Retention', 'Compaction', 'Size', 'Actions'];

export interface DatasourcesViewProps extends React.Props<any> {
  goToSql: (initSql: string) => void;
  goToSegments: (datasource: string, onlyUnavailable?: boolean) => void;
  noSqlMode: boolean;
}

interface Datasource {
  datasource: string;
  rules: any[];
  [key: string]: any;
}

interface DatasourceQueryResultRow {
  datasource: string;
  num_available_segments: number;
  num_rows: number;
  num_segments: number;
  size: number;
}

export interface DatasourcesViewState {
  datasourcesLoading: boolean;
  datasources: Datasource[] | null;
  tiers: string[];
  defaultRules: any[];
  datasourcesError: string | null;
  datasourcesFilter: Filter[];

  showUnused: boolean;
  retentionDialogOpenOn: { datasource: string, rules: any[] } | null;
  compactionDialogOpenOn: { datasource: string, configData: any } | null;
  dataSourceToMarkAsUnusedAllSegmentsIn: string | null;
  dataSourceToMarkAsUsedAllNonOvershadowedSegmentsIn: string | null;
  dataSourceToKillAllSegmentsIn: string | null;
  dataSourceToMarkSegmentsAsUsedOrUnusedIn: string | null;
  markAsUsedOrUnusedAction: 'markUsed' | 'markUnused';
  markAsUsedOrUnusedInterval: string;
}

export class DatasourcesView extends React.Component<DatasourcesViewProps, DatasourcesViewState> {
  static UNUSED_COLOR = '#0a1500';
  static FULLY_AVAILABLE_COLOR = '#57d500';
  static PARTIALLY_AVAILABLE_COLOR = '#ffbf00';

  static formatRules(rules: any[]): string {
    if (rules.length === 0) {
      return 'No rules';
    } else if (rules.length <= 2) {
      return rules.map(RuleEditor.ruleToString).join(', ');
    } else {
      return `${RuleEditor.ruleToString(rules[0])} +${rules.length - 1} more rules`;
    }
  }

  private datasourceQueryManager: QueryManager<string, { tiers: string[], defaultRules: any[], datasources: Datasource[] }>;
  private tableColumnSelectionHandler: TableColumnSelectionHandler;

  constructor(props: DatasourcesViewProps, context: any) {
    super(props, context);
    this.state = {
      datasourcesLoading: true,
      datasources: null,
      tiers: [],
      defaultRules: [],
      datasourcesError: null,
      datasourcesFilter: [],

      showUnused: false,
      retentionDialogOpenOn: null,
      compactionDialogOpenOn: null,
      dataSourceToMarkAsUnusedAllSegmentsIn: null,
      dataSourceToMarkAsUsedAllNonOvershadowedSegmentsIn: null,
      dataSourceToKillAllSegmentsIn: null,
      dataSourceToMarkSegmentsAsUsedOrUnusedIn: null,
      markAsUsedOrUnusedAction: 'markUnused',
      markAsUsedOrUnusedInterval: ''
    };

    this.tableColumnSelectionHandler = new TableColumnSelectionHandler(
      LocalStorageKeys.DATASOURCE_TABLE_COLUMN_SELECTION, () => this.setState({})
    );
  }

  componentDidMount(): void {
    const { noSqlMode } = this.props;

    this.datasourceQueryManager = new QueryManager({
      processQuery: async (query: string) => {
        let datasources: DatasourceQueryResultRow[];
        if (!noSqlMode) {
          datasources = await queryDruidSql({ query });
        } else {
          const datasourcesResp = await axios.get('/druid/coordinator/v1/datasources?simple');
          const loadstatusResp = await axios.get('/druid/coordinator/v1/loadstatus?simple');
          const loadstatus = loadstatusResp.data;
          datasources = datasourcesResp.data.map((d: any) => {
            return {
              datasource: d.name,
              num_available_segments: d.properties.segments.count,
              size: d.properties.segments.size,
              num_segments: d.properties.segments.count + loadstatus[d.name],
              num_rows: -1
            };
          });
        }

        const seen = countBy(datasources, (x: any) => x.datasource);

        const allDataSourcesResp = await axios.get('/druid/coordinator/v1/metadata/datasources?includeUnused');
        const unused: string[] = allDataSourcesResp.data.filter((d: string) => !seen[d]);

        const rulesResp = await axios.get('/druid/coordinator/v1/rules');
        const rules = rulesResp.data;

        const compactionResp = await axios.get('/druid/coordinator/v1/config/compaction');
        const compaction = lookupBy(compactionResp.data.compactionConfigs, (c: any) => c.dataSource);

        const tiersResp = await axios.get('/druid/coordinator/v1/tiers');
        const tiers = tiersResp.data;

        const allDatasources = (datasources as any).concat(unused.map(d => ({ datasource: d, unused: true })));
        allDatasources.forEach((ds: any) => {
          ds.rules = rules[ds.datasource] || [];
          ds.compaction = compaction[ds.datasource];
        });

        return {
          datasources: allDatasources,
          tiers,
          defaultRules: rules['_default']
        };
      },
      onStateChange: ({ result, loading, error }) => {
        this.setState({
          datasourcesLoading: loading,
          datasources: result ? result.datasources : null,
          tiers: result ? result.tiers : [],
          defaultRules: result ? result.defaultRules : [],
          datasourcesError: error
        });
      }
    });

    this.datasourceQueryManager.runQuery(`SELECT
  datasource,
  COUNT(*) AS num_segments,
  SUM(is_available) AS num_available_segments,
  SUM("size") AS size,
  SUM("num_rows") AS num_rows
FROM sys.segments
GROUP BY 1`);

  }

  componentWillUnmount(): void {
    this.datasourceQueryManager.terminate();
  }

  renderMarkAsUnusedAllSegmentsInDataSourceAction() {
    const { dataSourceToMarkAsUnusedAllSegmentsIn } = this.state;

    return <AsyncActionDialog
      action={
        dataSourceToMarkAsUnusedAllSegmentsIn ? async () => {
          const resp = await axios.delete(`/druid/coordinator/v1/datasources/${dataSourceToMarkAsUnusedAllSegmentsIn}`, {});
          return resp.data;
        } : null
      }
      confirmButtonText="Mark as unused all segments belonging to data source"
      successText="All segments belonging to data source has been marked as unused"
      failText="Failed to mark as unused all segments belonging to data source"
      intent={Intent.DANGER}
      onClose={(success) => {
        this.setState({ dataSourceToMarkAsUnusedAllSegmentsIn: null });
        if (success) this.datasourceQueryManager.rerunLastQuery();
      }}
    >
      <p>
        {`Are you sure you want to mark as unused all segments in data source '${dataSourceToMarkAsUnusedAllSegmentsIn}'?`}
      </p>
    </AsyncActionDialog>;
  }

  renderMarkAsUsedAllNonOvershadowedSegmentsInDataSourceAction() {
    const { dataSourceToMarkAsUsedAllNonOvershadowedSegmentsIn } = this.state;

    return <AsyncActionDialog
      action={
        dataSourceToMarkAsUsedAllNonOvershadowedSegmentsIn ? async () => {
          const resp = await axios.post(`/druid/coordinator/v1/datasources/${dataSourceToMarkAsUsedAllNonOvershadowedSegmentsIn}`, {});
          return resp.data;
        } : null
      }
      confirmButtonText="Mark as used all non-overshadowed segments belonging to data source"
      successText="All non-overshadowed segments belonging to data source has been marked as used"
      failText="Failed to mark as used all non-overshadowed segments belonging to data source"
      intent={Intent.PRIMARY}
      onClose={(success) => {
        this.setState({ dataSourceToMarkAsUsedAllNonOvershadowedSegmentsIn: null });
        if (success) this.datasourceQueryManager.rerunLastQuery();
      }}
    >
      <p>
        {`Are you sure you want to mark as used all non-overshadowed segments belonging to data source '${dataSourceToMarkAsUsedAllNonOvershadowedSegmentsIn}'?`}
      </p>
    </AsyncActionDialog>;
  }

  renderMarkAsUsedOrUnusedSegmentsInIntervalAction() {
    const { dataSourceToMarkSegmentsAsUsedOrUnusedIn, markAsUsedOrUnusedAction, markAsUsedOrUnusedInterval } = this.state;
    const usedOrUnused = markAsUsedOrUnusedAction === 'markUsed' ? 'used' : 'unused';

    return <AsyncActionDialog
      action={
        dataSourceToMarkSegmentsAsUsedOrUnusedIn ? async () => {
          if (!markAsUsedOrUnusedInterval) return;
          const resp = await axios.post(
              `/druid/coordinator/v1/datasources/${dataSourceToMarkSegmentsAsUsedOrUnusedIn}/${markAsUsedOrUnusedAction}`,
              { interval: markAsUsedOrUnusedInterval }
          );
          return resp.data;
        } : null
      }
      confirmButtonText={`Mark as ${usedOrUnused} segments belonging to data source in the specified interval`}
      confirmButtonDisabled={!/.\/./.test(markAsUsedOrUnusedInterval)}
      successText={`Segments belonging to data source in the specified interval has been marked as ${usedOrUnused}`}
      failText={`Failed to mark segments as ${usedOrUnused}`}
      intent={Intent.PRIMARY}
      onClose={(success) => {
        this.setState({ dataSourceToMarkSegmentsAsUsedOrUnusedIn: null });
        if (success) this.datasourceQueryManager.rerunLastQuery();
      }}
    >
      <p>
        {`Please select the interval in which you want to mark segments as ${usedOrUnused}`}
      </p>
      <FormGroup>
        <InputGroup
          value={markAsUsedOrUnusedInterval}
          onChange={(e: any) => {
            const v = e.target.value;
            this.setState({ markAsUsedOrUnusedInterval: v.toUpperCase() });
          }}
          placeholder="2018-01-01T00:00:00/2018-01-03T00:00:00"
        />
      </FormGroup>
    </AsyncActionDialog>;
  }

  renderKillAction() {
    const { dataSourceToKillAllSegmentsIn } = this.state;

    return <AsyncActionDialog
      action={
        dataSourceToKillAllSegmentsIn ? async () => {
          const resp = await axios.delete(`/druid/coordinator/v1/datasources/${dataSourceToKillAllSegmentsIn}?kill=true&interval=1000/3000`, {});
          return resp.data;
        } : null
      }
      confirmButtonText="Permanently delete (kill) all segments belonging to data source"
      successText="Kill task was issued. All segments belonging to data source will be permanently deleted"
      failText="Failed to submit a task to permanently delete (kill) all segments belonging to data source"
      intent={Intent.DANGER}
      onClose={(success) => {
        this.setState({ dataSourceToKillAllSegmentsIn: null });
        if (success) this.datasourceQueryManager.rerunLastQuery();
      }}
    >
      <p>
        {`Are you sure you want to permanently delete (kill) all segments belonging to data source '${dataSourceToKillAllSegmentsIn}'?`}
      </p>
      <p>
        This action can not be undone.
      </p>
    </AsyncActionDialog>;
  }

  private saveRules = async (datasource: string, rules: any[], comment: string) => {
    try {
      await axios.post(`/druid/coordinator/v1/rules/${datasource}`, rules, {
        headers: {
          'X-Druid-Author': 'console',
          'X-Druid-Comment': comment
        }
      });
    } catch (e) {
      AppToaster.show({
        message: `Failed to submit retention rules: ${getDruidErrorMessage(e)}`,
        intent: Intent.DANGER
      });
      return;
    }

    AppToaster.show({
      message: 'Retention rules submitted successfully',
      intent: Intent.SUCCESS
    });
    this.datasourceQueryManager.rerunLastQuery();
  }

  private editDefaultRules = () => {
    const { datasources, defaultRules } = this.state;
    if (!datasources) return;

    this.setState({ retentionDialogOpenOn: null });
    setTimeout(() => {
      this.setState({
        retentionDialogOpenOn: {
          datasource: '_default',
          rules: defaultRules
        }
      });
    }, 50);
  }

  private saveCompaction = async (compactionConfig: any) => {
    if (compactionConfig === null) return;
    try {
      await axios.post(`/druid/coordinator/v1/config/compaction`, compactionConfig);
      this.setState({compactionDialogOpenOn: null});
      this.datasourceQueryManager.rerunLastQuery();
    } catch (e) {
      AppToaster.show({
        message: e,
        intent: Intent.DANGER
      });
    }
  }

  private deleteCompaction = async () => {
    const {compactionDialogOpenOn} = this.state;
    if (compactionDialogOpenOn === null) return;
    const datasource = compactionDialogOpenOn.datasource;
    AppToaster.show({
      message: `Are you sure you want to delete ${datasource}'s compaction?`,
      intent: Intent.DANGER,
      action: {
        text: 'Confirm',
        onClick: async () => {
          try {
            await axios.delete(`/druid/coordinator/v1/config/compaction/${datasource}`);
            this.setState({compactionDialogOpenOn: null}, () => this.datasourceQueryManager.rerunLastQuery());
          } catch (e) {
            AppToaster.show({
              message: e,
              intent: Intent.DANGER
            });
          }
        }
      }
    });
  }

  getDatasourceActions(datasource: string, unused: boolean): BasicAction[] {
    if (unused) {
      return [
        {
          icon: IconNames.EXPORT,
          title: 'Mark as used all non-overshadowed segments in data source',
          onAction: () => this.setState({ dataSourceToMarkAsUsedAllNonOvershadowedSegmentsIn: datasource })
        },
        {
          icon: IconNames.TRASH,
          title: 'Permanently delete (kill) all segments in data source',
          intent: Intent.DANGER,
          onAction: () => this.setState({ dataSourceToKillAllSegmentsIn: datasource })
        }
      ];
    } else {
      return [
        {
          icon: IconNames.EXPORT,
          title: 'Mark segments as used by interval in data source',
          onAction: () => this.setState({ dataSourceToMarkSegmentsAsUsedOrUnusedIn: datasource, markAsUsedOrUnusedAction: 'markUsed' })
        },
        {
          icon: IconNames.IMPORT,
          title: 'Mark segments as unused by interval in data source',
          onAction: () => this.setState({ dataSourceToMarkSegmentsAsUsedOrUnusedIn: datasource, markAsUsedOrUnusedAction: 'markUnused' })
        },
        {
          icon: IconNames.IMPORT,
          title: 'Mark as unused all segments in data source',
          intent: Intent.DANGER,
          onAction: () => this.setState({ dataSourceToMarkAsUnusedAllSegmentsIn: datasource })
        }
      ];
    }
  }

  renderRetentionDialog() {
    const { retentionDialogOpenOn, tiers } = this.state;
    if (!retentionDialogOpenOn) return null;

    return <RetentionDialog
      datasource={retentionDialogOpenOn.datasource}
      rules={retentionDialogOpenOn.rules}
      tiers={tiers}
      onEditDefaults={this.editDefaultRules}
      onCancel={() => this.setState({ retentionDialogOpenOn: null })}
      onSave={this.saveRules}
    />;
  }

  renderCompactionDialog() {
    const { datasources, compactionDialogOpenOn } = this.state;

    if (!compactionDialogOpenOn || !datasources) return;

    return <CompactionDialog
      datasource={compactionDialogOpenOn.datasource}
      configData={compactionDialogOpenOn.configData}
      onClose={() => this.setState({compactionDialogOpenOn: null})}
      onSave={this.saveCompaction}
      onDelete={this.deleteCompaction}
    />;
  }

  renderDatasourceTable() {
    const { goToSegments, noSqlMode } = this.props;
    const { datasources, defaultRules, datasourcesLoading, datasourcesError, datasourcesFilter, showUnused } = this.state;
    const { tableColumnSelectionHandler } = this;
    let data = datasources || [];
    if (!showUnused) {
      data = data.filter(d => !d.unused);
    }
    return <>
      <ReactTable
        data={data}
        loading={datasourcesLoading}
        noDataText={!datasourcesLoading && datasources && !datasources.length ? 'No datasources' : (datasourcesError || '')}
        filterable
        filtered={datasourcesFilter}
        onFilteredChange={(filtered, column) => {
          this.setState({ datasourcesFilter: filtered });
        }}
        columns={[
          {
            Header: 'Datasource',
            accessor: 'datasource',
            width: 150,
            Cell: row => {
              const value = row.value;
              return <a onClick={() => { this.setState({ datasourcesFilter: addFilter(datasourcesFilter, 'datasource', value) }); }}>{value}</a>;
            },
            show: tableColumnSelectionHandler.showColumn('Datasource')
          },
          {
            Header: 'Availability',
            id: 'availability',
            filterable: false,
            accessor: (row) => {
              return {
                num_available: row.num_available_segments,
                num_total: row.num_segments
              };
            },
            Cell: (row) => {
              const { datasource, num_available_segments, num_segments, unused } = row.original;

              if (unused) {
                return <span>
                  <span style={{ color: DatasourcesView.UNUSED_COLOR }}>&#x25cf;&nbsp;</span>
                  Unused
                </span>;
              }

              const segmentsEl = <a onClick={() => goToSegments(datasource)}>{pluralIfNeeded(num_segments, 'segment')}</a>;
              if (num_available_segments === num_segments) {
                return <span>
                  <span style={{ color: DatasourcesView.FULLY_AVAILABLE_COLOR }}>&#x25cf;&nbsp;</span>
                  Fully available ({segmentsEl})
                </span>;

              } else {
                const percentAvailable = (Math.floor((num_available_segments / num_segments) * 1000) / 10).toFixed(1);
                const missing = num_segments - num_available_segments;
                const segmentsMissingEl = <a onClick={() => goToSegments(datasource, true)}>{`${pluralIfNeeded(missing, 'segment')} unavailable`}</a>;
                return <span>
                  <span style={{ color: DatasourcesView.PARTIALLY_AVAILABLE_COLOR }}>&#x25cf;&nbsp;</span>
                  {percentAvailable}% available ({segmentsEl}, {segmentsMissingEl})
                </span>;

              }
            },
            sortMethod: (d1, d2) => {
              const percentAvailable1 = d1.num_available / d1.num_total;
              const percentAvailable2 = d2.num_available / d2.num_total;
              return (percentAvailable1 - percentAvailable2) || (d1.num_total - d2.num_total);
            },
            show: tableColumnSelectionHandler.showColumn('Availability')
          },
          {
            Header: 'Retention',
            id: 'retention',
            accessor: (row) => row.rules.length,
            filterable: false,
            Cell: row => {
              const { rules } = row.original;
              let text: string;
              if (rules.length === 0) {
                text = 'Cluster default: ' + DatasourcesView.formatRules(defaultRules);
              } else {
                text = DatasourcesView.formatRules(rules);
              }

              return <span
                onClick={() => this.setState({retentionDialogOpenOn: { datasource: row.original.datasource, rules: row.original.rules }})}
                className="clickable-cell"
              >
                {text}&nbsp;
                <a>&#x270E;</a>
              </span>;
            },
            show: tableColumnSelectionHandler.showColumn('Retention')
          },
          {
            Header: 'Compaction',
            id: 'compaction',
            accessor: (row) => Boolean(row.compaction),
            filterable: false,
            Cell: row => {
              const { compaction } = row.original;
              const compactionOpenOn: {datasource: string, configData: any} | null = {
                datasource: row.original.datasource,
                configData: compaction
              };
              let text: string;
              if (compaction) {
                text = `Target: ${formatBytes(compaction.targetCompactionSizeBytes)}`;
              } else {
                text = 'None';
              }
              return <span
                className="clickable-cell"
                onClick={() => this.setState({compactionDialogOpenOn: compactionOpenOn})}
              >
                {text}&nbsp;
                <a>&#x270E;</a>
              </span>;
            },
            show: tableColumnSelectionHandler.showColumn('Compaction')
          },
          {
            Header: 'Size',
            accessor: 'size',
            filterable: false,
            width: 100,
            Cell: (row) => formatBytes(row.value),
            show: tableColumnSelectionHandler.showColumn('Size')
          },
          {
            Header: 'Num rows',
            accessor: 'num_rows',
            filterable: false,
            width: 100,
            Cell: (row) => formatNumber(row.value),
            show: !noSqlMode && tableColumnSelectionHandler.showColumn('Num rows')
          },
          {
            Header: 'Actions',
            accessor: 'datasource',
            id: 'actions',
            width: 70,
            filterable: false,
            Cell: row => {
              const datasource = row.value;
              const { unused } = row.original;
              const datasourceActions = this.getDatasourceActions(datasource, unused);
              const datasourceMenu = basicActionsToMenu(datasourceActions);

              return <ActionCell>
                {
                  datasourceMenu &&
                  <Popover content={datasourceMenu} position={Position.BOTTOM_RIGHT}>
                    <Icon icon={IconNames.WRENCH}/>
                  </Popover>
                }
              </ActionCell>;
            },
            show: tableColumnSelectionHandler.showColumn('Actions')
          }
        ]}
        defaultPageSize={50}
        className="-striped -highlight"
      />
      {this.renderMarkAsUnusedAllSegmentsInDataSourceAction()}
      {this.renderMarkAsUsedAllNonOvershadowedSegmentsInDataSourceAction()}
      {this.renderMarkAsUsedOrUnusedSegmentsInIntervalAction()}
      {this.renderKillAction()}
      {this.renderRetentionDialog()}
      {this.renderCompactionDialog()}
    </>;
  }

  render() {
    const { goToSql, noSqlMode } = this.props;
    const { showUnused } = this.state;
    const { tableColumnSelectionHandler } = this;

    return <div className="data-sources-view app-view">
      <ViewControlBar label="Datasources">
        <Button
          icon={IconNames.REFRESH}
          text="Refresh"
          onClick={() => this.datasourceQueryManager.rerunLastQuery()}
        />
        {
          !noSqlMode &&
          <Button
            icon={IconNames.APPLICATION}
            text="Go to SQL"
            onClick={() => goToSql(this.datasourceQueryManager.getLastQuery())}
          />
        }
        <Switch
          checked={showUnused}
          label="Show data sources without used segments"
          onChange={() => this.setState({ showUnused: !showUnused })}
        />
        <TableColumnSelection
          columns={noSqlMode ? tableColumnsNoSql : tableColumns}
          onChange={(column) => tableColumnSelectionHandler.changeTableColumnSelection(column)}
          tableColumnsHidden={tableColumnSelectionHandler.hiddenColumns}
        />
      </ViewControlBar>
      {this.renderDatasourceTable()}
    </div>;
  }
}
