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

import { Button, FormGroup, Icon, InputGroup, Intent, Popover, Position, Switch } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import axios from 'axios';
import * as React from 'react';
import ReactTable, { Filter } from 'react-table';

import { ActionCell, RuleEditor, TableColumnSelector, ViewControlBar } from '../../components';
import { ActionIcon } from '../../components/action-icon/action-icon';
import { AsyncActionDialog, CompactionDialog, RetentionDialog } from '../../dialogs';
import { AppToaster } from '../../singletons/toaster';
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
} from '../../utils';
import { BasicAction } from '../../utils/basic-action';

import './datasource-view.scss';

const tableColumns: string[] = ['Datasource', 'Availability', 'Retention', 'Compaction', 'Size', 'Num rows', ActionCell.COLUMN_LABEL];
const tableColumnsNoSql: string[] = ['Datasource', 'Availability', 'Retention', 'Compaction', 'Size', ActionCell.COLUMN_LABEL];

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

  showDisabled: boolean;
  retentionDialogOpenOn: { datasource: string, rules: any[] } | null;
  compactionDialogOpenOn: {datasource: string, configData: any} | null;
  dropDataDatasource: string | null;
  enableDatasource: string | null;
  killDatasource: string | null;
  dropReloadDatasource: string | null;
  dropReloadAction: 'drop' | 'reload';
  dropReloadInterval: string;
}

export class DatasourcesView extends React.Component<DatasourcesViewProps, DatasourcesViewState> {
  static DISABLED_COLOR = '#0a1500';
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

      showDisabled: false,
      retentionDialogOpenOn: null,
      compactionDialogOpenOn: null,
      dropDataDatasource: null,
      enableDatasource: null,
      killDatasource: null,
      dropReloadDatasource: null,
      dropReloadAction: 'drop',
      dropReloadInterval: ''
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

        let disabled: string [] = [];
        if (this.state.showDisabled) {
          const disabledResp = await axios.get('/druid/coordinator/v1/metadata/datasources?includeDisabled' );
          disabled = disabledResp.data.filter((d: string) => !seen[d]);
        }

        const rulesResp = await axios.get('/druid/coordinator/v1/rules');
        const rules = rulesResp.data;

        const compactionResp = await axios.get('/druid/coordinator/v1/config/compaction');
        const compaction = lookupBy(compactionResp.data.compactionConfigs, (c: any) => c.dataSource);

        const tiersResp = await axios.get('/druid/coordinator/v1/tiers');
        const tiers = tiersResp.data;

        const allDatasources = (datasources as any).concat(disabled.map(d => ({ datasource: d, disabled: true })));
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

  renderDropDataAction() {
    const { dropDataDatasource } = this.state;

    return <AsyncActionDialog
      action={
        dropDataDatasource ? async () => {
          const resp = await axios.delete(`/druid/coordinator/v1/datasources/${dropDataDatasource}`, {});
          return resp.data;
        } : null
      }
      confirmButtonText="Drop data"
      successText="Data drop request acknowledged, next time the coordinator runs data will be dropped"
      failText="Could not drop data"
      intent={Intent.DANGER}
      onClose={(success) => {
        this.setState({ dropDataDatasource: null });
        if (success) this.datasourceQueryManager.rerunLastQuery();
      }}
    >
      <p>
        {`Are you sure you want to drop all the data for datasource '${dropDataDatasource}'?`}
      </p>
    </AsyncActionDialog>;
  }

  renderEnableAction() {
    const { enableDatasource } = this.state;

    return <AsyncActionDialog
      action={
        enableDatasource ? async () => {
          const resp = await axios.post(`/druid/coordinator/v1/datasources/${enableDatasource}`, {});
          return resp.data;
        } : null
      }
      confirmButtonText="Enable datasource"
      successText="Datasource has been enabled"
      failText="Could not enable datasource"
      intent={Intent.PRIMARY}
      onClose={(success) => {
        this.setState({ enableDatasource: null });
        if (success) this.datasourceQueryManager.rerunLastQuery();
      }}
    >
      <p>
        {`Are you sure you want to enable datasource '${enableDatasource}'?`}
      </p>
    </AsyncActionDialog>;
  }

  renderDropReloadAction() {
    const { dropReloadDatasource, dropReloadAction, dropReloadInterval } = this.state;
    const isDrop = dropReloadAction === 'drop';

    return <AsyncActionDialog
      action={
        dropReloadDatasource ? async () => {
          if (!dropReloadInterval) return;
          const resp = await axios.post(`/druid/coordinator/v1/datasources/${dropReloadDatasource}/${isDrop ? 'markUnused' : 'markUsed'}`, {
            interval: dropReloadInterval
          });
          return resp.data;
        } : null
      }
      confirmButtonText={`${isDrop ? 'Drop' : 'Reload'} selected data`}
      confirmButtonDisabled={!/.\/./.test(dropReloadInterval)}
      successText={`${isDrop ? 'Drop' : 'Reload'} request submitted`}
      failText={`Could not ${isDrop ? 'drop' : 'reload'} data`}
      intent={Intent.PRIMARY}
      onClose={(success) => {
        this.setState({ dropReloadDatasource: null });
        if (success) this.datasourceQueryManager.rerunLastQuery();
      }}
    >
      <p>
        {`Please select the interval that you want to ${isDrop ? 'drop' : 'reload'}?`}
      </p>
      <FormGroup>
        <InputGroup
          value={dropReloadInterval}
          onChange={(e: any) => {
            const v = e.target.value;
            this.setState({ dropReloadInterval: v.toUpperCase() });
          }}
          placeholder="2018-01-01T00:00:00/2018-01-03T00:00:00"
        />
      </FormGroup>
    </AsyncActionDialog>;
  }

  renderKillAction() {
    const { killDatasource } = this.state;

    return <AsyncActionDialog
      action={
        killDatasource ? async () => {
          const resp = await axios.delete(`/druid/coordinator/v1/datasources/${killDatasource}?kill=true&interval=1000/3000`, {});
          return resp.data;
        } : null
      }
      confirmButtonText="Permanently delete data"
      successText="Kill task was issued. Datasource will be deleted"
      failText="Could not submit kill task"
      intent={Intent.DANGER}
      onClose={(success) => {
        this.setState({ killDatasource: null });
        if (success) this.datasourceQueryManager.rerunLastQuery();
      }}
    >
      <p>
        {`Are you sure you want to permanently delete the data in datasource '${killDatasource}'?`}
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

  private  toggleDisabled(showDisabled: boolean) {
    if (!showDisabled) {
      this.datasourceQueryManager.rerunLastQuery();
    }
    this.setState({showDisabled: !showDisabled});
  }

  getDatasourceActions(datasource: string, disabled: boolean): BasicAction[] {
    const { goToSql } = this.props;

    if (disabled) {
      return [
        {
          icon: IconNames.EXPORT,
          title: 'Enable',
          onAction: () => this.setState({ enableDatasource: datasource })
        },
        {
          icon: IconNames.TRASH,
          title: 'Permanently delete (kill task)',
          intent: Intent.DANGER,
          onAction: () => this.setState({ killDatasource: datasource })
        }
      ];
    } else {
      return [
        {
          icon: IconNames.APPLICATION,
          title: 'Query with SQL',
          onAction: () => goToSql(`SELECT * FROM "${datasource}"`)
        },
        {
          icon: IconNames.EXPORT,
          title: 'Reload data by interval',
          onAction: () => this.setState({ dropReloadDatasource: datasource, dropReloadAction: 'reload' })
        },
        {
          icon: IconNames.IMPORT,
          title: 'Drop data by interval',
          onAction: () => this.setState({ dropReloadDatasource: datasource, dropReloadAction: 'drop' })
        },
        {
          icon: IconNames.IMPORT,
          title: 'Drop datasource (disable)',
          intent: Intent.DANGER,
          onAction: () => this.setState({ dropDataDatasource: datasource })
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
    const { datasources, defaultRules, datasourcesLoading, datasourcesError, datasourcesFilter, showDisabled } = this.state;
    const { tableColumnSelectionHandler } = this;
    let data = datasources || [];
    if (!showDisabled) {
      data = data.filter(d => !d.disabled);
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
              const { datasource, num_available_segments, num_segments, disabled } = row.original;

              if (disabled) {
                return <span>
                  <span style={{ color: DatasourcesView.DISABLED_COLOR }}>&#x25cf;&nbsp;</span>
                  Disabled
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
                <ActionIcon icon={IconNames.EDIT}/>
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
                <ActionIcon icon={IconNames.EDIT}/>
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
            Header: ActionCell.COLUMN_LABEL,
            accessor: 'datasource',
            id: ActionCell.COLUMN_ID,
            width: ActionCell.COLUMN_WIDTH,
            filterable: false,
            Cell: row => {
              const datasource = row.value;
              const { disabled } = row.original;
              const datasourceActions = this.getDatasourceActions(datasource, disabled);
              return <ActionCell actions={datasourceActions}/>;
            },
            show: tableColumnSelectionHandler.showColumn(ActionCell.COLUMN_LABEL)
          }
        ]}
        defaultPageSize={50}
      />
      {this.renderDropDataAction()}
      {this.renderEnableAction()}
      {this.renderDropReloadAction()}
      {this.renderKillAction()}
      {this.renderRetentionDialog()}
      {this.renderCompactionDialog()}
    </>;
  }

  render() {
    const { goToSql, noSqlMode } = this.props;
    const { showDisabled } = this.state;
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
          checked={showDisabled}
          label="Show disabled"
          onChange={() => this.toggleDisabled(showDisabled)}
        />
        <TableColumnSelector
          columns={noSqlMode ? tableColumnsNoSql : tableColumns}
          onChange={(column) => tableColumnSelectionHandler.changeTableColumnSelector(column)}
          tableColumnsHidden={tableColumnSelectionHandler.hiddenColumns}
        />
      </ViewControlBar>
      {this.renderDatasourceTable()}
    </div>;
  }
}
