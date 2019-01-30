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

import axios from 'axios';
import * as React from 'react';
import * as classNames from 'classnames';
import ReactTable from "react-table";
import { Filter } from "react-table";
import { Button, Intent, Switch } from "@blueprintjs/core";
import { IconNames } from "@blueprintjs/icons";
import { AppToaster } from '../singletons/toaster';
import { RuleEditor } from '../components/rule-editor';
import { AsyncActionDialog } from '../dialogs/async-action-dialog';
import {
  addFilter,
  formatNumber,
  formatBytes,
  countBy,
  lookupBy,
  QueryManager,
  pluralIfNeeded, queryDruidSql, getDruidErrorMessage
} from "../utils";
import { RetentionDialog } from '../dialogs/retention-dialog';

import "./datasource-view.scss";

export interface DatasourcesViewProps extends React.Props<any> {
  goToSql: (initSql: string) => void;
  goToSegments: (datasource: string, onlyUnavailable?: boolean) => void;
}

interface Datasource {
  datasource: string;
  rules: any[];
  [key: string]: any;
}

export interface DatasourcesViewState {
  datasourcesLoading: boolean;
  datasources: Datasource[] | null;
  tiers: string[];
  defaultRules: any[]
  datasourcesError: string | null;
  datasourcesFilter: Filter[];

  showDisabled: boolean;
  retentionDialogOpenOn: { datasource: string, rules: any[] } | null;
  dropDataDatasource: string | null;
  enableDatasource: string | null;
  killDatasource: string | null;
}

export class DatasourcesView extends React.Component<DatasourcesViewProps, DatasourcesViewState> {
  static DISABLED_COLOR = '#0a1500';
  static FULLY_AVAILABLE_COLOR = '#57d500';
  static PARTIALLY_AVAILABLE_COLOR = '#ffbf00';

  static formatRules(rules: any[]): string {
    if (rules.length === 0) {
      return 'No rules';
    } if (rules.length <= 2) {
      return rules.map(RuleEditor.ruleToString).join(', ');
    } else {
      return `${RuleEditor.ruleToString(rules[0])} +${rules.length - 1} more rules`;
    }
  }

  private datasourceQueryManager: QueryManager<string, { tiers: string[], defaultRules: any[], datasources: Datasource[] }>;

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
      dropDataDatasource: null,
      enableDatasource: null,
      killDatasource: null
    };
  }

  componentDidMount(): void {
    this.datasourceQueryManager = new QueryManager({
      processQuery: async (query: string) => {
        const datasources: any[] = await queryDruidSql({ query });
        const seen = countBy(datasources, (x: any) => x.datasource);

        const disabledResp = await axios.get('/druid/coordinator/v1/metadata/datasources?includeDisabled');
        const disabled: string[] = disabledResp.data.filter((d: string) => !seen[d]);

        const rulesResp = await axios.get('/druid/coordinator/v1/rules');
        const rules = rulesResp.data;

        const compactionResp = await axios.get('/druid/coordinator/v1/config/compaction');
        const compaction = lookupBy(compactionResp.data.compactionConfigs, (c: any) => c.dataSource);

        const tiersResp = await axios.get('/druid/coordinator/v1/tiers');
        const tiers = tiersResp.data;

        const allDatasources = datasources.concat(disabled.map(d => ({ datasource: d, disabled: true })));
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
          const resp = await axios.delete(`/druid/coordinator/v1/datasources/${dropDataDatasource}`, {})
          return resp.data;
        } : null
      }
      confirmButtonText="Drop data"
      successText="Data has been dropped"
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
          const resp = await axios.post(`/druid/coordinator/v1/datasources/${enableDatasource}`, {})
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

  private saveRules = async (datasource: string, rules: any[], author: string, comment: string) => {
    try {
      await axios.post(`/druid/coordinator/v1/rules/${datasource}`, rules, {
        headers: {
          "X-Druid-Author": author,
          "X-Druid-Comment": comment
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

  renderDatasourceTable() {
    const { goToSegments } = this.props;
    const { datasources, defaultRules, datasourcesLoading, datasourcesError, datasourcesFilter, showDisabled } = this.state;

    let data = datasources || [];
    if (!showDisabled) {
      data = data.filter(d => !d.disabled)
    }

    return <>
      <ReactTable
        data={data}
        loading={datasourcesLoading}
        noDataText={!datasourcesLoading && datasources && !datasources.length ? 'No datasources' : (datasourcesError || '')}
        filterable={true}
        filtered={datasourcesFilter}
        onFilteredChange={(filtered, column) => {
          this.setState({ datasourcesFilter: filtered });
        }}
        columns={[
          {
            Header: "Datasource",
            accessor: "datasource",
            width: 150,
            Cell: row => {
              const value = row.value;
              return <a onClick={() => { this.setState({ datasourcesFilter: addFilter(datasourcesFilter, 'datasource', value) }) }}>{value}</a>
            }
          },
          {
            Header: "Availability",
            id: "availability",
            filterable: false,
            accessor: (row) => row.num_available_segments / row.num_segments,
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
            }
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

              return <span>
                {text}&nbsp;
                <a onClick={() => this.setState({retentionDialogOpenOn: { datasource: row.original.datasource, rules: row.original.rules }})}>&#x270E;</a>
              </span>;
            }
          },
          {
            Header: 'Compaction',
            id: 'compaction',
            accessor: (row) => Boolean(row.compaction),
            filterable: false,
            Cell: row => {
              const { compaction } = row.original;
              let text: string;
              if (compaction) {
                text = `Target: ${formatBytes(compaction.targetCompactionSizeBytes)}`;
              } else {
                text = 'None';
              }
              return <span>{text} <a onClick={() => alert('ToDo')}>&#x270E;</a></span>;
            },
            show: false // This feature is not ready, it will be enabled later
          },
          {
            Header: 'Size',
            accessor: 'size',
            filterable: false,
            width: 100,
            Cell: (row) => formatBytes(row.value)
          },
          {
            Header: 'Num rows',
            accessor: 'num_rows',
            filterable: false,
            width: 100,
            Cell: (row) => formatNumber(row.value)
          },
          {
            Header: 'Actions',
            accessor: 'datasource',
            id: 'actions',
            width: 160,
            filterable: false,
            Cell: row => {
              const datasource = row.value;
              const { disabled } = row.original;
              if (disabled) {
                return <div>
                  <a onClick={() => this.setState({ enableDatasource: datasource })}>Enable</a>&nbsp;&nbsp;&nbsp;
                  <a onClick={() => this.setState({ killDatasource: datasource })}>Permanently delete</a>
                </div>
              } else {
                return <div>
                  <a onClick={() => this.setState({ dropDataDatasource: datasource })}>Drop data</a>
                </div>
              }
            }
          }
        ]}
        defaultPageSize={50}
        className="-striped -highlight"
      />
      {this.renderDropDataAction()}
      {this.renderEnableAction()}
      {this.renderKillAction()}
      {this.renderRetentionDialog()}
    </>;
  }

  render() {
    const { goToSql } = this.props;
    const { showDisabled } = this.state;

    return <div className="data-sources-view app-view">
      <div className="control-bar">
        <div className="control-label">Datasources</div>
        <Button
          icon={IconNames.REFRESH}
          text="Refresh"
          onClick={() => this.datasourceQueryManager.rerunLastQuery()}
        />
        <Button
          icon={IconNames.CONSOLE}
          text="Go to SQL"
          onClick={() => goToSql(this.datasourceQueryManager.getLastQuery())}
        />
        <Switch
          checked={showDisabled}
          label="Show disabled"
          onChange={() => this.setState({ showDisabled: !showDisabled })}
        />
      </div>
      {this.renderDatasourceTable()}
    </div>
  }
}

