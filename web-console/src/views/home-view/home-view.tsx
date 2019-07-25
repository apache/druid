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

import { Card, H5, Icon } from '@blueprintjs/core';
import { IconName, IconNames } from '@blueprintjs/icons';
import axios from 'axios';
import { sum } from 'd3-array';
import React from 'react';

import { UrlBaser } from '../../singletons/url-baser';
import { lookupBy, pluralIfNeeded, queryDruidSql, QueryManager } from '../../utils';
import { deepGet } from '../../utils/object-change';

import './home-view.scss';

export interface CardOptions {
  href: string;
  icon: IconName;
  title: string;
  loading?: boolean;
  content: JSX.Element | string;
  error?: string | null;
}

export interface HomeViewProps {
  noSqlMode: boolean;
}

export interface HomeViewState {
  versionLoading: boolean;
  version: string;
  versionError?: string;

  datasourceCountLoading: boolean;
  datasourceCount: number;
  datasourceCountError?: string;

  segmentCountLoading: boolean;
  segmentCount: number;
  unavailableSegmentCount: number;
  segmentCountError?: string;

  supervisorCountLoading: boolean;
  runningSupervisorCount: number;
  suspendedSupervisorCount: number;
  supervisorCountError?: string;

  lookupsCountLoading: boolean;
  lookupsCount: number;
  lookupsCountError: string | null;
  lookupsUninitialized: boolean;

  taskCountLoading: boolean;
  runningTaskCount: number;
  pendingTaskCount: number;
  successTaskCount: number;
  failedTaskCount: number;
  waitingTaskCount: number;
  taskCountError?: string;

  serverCountLoading: boolean;
  coordinatorCount: number;
  overlordCount: number;
  routerCount: number;
  brokerCount: number;
  historicalCount: number;
  middleManagerCount: number;
  peonCount: number;
  serverCountError?: string;
}

export class HomeView extends React.PureComponent<HomeViewProps, HomeViewState> {
  private versionQueryManager: QueryManager<null, string>;
  private datasourceQueryManager: QueryManager<boolean, any>;
  private segmentQueryManager: QueryManager<boolean, any>;
  private supervisorQueryManager: QueryManager<null, any>;
  private taskQueryManager: QueryManager<boolean, any>;
  private serverQueryManager: QueryManager<boolean, any>;
  private lookupsQueryManager: QueryManager<null, any>;

  constructor(props: HomeViewProps, context: any) {
    super(props, context);
    this.state = {
      versionLoading: true,
      version: '',

      datasourceCountLoading: false,
      datasourceCount: 0,

      segmentCountLoading: false,
      segmentCount: 0,
      unavailableSegmentCount: 0,

      supervisorCountLoading: false,
      runningSupervisorCount: 0,
      suspendedSupervisorCount: 0,

      lookupsCountLoading: false,
      lookupsCount: 0,
      lookupsCountError: null,
      lookupsUninitialized: false,

      taskCountLoading: false,
      runningTaskCount: 0,
      pendingTaskCount: 0,
      successTaskCount: 0,
      failedTaskCount: 0,
      waitingTaskCount: 0,

      serverCountLoading: false,
      coordinatorCount: 0,
      overlordCount: 0,
      routerCount: 0,
      brokerCount: 0,
      historicalCount: 0,
      middleManagerCount: 0,
      peonCount: 0,
    };

    this.versionQueryManager = new QueryManager({
      processQuery: async () => {
        const statusResp = await axios.get('/status');
        return statusResp.data.version;
      },
      onStateChange: ({ result, loading, error }) => {
        this.setState({
          versionLoading: loading,
          version: result,
          versionError: error,
        });
      },
    });

    this.datasourceQueryManager = new QueryManager({
      processQuery: async noSqlMode => {
        let datasources: string[];
        if (!noSqlMode) {
          datasources = await queryDruidSql({
            query: `SELECT datasource FROM sys.segments GROUP BY 1`,
          });
        } else {
          const datasourcesResp = await axios.get('/druid/coordinator/v1/datasources');
          datasources = datasourcesResp.data;
        }
        return datasources.length;
      },
      onStateChange: ({ result, loading, error }) => {
        this.setState({
          datasourceCountLoading: loading,
          datasourceCount: result,
          datasourceCountError: error || undefined,
        });
      },
    });

    this.segmentQueryManager = new QueryManager({
      processQuery: async noSqlMode => {
        if (noSqlMode) {
          const loadstatusResp = await axios.get('/druid/coordinator/v1/loadstatus?simple');
          const loadstatus = loadstatusResp.data;
          const unavailableSegmentNum = sum(Object.keys(loadstatus), key => loadstatus[key]);

          const datasourcesMetaResp = await axios.get('/druid/coordinator/v1/datasources?simple');
          const datasourcesMeta = datasourcesMetaResp.data;
          const availableSegmentNum = sum(datasourcesMeta, (curr: any) =>
            deepGet(curr, 'properties.segments.count'),
          );

          return {
            count: availableSegmentNum + unavailableSegmentNum,
            unavailable: unavailableSegmentNum,
          };
        } else {
          const segments = await queryDruidSql({
            query: `SELECT
  COUNT(*) as "count",
  COUNT(*) FILTER (WHERE is_available = 0) as "unavailable"
FROM sys.segments`,
          });
          return segments.length === 1 ? segments[0] : null;
        }
      },
      onStateChange: ({ result, loading, error }) => {
        this.setState({
          segmentCountLoading: loading,
          segmentCount: result ? result.count : 0,
          unavailableSegmentCount: result ? result.unavailable : 0,
          segmentCountError: error,
        });
      },
    });

    this.supervisorQueryManager = new QueryManager({
      processQuery: async () => {
        const resp = await axios.get('/druid/indexer/v1/supervisor?full');
        const data = resp.data;
        const runningSupervisorCount = data.filter((d: any) => d.spec.suspended === false).length;
        const suspendedSupervisorCount = data.filter((d: any) => d.spec.suspended === true).length;
        return {
          runningSupervisorCount,
          suspendedSupervisorCount,
        };
      },
      onStateChange: ({ result, loading, error }) => {
        this.setState({
          runningSupervisorCount: result ? result.runningSupervisorCount : 0,
          suspendedSupervisorCount: result ? result.suspendedSupervisorCount : 0,
          supervisorCountLoading: loading,
          supervisorCountError: error,
        });
      },
    });

    this.taskQueryManager = new QueryManager({
      processQuery: async noSqlMode => {
        if (noSqlMode) {
          const completeTasksResp = await axios.get('/druid/indexer/v1/completeTasks');
          const runningTasksResp = await axios.get('/druid/indexer/v1/runningTasks');
          const pendingTasksResp = await axios.get('/druid/indexer/v1/pendingTasks');
          const waitingTasksResp = await axios.get('/druid/indexer/v1/waitingTasks');
          return {
            SUCCESS: completeTasksResp.data.filter((d: any) => d.status === 'SUCCESS').length,
            FAILED: completeTasksResp.data.filter((d: any) => d.status === 'FAILED').length,
            RUNNING: runningTasksResp.data.length,
            PENDING: pendingTasksResp.data.length,
            WAITING: waitingTasksResp.data.length,
          };
        } else {
          const taskCountsFromQuery: { status: string; count: number }[] = await queryDruidSql({
            query: `SELECT
  CASE WHEN "status" = 'RUNNING' THEN "runner_status" ELSE "status" END AS "status",
  COUNT (*) AS "count"
FROM sys.tasks
GROUP BY 1`,
          });
          return lookupBy(taskCountsFromQuery, x => x.status, x => x.count);
        }
      },
      onStateChange: ({ result, loading, error }) => {
        this.setState({
          taskCountLoading: loading,
          successTaskCount: result ? result.SUCCESS : 0,
          failedTaskCount: result ? result.FAILED : 0,
          runningTaskCount: result ? result.RUNNING : 0,
          pendingTaskCount: result ? result.PENDING : 0,
          waitingTaskCount: result ? result.WAITING : 0,
          taskCountError: error,
        });
      },
    });

    this.serverQueryManager = new QueryManager({
      processQuery: async noSqlMode => {
        if (noSqlMode) {
          const serversResp = await axios.get('/druid/coordinator/v1/servers?simple');
          const middleManagerResp = await axios.get('/druid/indexer/v1/workers');
          return {
            historical: serversResp.data.filter((s: any) => s.type === 'historical').length,
            middle_manager: middleManagerResp.data.length,
            peon: serversResp.data.filter((s: any) => s.type === 'indexer-executor').length,
          };
        } else {
          const serverCountsFromQuery: {
            server_type: string;
            count: number;
          }[] = await queryDruidSql({
            query: `SELECT server_type, COUNT(*) as "count" FROM sys.servers GROUP BY 1`,
          });
          return lookupBy(serverCountsFromQuery, x => x.server_type, x => x.count);
        }
      },
      onStateChange: ({ result, loading, error }) => {
        this.setState({
          serverCountLoading: loading,
          coordinatorCount: result ? result.coordinator : 0,
          overlordCount: result ? result.overlord : 0,
          routerCount: result ? result.router : 0,
          brokerCount: result ? result.broker : 0,
          historicalCount: result ? result.historical : 0,
          middleManagerCount: result ? result.middle_manager : 0,
          peonCount: result ? result.peon : 0,
          serverCountError: error,
        });
      },
    });

    this.lookupsQueryManager = new QueryManager({
      processQuery: async () => {
        const resp = await axios.get('/druid/coordinator/v1/lookups/status');
        const data = resp.data;
        const lookupsCount = Object.keys(data.__default).length;
        return {
          lookupsCount,
        };
      },
      onStateChange: ({ result, loading, error }) => {
        this.setState({
          lookupsCount: result ? result.lookupsCount : 0,
          lookupsCountLoading: loading,
          lookupsCountError: error,
          lookupsUninitialized: error === 'Request failed with status code 404',
        });
      },
    });
  }

  componentDidMount(): void {
    const { noSqlMode } = this.props;

    this.versionQueryManager.runQuery(null);
    this.datasourceQueryManager.runQuery(noSqlMode);
    this.segmentQueryManager.runQuery(noSqlMode);
    this.supervisorQueryManager.runQuery(null);
    this.taskQueryManager.runQuery(noSqlMode);
    this.serverQueryManager.runQuery(noSqlMode);
    this.lookupsQueryManager.runQuery(null);
  }

  componentWillUnmount(): void {
    this.versionQueryManager.terminate();
    this.datasourceQueryManager.terminate();
    this.segmentQueryManager.terminate();
    this.supervisorQueryManager.terminate();
    this.taskQueryManager.terminate();
    this.serverQueryManager.terminate();
  }

  renderCard(cardOptions: CardOptions): JSX.Element {
    return (
      <a href={cardOptions.href} target={cardOptions.href[0] === '/' ? '_blank' : undefined}>
        <Card className="home-view-card" interactive>
          <H5>
            <Icon color="#bfccd5" icon={cardOptions.icon} />
            &nbsp;{cardOptions.title}
          </H5>
          {cardOptions.loading ? (
            <p>Loading...</p>
          ) : cardOptions.error ? (
            `Error: ${cardOptions.error}`
          ) : (
            cardOptions.content
          )}
        </Card>
      </a>
    );
  }

  render(): JSX.Element {
    const state = this.state;

    return (
      <div className="home-view app-view">
        {this.renderCard({
          href: UrlBaser.base('/status'),
          icon: IconNames.GRAPH,
          title: 'Status',
          loading: state.versionLoading,
          content: state.version ? `Apache Druid is running version ${state.version}` : '',
          error: state.versionError,
        })}
        {this.renderCard({
          href: '#datasources',
          icon: IconNames.MULTI_SELECT,
          title: 'Datasources',
          loading: state.datasourceCountLoading,
          content: pluralIfNeeded(state.datasourceCount, 'datasource'),
          error: state.datasourceCountError,
        })}
        {this.renderCard({
          href: '#segments',
          icon: IconNames.STACKED_CHART,
          title: 'Segments',
          loading: state.segmentCountLoading,
          content: (
            <>
              <p>{pluralIfNeeded(state.segmentCount, 'segment')}</p>
              {Boolean(state.unavailableSegmentCount) && (
                <p>{pluralIfNeeded(state.unavailableSegmentCount, 'unavailable segment')}</p>
              )}
            </>
          ),
          error: state.datasourceCountError,
        })}
        {this.renderCard({
          href: '#tasks',
          icon: IconNames.LIST_COLUMNS,
          title: 'Supervisors',
          loading: state.supervisorCountLoading,
          content: (
            <>
              {!Boolean(state.runningSupervisorCount + state.suspendedSupervisorCount) && (
                <p>0 supervisors</p>
              )}
              {Boolean(state.runningSupervisorCount) && (
                <p>{pluralIfNeeded(state.runningSupervisorCount, 'running supervisor')}</p>
              )}
              {Boolean(state.suspendedSupervisorCount) && (
                <p>{pluralIfNeeded(state.suspendedSupervisorCount, 'suspended supervisor')}</p>
              )}
            </>
          ),
          error: state.supervisorCountError,
        })}
        {this.renderCard({
          href: '#tasks',
          icon: IconNames.GANTT_CHART,
          title: 'Tasks',
          loading: state.taskCountLoading,
          content: (
            <>
              {Boolean(state.runningTaskCount) && (
                <p>{pluralIfNeeded(state.runningTaskCount, 'running task')}</p>
              )}
              {Boolean(state.pendingTaskCount) && (
                <p>{pluralIfNeeded(state.pendingTaskCount, 'pending task')}</p>
              )}
              {Boolean(state.successTaskCount) && (
                <p>{pluralIfNeeded(state.successTaskCount, 'successful task')}</p>
              )}
              {Boolean(state.waitingTaskCount) && (
                <p>{pluralIfNeeded(state.waitingTaskCount, 'waiting task')}</p>
              )}
              {Boolean(state.failedTaskCount) && (
                <p>{pluralIfNeeded(state.failedTaskCount, 'failed task')}</p>
              )}
              {!(
                Boolean(state.runningTaskCount) ||
                Boolean(state.pendingTaskCount) ||
                Boolean(state.successTaskCount) ||
                Boolean(state.waitingTaskCount) ||
                Boolean(state.failedTaskCount)
              ) && <p>There are no tasks</p>}
            </>
          ),
          error: state.taskCountError,
        })}
        {this.renderCard({
          href: '#servers',
          icon: IconNames.DATABASE,
          title: 'Servers',
          loading: state.serverCountLoading,
          content: (
            <>
              <p>{`${pluralIfNeeded(state.overlordCount, 'overlord')}, ${pluralIfNeeded(
                state.coordinatorCount,
                'coordinator',
              )}`}</p>
              <p>{`${pluralIfNeeded(state.routerCount, 'router')}, ${pluralIfNeeded(
                state.brokerCount,
                'broker',
              )}`}</p>
              <p>{`${pluralIfNeeded(state.historicalCount, 'historical')}, ${pluralIfNeeded(
                state.middleManagerCount,
                'middle manager',
              )}`}</p>
              {Boolean(state.peonCount) && <p>{pluralIfNeeded(state.peonCount, 'peon')}</p>}
            </>
          ),
          error: state.serverCountError,
        })}
        {this.renderCard({
          href: '#lookups',
          icon: IconNames.PROPERTIES,
          title: 'Lookups',
          loading: state.lookupsCountLoading,
          content: (
            <>
              <p>
                {!state.lookupsUninitialized
                  ? pluralIfNeeded(state.lookupsCount, 'lookup')
                  : 'Lookups uninitialized'}
              </p>
            </>
          ),
          error: !state.lookupsUninitialized ? state.lookupsCountError : null,
        })}
      </div>
    );
  }
}
