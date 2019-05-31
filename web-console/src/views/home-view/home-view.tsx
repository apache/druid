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
import * as React from 'react';

import { UrlBaser } from '../../singletons/url-baser';
import { getHeadProp, lookupBy, pluralIfNeeded, queryDruidSql, QueryManager } from '../../utils';

import './home-view.scss';

export interface CardOptions {
  href: string;
  icon: IconName;
  title: string;
  loading?: boolean;
  content: JSX.Element | string;
  error?: string | null;
}

export interface HomeViewProps extends React.Props<any> {
  noSqlMode: boolean;
}

export interface HomeViewState {
  statusLoading: boolean;
  status: any;
  statusError: string | null;

  datasourceCountLoading: boolean;
  datasourceCount: number;
  datasourceCountError: string | null;

  segmentCountLoading: boolean;
  segmentCount: number;
  segmentCountError: string | null;

  supervisorCountLoading: boolean;
  runningSupervisorCount: number;
  suspendedSupervisorCount: number;
  supervisorCountError: string | null;

  taskCountLoading: boolean;
  runningTaskCount: number;
  pendingTaskCount: number;
  successTaskCount: number;
  failedTaskCount: number;
  waitingTaskCount: number;
  taskCountError: string | null;

  serverCountLoading: boolean;
  coordinatorCount: number;
  overlordCount: number;
  routerCount: number;
  brokerCount: number;
  historicalCount: number;
  middleManagerCount: number;
  peonCount: number;
  serverCountError: string | null;
}

export class HomeView extends React.Component<HomeViewProps, HomeViewState> {
  private statusQueryManager: QueryManager<string, any>;
  private datasourceQueryManager: QueryManager<string, any>;
  private segmentQueryManager: QueryManager<string, any>;
  private supervisorQueryManager: QueryManager<string, any>;
  private taskQueryManager: QueryManager<string, any>;
  private serverQueryManager: QueryManager<string, any>;

  constructor(props: HomeViewProps, context: any) {
    super(props, context);
    this.state = {
      statusLoading: true,
      status: null,
      statusError: null,

      datasourceCountLoading: false,
      datasourceCount: 0,
      datasourceCountError: null,

      segmentCountLoading: false,
      segmentCount: 0,
      segmentCountError: null,

      supervisorCountLoading: false,
      runningSupervisorCount: 0,
      suspendedSupervisorCount: 0,
      supervisorCountError: null,

      taskCountLoading: false,
      runningTaskCount: 0,
      pendingTaskCount: 0,
      successTaskCount: 0,
      failedTaskCount: 0,
      waitingTaskCount: 0,
      taskCountError: null,

      serverCountLoading: false,
      coordinatorCount: 0,
      overlordCount: 0,
      routerCount: 0,
      brokerCount: 0,
      historicalCount: 0,
      middleManagerCount: 0,
      peonCount: 0,
      serverCountError: null
    };
  }

  componentDidMount(): void {
    const { noSqlMode } = this.props;

    this.statusQueryManager = new QueryManager({
      processQuery: async (query) => {
        const statusResp = await axios.get('/status');
        return statusResp.data;
      },
      onStateChange: ({ result, loading, error }) => {
        this.setState({
          statusLoading: loading,
          status: result,
          statusError: error
        });
      }
    });

    this.statusQueryManager.runQuery(`dummy`);

    // -------------------------

    this.datasourceQueryManager = new QueryManager({
      processQuery: async (query) => {
        let datasources: string[];
        if (!noSqlMode) {
          datasources = await queryDruidSql({ query });
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
          datasourceCountError: error
        });
      }
    });

    this.datasourceQueryManager.runQuery(`SELECT datasource FROM sys.segments GROUP BY 1`);

    // -------------------------

    this.segmentQueryManager = new QueryManager({
      processQuery: async (query) => {
        if (noSqlMode) {
          const loadstatusResp = await axios.get('/druid/coordinator/v1/loadstatus?simple');
          const loadstatus = loadstatusResp.data;
          const unavailableSegmentNum = Object.keys(loadstatus).reduce((sum, key) => {
            return sum + loadstatus[key];
          }, 0);

          const datasourcesMetaResp = await axios.get('/druid/coordinator/v1/datasources?simple');
          const datasourcesMeta = datasourcesMetaResp.data;
          const availableSegmentNum = datasourcesMeta.reduce((sum: number, curr: any) => {
            return sum + curr.properties.segments.count;
          }, 0);

          return availableSegmentNum + unavailableSegmentNum;

        } else {
          const segments = await queryDruidSql({ query });
          return getHeadProp(segments, 'count') || 0;

        }
      },
      onStateChange: ({ result, loading, error }) => {
        this.setState({
          segmentCountLoading: loading,
          segmentCount: result,
          segmentCountError: error
        });
      }
    });

    this.segmentQueryManager.runQuery(`SELECT COUNT(*) as "count" FROM sys.segments`);

    // -------------------------
    this.supervisorQueryManager = new QueryManager({
      processQuery: async (query: string) => {
        const resp = await axios.get('/druid/indexer/v1/supervisor?full');
        const data = resp.data;
        const runningSupervisorCount = data.filter((d: any) => d.spec.suspended === false).length;
        const suspendedSupervisorCount = data.filter((d: any) => d.spec.suspended === true).length;
        return {
          runningSupervisorCount,
          suspendedSupervisorCount
        };
      },
      onStateChange: ({result, loading, error}) => {
        this.setState({
          runningSupervisorCount: result ? result.runningSupervisorCount : 0,
          suspendedSupervisorCount: result ? result.suspendedSupervisorCount : 0,
          supervisorCountLoading: loading,
          supervisorCountError: error
        });
      }
    });

    this.supervisorQueryManager.runQuery('dummy');

    // -------------------------

    this.taskQueryManager = new QueryManager({
      processQuery: async (query) => {
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
            WAITING: waitingTasksResp.data.length
          };

        } else {
          const taskCountsFromQuery: { status: string, count: number }[] = await queryDruidSql({ query });
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
          taskCountError: error
        });
      }
    });

    this.taskQueryManager.runQuery(`SELECT
  CASE WHEN "status" = 'RUNNING' THEN "runner_status" ELSE "status" END AS "status",
  COUNT (*) AS "count"
FROM sys.tasks
GROUP BY 1`);

    // -------------------------

    this.serverQueryManager = new QueryManager({
      processQuery: async (query) => {
        if (noSqlMode) {
          const serversResp = await axios.get('/druid/coordinator/v1/servers?simple');
          const middleManagerResp = await axios.get('/druid/indexer/v1/workers');
          return {
            historical: serversResp.data.filter((s: any) => s.type === 'historical').length,
            middle_manager: middleManagerResp.data.length,
            peon: serversResp.data.filter((s: any) => s.type === 'indexer-executor').length
          };

        } else {
          const serverCountsFromQuery: { server_type: string, count: number }[] = await queryDruidSql({ query });
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
          serverCountError: error
        });
      }
    });

    this.serverQueryManager.runQuery(`SELECT server_type, COUNT(*) as "count" FROM sys.servers GROUP BY 1`);
  }

  componentWillUnmount(): void {
    this.statusQueryManager.terminate();
    this.datasourceQueryManager.terminate();
    this.segmentQueryManager.terminate();
    this.supervisorQueryManager.terminate();
    this.taskQueryManager.terminate();
    this.serverQueryManager.terminate();
  }

  renderCard(cardOptions: CardOptions): JSX.Element {
    return <a href={cardOptions.href} target={cardOptions.href[0] === '/' ? '_blank' : undefined}>
      <Card className="status-card" interactive>
        <H5><Icon color="#bfccd5" icon={cardOptions.icon}/>&nbsp;{cardOptions.title}</H5>
        {cardOptions.loading ? <p>Loading...</p> : (cardOptions.error ? `Error: ${cardOptions.error}` : cardOptions.content)}
      </Card>
    </a>;
  }

  render() {
    const state = this.state;

    return <div className="home-view app-view">
      {
        this.renderCard({
          href: UrlBaser.base('/status'),
          icon: IconNames.GRAPH,
          title: 'Status',
          loading: state.statusLoading,
          content: state.status ? `Apache Druid is running version ${state.status.version}` : '',
          error: state.statusError
        })
      }
      {
        this.renderCard({
          href: '#datasources',
          icon: IconNames.MULTI_SELECT,
          title: 'Datasources',
          loading: state.datasourceCountLoading,
          content: pluralIfNeeded(state.datasourceCount, 'datasource'),
          error: state.datasourceCountError
        })
      }
      {
        this.renderCard({
          href: '#segments',
          icon: IconNames.STACKED_CHART,
          title: 'Segments',
          loading: state.segmentCountLoading,
          content: pluralIfNeeded(state.segmentCount, 'segment'),
          error: state.datasourceCountError
        })
      }
      {
        this.renderCard({
          href: '#tasks',
          icon: IconNames.LIST_COLUMNS,
          title: 'Supervisors',
          loading: state.supervisorCountLoading,
          content: <>
            {!Boolean(state.runningSupervisorCount + state.suspendedSupervisorCount) && <p>0 supervisors</p>}
            {Boolean(state.runningSupervisorCount) && <p>{pluralIfNeeded(state.runningSupervisorCount, 'running supervisor')}</p>}
            {Boolean(state.suspendedSupervisorCount) && <p>{pluralIfNeeded(state.suspendedSupervisorCount, 'suspended supervisor')}</p>}
          </>,
          error: state.supervisorCountError
        })
      }
      {
        this.renderCard({
          href: '#tasks',
          icon: IconNames.GANTT_CHART,
          title: 'Tasks',
          loading: state.taskCountLoading,
          content: <>
            {Boolean(state.runningTaskCount) && <p>{pluralIfNeeded(state.runningTaskCount, 'running task')}</p>}
            {Boolean(state.pendingTaskCount) && <p>{pluralIfNeeded(state.pendingTaskCount, 'pending task')}</p>}
            {Boolean(state.successTaskCount) && <p>{pluralIfNeeded(state.successTaskCount, 'successful task')}</p>}
            {Boolean(state.waitingTaskCount) && <p>{pluralIfNeeded(state.waitingTaskCount, 'waiting task')}</p>}
            {Boolean(state.failedTaskCount) && <p>{pluralIfNeeded(state.failedTaskCount, 'failed task')}</p>}
            {
              !(
                Boolean(state.runningTaskCount) ||
                Boolean(state.pendingTaskCount) ||
                Boolean(state.successTaskCount) ||
                Boolean(state.waitingTaskCount) ||
                Boolean(state.failedTaskCount)
              ) &&
              <p>There are no tasks</p>
            }
          </>,
          error: state.taskCountError
        })
      }
      {
        this.renderCard({
          href: '#servers',
          icon: IconNames.DATABASE,
          title: 'Servers',
          loading: state.serverCountLoading,
          content: <>
            <p>{`${pluralIfNeeded(state.overlordCount, 'overlord')}, ${pluralIfNeeded(state.coordinatorCount, 'coordinator')}`}</p>
            <p>{`${pluralIfNeeded(state.routerCount, 'router')}, ${pluralIfNeeded(state.brokerCount, 'broker')}`}</p>
            <p>{`${pluralIfNeeded(state.historicalCount, 'historical')}, ${pluralIfNeeded(state.middleManagerCount, 'middle manager')}`}</p>
            {
              Boolean(state.peonCount) &&
              <p>{pluralIfNeeded(state.peonCount, 'peon')}</p>
            }
          </>,
          error: state.serverCountError
        })
      }
    </div>;
  }
}
