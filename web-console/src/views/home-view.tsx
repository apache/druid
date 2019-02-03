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
import { H5, Card, Icon } from "@blueprintjs/core";
import { IconName, IconNames } from "@blueprintjs/icons";
import { QueryManager, pluralIfNeeded, queryDruidSql } from '../utils';
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

  taskCountLoading: boolean;
  runningTaskCount: number;
  pendingTaskCount: number;
  successTaskCount: number;
  failedTaskCount: number;
  waitingTaskCount: number;
  taskCountError: string | null;

  dataServerCountLoading: boolean;
  dataServerCount: number;
  dataServerCountError: string | null;

  middleManagerCountLoading: boolean;
  middleManagerCount: number;
  middleManagerCountError: string | null;
}

export class HomeView extends React.Component<HomeViewProps, HomeViewState> {
  private statusQueryManager: QueryManager<string, any>;
  private datasourceQueryManager: QueryManager<string, any>;
  private segmentQueryManager: QueryManager<string, any>;
  private taskQueryManager: QueryManager<string, any>;
  private dataServerQueryManager: QueryManager<string, any>;
  private middleManagerQueryManager: QueryManager<string, any>;

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

      taskCountLoading: false,
      runningTaskCount: 0,
      pendingTaskCount: 0,
      successTaskCount: 0,
      failedTaskCount: 0,
      waitingTaskCount: 0,
      taskCountError: null,

      dataServerCountLoading: false,
      dataServerCount: 0,
      dataServerCountError: null,

      middleManagerCountLoading: false,
      middleManagerCount: 0,
      middleManagerCountError: null
    };
  }

  componentDidMount(): void {
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
        const datasources = await queryDruidSql({ query });
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
        const segments = await queryDruidSql({ query });
        return segments[0].count;
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

    this.taskQueryManager = new QueryManager({
      processQuery: async (query) => {
        const taskCountsFromSql = await queryDruidSql({ query });
        let taskCounts = {
          successTaskCount: 0,
          failedTaskCount: 0,
          runningTaskCount: 0,
          waitingTaskCount: 0,
          pendingTaskCount: 0
        };
        for (let dataStatus of taskCountsFromSql) {
          if (dataStatus.status === "SUCCESS") {
            taskCounts.successTaskCount = dataStatus.count;
          } else if (dataStatus.status === "FAILED") {
            taskCounts.failedTaskCount = dataStatus.count;
          } else if (dataStatus.status === "RUNNING") {
            taskCounts.runningTaskCount = dataStatus.count;
          } else if (dataStatus.status === "WAITING") {
            taskCounts.waitingTaskCount = dataStatus.count;
          } else {
            taskCounts.pendingTaskCount = dataStatus.count;
          }
        }
        return taskCounts;
      },
      onStateChange: ({ result, loading, error }) => {
        this.setState({
          taskCountLoading: loading,
          successTaskCount: result ? result.successTaskCount : 0,
          failedTaskCount: result ? result.failedTaskCount : 0,
          runningTaskCount: result ? result.runningTaskCount : 0,
          pendingTaskCount: result ? result.pendingTaskCount : 0,
          waitingTaskCount: result ? result.waitingTaskCount : 0,
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

    this.dataServerQueryManager = new QueryManager({
      processQuery: async (query) => {
        const dataServerCounts = await queryDruidSql({ query });
        return dataServerCounts[0].count;
      },
      onStateChange: ({ result, loading, error }) => {
        this.setState({
          dataServerCountLoading: loading,
          dataServerCount: result,
          dataServerCountError: error
        });
      }
    });

    this.dataServerQueryManager.runQuery(`SELECT COUNT(*) as "count" FROM sys.servers WHERE "server_type" = 'historical'`);

    // -------------------------

    this.middleManagerQueryManager = new QueryManager({
      processQuery: async (query) => {
        const middleManagerResp = await axios.get("/druid/indexer/v1/workers");
        const middleManagerCount: number = middleManagerResp.data.length;
        return middleManagerCount;
      },
      onStateChange: ({ result, loading, error }) => {
        this.setState({
          middleManagerCountLoading: loading,
          middleManagerCount: result,
          middleManagerCountError: error
        });
      }
    });

    this.middleManagerQueryManager.runQuery(`dummy`);
  }

  componentWillUnmount(): void {
    this.statusQueryManager.terminate();
    this.datasourceQueryManager.terminate();
    this.segmentQueryManager.terminate();
    this.taskQueryManager.terminate();
    this.dataServerQueryManager.terminate();
    this.middleManagerQueryManager.terminate();
  }

  renderCard(cardOptions: CardOptions): JSX.Element {
    return <a href={cardOptions.href} target={cardOptions.href[0] === '/' ? '_blank' : undefined}>
      <Card interactive={true}>
        <H5><Icon color="#bfccd5" icon={cardOptions.icon}/>&nbsp;{cardOptions.title}</H5>
        {cardOptions.loading ? <p>Loading...</p> : (cardOptions.error ? `Error: ${cardOptions.error}` : cardOptions.content)}
      </Card>
    </a>;
  }

  render() {
    const state = this.state;

    return <div className="home-view app-view">
      {this.renderCard({
        href: "/status",
        icon: IconNames.INFO_SIGN,
        title: "Status",
        loading: state.statusLoading,
        content: state.status ? `Apache Druid is running version ${state.status.version}` : '',
        error: state.statusError
      })}

      {this.renderCard({
        href: "#datasources",
        icon: IconNames.MULTI_SELECT,
        title: "Datasources",
        loading: state.datasourceCountLoading,
        content: pluralIfNeeded(state.datasourceCount, 'datasource'),
        error: state.datasourceCountError
      })}

      {this.renderCard({
        href: "#segments",
        icon: IconNames.FULL_STACKED_CHART,
        title: "Segments",
        loading: state.segmentCountLoading,
        content: pluralIfNeeded(state.segmentCount, 'segment'),
        error: state.datasourceCountError
      })}

      {this.renderCard({
        href: "#tasks",
        icon: IconNames.GANTT_CHART,
        title: "Tasks",
        loading: state.taskCountLoading,
        content: <>
          { Boolean(state.runningTaskCount) && <p>{pluralIfNeeded(state.runningTaskCount, 'running task')}</p> }
          { Boolean(state.pendingTaskCount) && <p>{pluralIfNeeded(state.pendingTaskCount, 'pending task')}</p> }
          { Boolean(state.successTaskCount) && <p>{pluralIfNeeded(state.successTaskCount, 'successful task')}</p> }
          { Boolean(state.waitingTaskCount) && <p>{pluralIfNeeded(state.waitingTaskCount, 'waiting task')}</p> }
          { Boolean(state.failedTaskCount) && <p>{pluralIfNeeded(state.failedTaskCount, 'failed task')}</p> }
          { !(state.runningTaskCount + state.pendingTaskCount + state.successTaskCount + state.waitingTaskCount + state.failedTaskCount) &&
            <p>There are no tasks</p>
          }
          </>,
        error: state.taskCountError
      })}

      {this.renderCard({
        href: "#servers",
        icon: IconNames.DATABASE,
        title: "Data servers",
        loading: state.dataServerCountLoading || state.middleManagerCountLoading,
        content: <>
          <p>{pluralIfNeeded(state.dataServerCount, 'historical')}</p>
          <p>{pluralIfNeeded(state.middleManagerCount, 'middlemanager')}</p>
        </>,
        error: state.dataServerCountError
      })}
    </div>
  }
}

