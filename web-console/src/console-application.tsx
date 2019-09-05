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

import { Intent } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import axios from 'axios';
import classNames from 'classnames';
import React from 'react';
import { HashRouter, Route, Switch } from 'react-router-dom';

import { ExternalLink, HeaderActiveTab, HeaderBar, Loader } from './components';
import { AppToaster } from './singletons/toaster';
import { localStorageGet, LocalStorageKeys, QueryManager } from './utils';
import { DRUID_DOCS_API, DRUID_DOCS_SQL } from './variables';
import {
  DatasourcesView,
  HomeView,
  LoadDataView,
  LookupsView,
  QueryView,
  SegmentsView,
  ServersView,
  TasksView,
} from './views';

import './console-application.scss';

type Capabilities = 'working-with-sql' | 'working-without-sql' | 'broken';

export interface ConsoleApplicationProps {
  hideLegacy: boolean;
  exampleManifestsUrl?: string;
}

export interface ConsoleApplicationState {
  noSqlMode: boolean;
  capabilitiesLoading: boolean;
}

export class ConsoleApplication extends React.PureComponent<
  ConsoleApplicationProps,
  ConsoleApplicationState
> {
  static STATUS_TIMEOUT = 2000;

  private capabilitiesQueryManager: QueryManager<null, Capabilities>;

  static async discoverCapabilities(): Promise<Capabilities> {
    const capabilitiesOverride = localStorageGet(LocalStorageKeys.CAPABILITIES_OVERRIDE);
    if (capabilitiesOverride) return capabilitiesOverride as Capabilities;

    try {
      await axios.post(
        '/druid/v2/sql',
        { query: 'SELECT 1337', context: { timeout: ConsoleApplication.STATUS_TIMEOUT } },
        { timeout: ConsoleApplication.STATUS_TIMEOUT },
      );
    } catch (e) {
      const { response } = e;
      if (response.status !== 405 || response.statusText !== 'Method Not Allowed') {
        return 'working-with-sql'; // other failure
      }
      try {
        await axios.get('/status', { timeout: ConsoleApplication.STATUS_TIMEOUT });
      } catch (e) {
        return 'broken'; // total failure
      }
      // Status works but SQL 405s => the SQL endpoint is disabled
      return 'working-without-sql';
    }
    return 'working-with-sql';
  }

  static shownNotifications(capabilities: string) {
    let message: JSX.Element = <></>;

    if (capabilities === 'working-without-sql') {
      message = (
        <>
          It appears that the SQL endpoint is disabled. The console will fall back to{' '}
          <ExternalLink href={DRUID_DOCS_API}>native Druid APIs</ExternalLink> and will be limited
          in functionality. Look at <ExternalLink href={DRUID_DOCS_SQL}>the SQL docs</ExternalLink>{' '}
          to enable the SQL endpoint.
        </>
      );
    } else if (capabilities === 'broken') {
      message = (
        <>It appears that the Druid is not responding. Data cannot be retrieved right now</>
      );
    }

    AppToaster.show({
      icon: IconNames.ERROR,
      intent: Intent.DANGER,
      timeout: 120000,
      message: message,
    });
  }

  private supervisorId?: string;
  private taskId?: string;
  private openDialog?: string;
  private datasource?: string;
  private onlyUnavailable?: boolean;
  private initQuery?: string;
  private middleManager?: string;

  constructor(props: ConsoleApplicationProps, context: any) {
    super(props, context);
    this.state = {
      noSqlMode: false,
      capabilitiesLoading: true,
    };

    this.capabilitiesQueryManager = new QueryManager({
      processQuery: async () => {
        const capabilities = await ConsoleApplication.discoverCapabilities();
        if (capabilities !== 'working-with-sql') {
          ConsoleApplication.shownNotifications(capabilities);
        }
        return capabilities;
      },
      onStateChange: ({ result, loading }) => {
        this.setState({
          noSqlMode: result !== 'working-with-sql',
          capabilitiesLoading: loading,
        });
      },
    });
  }

  componentDidMount(): void {
    this.capabilitiesQueryManager.runQuery(null);
  }

  componentWillUnmount(): void {
    this.capabilitiesQueryManager.terminate();
  }

  private resetInitialsWithDelay() {
    setTimeout(() => {
      this.taskId = undefined;
      this.supervisorId = undefined;
      this.openDialog = undefined;
      this.datasource = undefined;
      this.onlyUnavailable = undefined;
      this.initQuery = undefined;
      this.middleManager = undefined;
    }, 50);
  }

  private goToLoadData = (supervisorId?: string, taskId?: string) => {
    if (taskId) this.taskId = taskId;
    if (supervisorId) this.supervisorId = supervisorId;
    window.location.hash = 'load-data';
    this.resetInitialsWithDelay();
  };

  private goToDatasources = (datasource: string) => {
    this.datasource = datasource;
    window.location.hash = 'datasources';
    this.resetInitialsWithDelay();
  };

  private goToSegments = (datasource: string, onlyUnavailable = false) => {
    this.datasource = datasource;
    this.onlyUnavailable = onlyUnavailable;
    window.location.hash = 'segments';
    this.resetInitialsWithDelay();
  };

  private goToTaskWithTaskId = (taskId?: string, openDialog?: string) => {
    this.taskId = taskId;
    if (openDialog) this.openDialog = openDialog;
    window.location.hash = 'tasks';
    this.resetInitialsWithDelay();
  };

  private goToTaskWithDatasource = (datasource?: string, openDialog?: string) => {
    this.datasource = datasource;
    if (openDialog) this.openDialog = openDialog;
    window.location.hash = 'tasks';
    this.resetInitialsWithDelay();
  };

  private goToMiddleManager = (middleManager: string) => {
    this.middleManager = middleManager;
    window.location.hash = 'servers';
    this.resetInitialsWithDelay();
  };

  private goToQuery = (initQuery: string) => {
    this.initQuery = initQuery;
    window.location.hash = 'query';
    this.resetInitialsWithDelay();
  };

  private wrapInViewContainer = (
    active: HeaderActiveTab,
    el: JSX.Element,
    classType: 'normal' | 'narrow-pad' = 'normal',
  ) => {
    const { hideLegacy } = this.props;

    return (
      <>
        <HeaderBar active={active} hideLegacy={hideLegacy} />
        <div className={classNames('view-container', classType)}>{el}</div>
      </>
    );
  };

  private wrappedHomeView = () => {
    const { noSqlMode } = this.state;
    return this.wrapInViewContainer(null, <HomeView noSqlMode={noSqlMode} />);
  };

  private wrappedLoadDataView = () => {
    const { exampleManifestsUrl } = this.props;

    return this.wrapInViewContainer(
      'load-data',
      <LoadDataView
        initSupervisorId={this.supervisorId}
        initTaskId={this.taskId}
        exampleManifestsUrl={exampleManifestsUrl}
        goToTask={this.goToTaskWithTaskId}
      />,
      'narrow-pad',
    );
  };

  private wrappedQueryView = () => {
    return this.wrapInViewContainer('query', <QueryView initQuery={this.initQuery} />);
  };

  private wrappedDatasourcesView = () => {
    const { noSqlMode } = this.state;
    return this.wrapInViewContainer(
      'datasources',
      <DatasourcesView
        initDatasource={this.datasource}
        goToQuery={this.goToQuery}
        goToTask={this.goToTaskWithDatasource}
        goToSegments={this.goToSegments}
        noSqlMode={noSqlMode}
      />,
    );
  };

  private wrappedSegmentsView = () => {
    const { noSqlMode } = this.state;
    return this.wrapInViewContainer(
      'segments',
      <SegmentsView
        datasource={this.datasource}
        onlyUnavailable={this.onlyUnavailable}
        goToQuery={this.goToQuery}
        noSqlMode={noSqlMode}
      />,
    );
  };

  private wrappedTasksView = () => {
    const { noSqlMode } = this.state;
    return this.wrapInViewContainer(
      'tasks',
      <TasksView
        taskId={this.taskId}
        datasourceId={this.datasource}
        openDialog={this.openDialog}
        goToDatasource={this.goToDatasources}
        goToQuery={this.goToQuery}
        goToMiddleManager={this.goToMiddleManager}
        goToLoadData={this.goToLoadData}
        noSqlMode={noSqlMode}
      />,
    );
  };

  private wrappedServersView = () => {
    const { noSqlMode } = this.state;
    return this.wrapInViewContainer(
      'servers',
      <ServersView
        middleManager={this.middleManager}
        goToQuery={this.goToQuery}
        goToTask={this.goToTaskWithTaskId}
        noSqlMode={noSqlMode}
      />,
    );
  };

  private wrappedLookupsView = () => {
    return this.wrapInViewContainer('lookups', <LookupsView />);
  };

  render(): JSX.Element {
    const { capabilitiesLoading } = this.state;

    if (capabilitiesLoading) {
      return (
        <div className="loading-capabilities">
          <Loader loadingText="" loading={capabilitiesLoading} />
        </div>
      );
    }

    return (
      <HashRouter hashType="noslash">
        <div className="console-application">
          <Switch>
            <Route path="/load-data" component={this.wrappedLoadDataView} />
            <Route path="/query" component={this.wrappedQueryView} />

            <Route path="/datasources" component={this.wrappedDatasourcesView} />
            <Route path="/segments" component={this.wrappedSegmentsView} />
            <Route path="/tasks" component={this.wrappedTasksView} />
            <Route path="/servers" component={this.wrappedServersView} />

            <Route path="/lookups" component={this.wrappedLookupsView} />
            <Route component={this.wrappedHomeView} />
          </Switch>
        </div>
      </HashRouter>
    );
  }
}
