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
import * as classNames from 'classnames';
import * as React from 'react';
import { HashRouter, Route, Switch } from 'react-router-dom';

import { ExternalLink } from './components/external-link/external-link';
import { HeaderActiveTab, HeaderBar } from './components/header-bar/header-bar';
import { Loader } from './components/loader/loader';
import { AppToaster } from './singletons/toaster';
import { UrlBaser } from './singletons/url-baser';
import { QueryManager } from './utils';
import { DRUID_DOCS_API, DRUID_DOCS_SQL } from './variables';
import { DatasourcesView } from './views/datasource-view/datasource-view';
import { HomeView } from './views/home-view/home-view';
import { LoadDataView } from './views/load-data-view/load-data-view';
import { LookupsView } from './views/lookups-view/lookups-view';
import { SegmentsView } from './views/segments-view/segments-view';
import { ServersView } from './views/servers-view/servers-view';
import { SqlView } from './views/sql-view/sql-view';
import { TasksView } from './views/task-view/tasks-view';

import './console-application.scss';

export interface ConsoleApplicationProps extends React.Props<any> {
  hideLegacy: boolean;
  baseURL?: string;
  customHeaderName?: string;
  customHeaderValue?: string;
}

export interface ConsoleApplicationState {
  aboutDialogOpen: boolean;
  noSqlMode: boolean;
  capabilitiesLoading: boolean;
}

export class ConsoleApplication extends React.Component<ConsoleApplicationProps, ConsoleApplicationState> {
  static MESSAGE_KEY = 'druid-console-message';
  static MESSAGE_DISMISSED = 'dismissed';
  private capabilitiesQueryManager: QueryManager<string, string>;

  static async discoverCapabilities(): Promise<'working-with-sql' | 'working-without-sql' | 'broken'> {
    try {
      await axios.post('/druid/v2/sql', { query: 'SELECT 1337' });
    } catch (e) {
      const { response } = e;
      if (response.status !== 405 || response.statusText !== 'Method Not Allowed') return 'working-with-sql'; // other failure
      try {
        await axios.get('/status');
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
    /* tslint:disable:jsx-alignment */
    if (capabilities === 'working-without-sql') {
      message = <>
        It appears that the SQL endpoint is disabled. The console will fall back
        to <ExternalLink href={DRUID_DOCS_API}>native Druid APIs</ExternalLink> and will be
        limited in functionality. Look at <ExternalLink href={DRUID_DOCS_SQL}>the SQL docs</ExternalLink> to
        enable the SQL endpoint.
      </>;
    } else if (capabilities === 'broken') {
      message = <>
        It appears that the Druid is not responding. Data cannot be retrieved right now
      </>;
    }
    /* tslint:enable:jsx-alignment */
    AppToaster.show({
      icon: IconNames.ERROR,
      intent: Intent.DANGER,
      timeout: 120000,
      message: message
    });
  }

  private supervisorId: string | null;
  private taskId: string | null;
  private openDialog: string | null;
  private datasource: string | null;
  private onlyUnavailable: boolean | null;
  private initSql: string | null;
  private middleManager: string | null;

  constructor(props: ConsoleApplicationProps, context: any) {
    super(props, context);
    this.state = {
      aboutDialogOpen: false,
      noSqlMode: false,
      capabilitiesLoading: true
    };

    if (props.baseURL) {
      axios.defaults.baseURL = props.baseURL;
      UrlBaser.baseURL = props.baseURL;
    }
    if (props.customHeaderName && props.customHeaderValue) {
      axios.defaults.headers.common[props.customHeaderName] = props.customHeaderValue;
    }

    this.capabilitiesQueryManager = new QueryManager({
      processQuery: async (query: string) => {
        const capabilities = await ConsoleApplication.discoverCapabilities();
        if (capabilities !== 'working-with-sql') {
          ConsoleApplication.shownNotifications(capabilities);
        }
        return capabilities;
      },
      onStateChange: ({ result, loading, error }) => {
        this.setState({
          noSqlMode: result === 'working-with-sql' ? false : true,
          capabilitiesLoading: loading
        });
      }
    });
  }

  componentDidMount(): void {
    this.capabilitiesQueryManager.runQuery('dummy');
  }

  componentWillUnmount(): void {
    this.capabilitiesQueryManager.terminate();
  }

  private resetInitialsWithDelay() {
    setTimeout(() => {
      this.taskId = null;
      this.supervisorId = null;
      this.openDialog = null;
      this.datasource = null;
      this.onlyUnavailable = null;
      this.initSql = null;
      this.middleManager = null;
    }, 50);
  }

  private goToLoadDataView = (supervisorId?: string, taskId?: string ) => {
    if (taskId) this.taskId = taskId;
    if (supervisorId) this.supervisorId = supervisorId;
    window.location.hash = 'load-data';
    this.resetInitialsWithDelay();
  }

  private goToTask = (taskId: string | null, openDialog?: string) => {
    this.taskId = taskId;
    if (openDialog) this.openDialog = openDialog;
    window.location.hash = 'tasks';
    this.resetInitialsWithDelay();
  }

  private goToSegments = (datasource: string, onlyUnavailable = false) => {
    this.datasource = `"${datasource}"`;
    this.onlyUnavailable = onlyUnavailable;
    window.location.hash = 'segments';
    this.resetInitialsWithDelay();
  }

  private goToMiddleManager = (middleManager: string) => {
    this.middleManager = middleManager;
    window.location.hash = 'servers';
    this.resetInitialsWithDelay();
  }

  private goToSql = (initSql: string) => {
    this.initSql = initSql;
    window.location.hash = 'query';
    this.resetInitialsWithDelay();
  }

  private wrapInViewContainer = (active: HeaderActiveTab, el: JSX.Element, classType: 'normal' | 'narrow-pad' = 'normal') => {
    const { hideLegacy } = this.props;

    return <>
      <HeaderBar active={active} hideLegacy={hideLegacy}/>
      <div className={classNames('view-container', classType)}>{el}</div>
    </>;
  }

  private wrappedHomeView = () => {
    const { noSqlMode } = this.state;
    return this.wrapInViewContainer(null, <HomeView noSqlMode={noSqlMode}/>);
  }

  private wrappedLoadDataView = () => {

    return this.wrapInViewContainer('load-data', <LoadDataView initSupervisorId={this.supervisorId} initTaskId={this.taskId} goToTask={this.goToTask}/>, 'narrow-pad');
  }

  private wrappedSqlView = () => {
    return this.wrapInViewContainer('query', <SqlView initSql={this.initSql}/>);
  }

  private wrappedDatasourcesView = () => {
    const { noSqlMode } = this.state;
    return this.wrapInViewContainer('datasources', <DatasourcesView goToSql={this.goToSql} goToSegments={this.goToSegments} noSqlMode={noSqlMode}/>);
  }

  private wrappedSegmentsView = () => {
    const { noSqlMode } = this.state;
    return this.wrapInViewContainer('segments', <SegmentsView datasource={this.datasource} onlyUnavailable={this.onlyUnavailable} goToSql={this.goToSql} noSqlMode={noSqlMode}/>);
  }

  private wrappedTasksView = () => {
    const { noSqlMode } = this.state;
    return this.wrapInViewContainer('tasks', <TasksView taskId={this.taskId} openDialog={this.openDialog} goToSql={this.goToSql} goToMiddleManager={this.goToMiddleManager} goToLoadDataView={this.goToLoadDataView} noSqlMode={noSqlMode}/>);
  }

  private wrappedServersView = () => {
    const { noSqlMode } = this.state;
    return this.wrapInViewContainer('servers', <ServersView middleManager={this.middleManager} goToSql={this.goToSql} goToTask={this.goToTask} noSqlMode={noSqlMode}/>);
  }

  private wrappedLookupsView = () => {
    return this.wrapInViewContainer('lookups', <LookupsView/>);
  }

  render() {
    const { capabilitiesLoading } = this.state;

    if (capabilitiesLoading) {
      return <div className="loading-capabilities">
        <Loader
          loadingText=""
          loading={capabilitiesLoading}
        />
      </div>;
    }

    return <HashRouter hashType="noslash">
      <div className="console-application">
        <Switch>
          <Route path="/load-data" component={this.wrappedLoadDataView}/>
          <Route path="/query" component={this.wrappedSqlView}/>
          <Route path="/sql" component={this.wrappedSqlView}/>

          <Route path="/datasources" component={this.wrappedDatasourcesView}/>
          <Route path="/segments" component={this.wrappedSegmentsView}/>
          <Route path="/tasks" component={this.wrappedTasksView}/>
          <Route path="/servers" component={this.wrappedServersView}/>

          <Route path="/lookups" component={this.wrappedLookupsView}/>
          <Route component={this.wrappedHomeView}/>
        </Switch>
      </div>
    </HashRouter>;
  }
}
