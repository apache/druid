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
import classNames from 'classnames';
import React from 'react';
import { HashRouter, Route, Switch } from 'react-router-dom';

import { HeaderActiveTab, HeaderBar, Loader } from './components';
import { AppToaster } from './singletons/toaster';
import { QueryManager } from './utils';
import { Capabilities } from './utils/capabilities';
import {
  DatasourcesView,
  HomeView,
  IngestionView,
  LoadDataView,
  LookupsView,
  QueryView,
  SegmentsView,
  ServicesView,
} from './views';

import './console-application.scss';

export interface ConsoleApplicationProps {
  exampleManifestsUrl?: string;
  defaultQueryContext?: Record<string, any>;
  mandatoryQueryContext?: Record<string, any>;
}

export interface ConsoleApplicationState {
  capabilities: Capabilities;
  capabilitiesLoading: boolean;
}

export class ConsoleApplication extends React.PureComponent<
  ConsoleApplicationProps,
  ConsoleApplicationState
> {
  private capabilitiesQueryManager: QueryManager<null, Capabilities>;

  static shownNotifications() {
    AppToaster.show({
      icon: IconNames.ERROR,
      intent: Intent.DANGER,
      timeout: 120000,
      message: (
        <>
          It appears that the service serving this console is not responding. The console will not
          function at the moment.
        </>
      ),
    });
  }

  private supervisorId?: string;
  private taskId?: string;
  private taskGroupId?: string;
  private openDialog?: string;
  private datasource?: string;
  private onlyUnavailable?: boolean;
  private initQuery?: string;
  private middleManager?: string;

  constructor(props: ConsoleApplicationProps, context: any) {
    super(props, context);
    this.state = {
      capabilities: Capabilities.FULL,
      capabilitiesLoading: true,
    };

    this.capabilitiesQueryManager = new QueryManager({
      processQuery: async () => {
        const capabilities = await Capabilities.detectCapabilities();
        if (!capabilities) ConsoleApplication.shownNotifications();
        return capabilities || Capabilities.FULL;
      },
      onStateChange: ({ result, loading }) => {
        this.setState({
          capabilities: result || Capabilities.FULL,
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
      this.taskGroupId = undefined;
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

  private goToIngestionWithTaskGroupId = (taskGroupId?: string, openDialog?: string) => {
    this.taskGroupId = taskGroupId;
    if (openDialog) this.openDialog = openDialog;
    window.location.hash = 'ingestion';
    this.resetInitialsWithDelay();
  };

  private goToIngestionWithDatasource = (datasource?: string, openDialog?: string) => {
    this.datasource = datasource;
    if (openDialog) this.openDialog = openDialog;
    window.location.hash = 'ingestion';
    this.resetInitialsWithDelay();
  };

  private goToMiddleManager = (middleManager: string) => {
    this.middleManager = middleManager;
    window.location.hash = 'services';
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
    const { capabilities } = this.state;

    return (
      <>
        <HeaderBar active={active} capabilities={capabilities} />
        <div className={classNames('view-container', classType)}>{el}</div>
      </>
    );
  };

  private wrappedHomeView = () => {
    const { capabilities } = this.state;
    return this.wrapInViewContainer(null, <HomeView capabilities={capabilities} />);
  };

  private wrappedLoadDataView = () => {
    const { exampleManifestsUrl } = this.props;

    return this.wrapInViewContainer(
      'load-data',
      <LoadDataView
        initSupervisorId={this.supervisorId}
        initTaskId={this.taskId}
        exampleManifestsUrl={exampleManifestsUrl}
        goToIngestion={this.goToIngestionWithTaskGroupId}
      />,
      'narrow-pad',
    );
  };

  private wrappedQueryView = () => {
    const { defaultQueryContext, mandatoryQueryContext } = this.props;

    return this.wrapInViewContainer(
      'query',
      <QueryView
        initQuery={this.initQuery}
        defaultQueryContext={defaultQueryContext}
        mandatoryQueryContext={mandatoryQueryContext}
      />,
    );
  };

  private wrappedDatasourcesView = () => {
    const { capabilities } = this.state;
    return this.wrapInViewContainer(
      'datasources',
      <DatasourcesView
        initDatasource={this.datasource}
        goToQuery={this.goToQuery}
        goToTask={this.goToIngestionWithDatasource}
        goToSegments={this.goToSegments}
        capabilities={capabilities}
      />,
    );
  };

  private wrappedSegmentsView = () => {
    const { capabilities } = this.state;
    return this.wrapInViewContainer(
      'segments',
      <SegmentsView
        datasource={this.datasource}
        onlyUnavailable={this.onlyUnavailable}
        goToQuery={this.goToQuery}
        capabilities={capabilities}
      />,
    );
  };

  private wrappedIngestionView = () => {
    const { capabilities } = this.state;
    return this.wrapInViewContainer(
      'ingestion',
      <IngestionView
        taskGroupId={this.taskGroupId}
        datasourceId={this.datasource}
        openDialog={this.openDialog}
        goToDatasource={this.goToDatasources}
        goToQuery={this.goToQuery}
        goToMiddleManager={this.goToMiddleManager}
        goToLoadData={this.goToLoadData}
        capabilities={capabilities}
      />,
    );
  };

  private wrappedServicesView = () => {
    const { capabilities } = this.state;
    return this.wrapInViewContainer(
      'services',
      <ServicesView
        middleManager={this.middleManager}
        goToQuery={this.goToQuery}
        goToTask={this.goToIngestionWithTaskGroupId}
        capabilities={capabilities}
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
          <Loader loadingText="" loading />
        </div>
      );
    }

    return (
      <HashRouter hashType="noslash">
        <div className="console-application">
          <Switch>
            <Route path="/load-data" component={this.wrappedLoadDataView} />

            <Route path="/ingestion" component={this.wrappedIngestionView} />
            <Route path="/datasources" component={this.wrappedDatasourcesView} />
            <Route path="/segments" component={this.wrappedSegmentsView} />
            <Route path="/services" component={this.wrappedServicesView} />

            <Route path="/query" component={this.wrappedQueryView} />

            <Route path="/lookups" component={this.wrappedLookupsView} />
            <Route component={this.wrappedHomeView} />
          </Switch>
        </div>
      </HashRouter>
    );
  }
}
