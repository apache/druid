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

import { HotkeysProvider, Intent } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import classNames from 'classnames';
import React from 'react';
import { Redirect, RouteComponentProps } from 'react-router';
import { HashRouter, Route, Switch } from 'react-router-dom';

import { HeaderActiveTab, HeaderBar, Loader } from './components';
import { DruidEngine, QueryWithContext } from './druid-models';
import { AppToaster } from './singletons';
import { Capabilities, QueryManager } from './utils';
import {
  DatasourcesView,
  HomeView,
  IngestionView,
  LoadDataView,
  LookupsView,
  SegmentsView,
  ServicesView,
  SqlDataLoaderView,
  WorkbenchView,
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
  private readonly capabilitiesQueryManager: QueryManager<null, Capabilities>;

  static shownServiceNotification() {
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
  private queryWithContext?: QueryWithContext;

  constructor(props: ConsoleApplicationProps, context: any) {
    super(props, context);
    this.state = {
      capabilities: Capabilities.FULL,
      capabilitiesLoading: true,
    };

    this.capabilitiesQueryManager = new QueryManager({
      processQuery: async () => {
        const capabilities = await Capabilities.detectCapabilities();
        if (!capabilities) ConsoleApplication.shownServiceNotification();
        return capabilities || Capabilities.FULL;
      },
      onStateChange: ({ data, loading }) => {
        this.setState({
          capabilities: data || Capabilities.FULL,
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

  private readonly handleUnrestrict = (capabilities: Capabilities) => {
    this.setState({ capabilities });
  };

  private resetInitialsWithDelay() {
    setTimeout(() => {
      this.taskId = undefined;
      this.taskGroupId = undefined;
      this.supervisorId = undefined;
      this.openDialog = undefined;
      this.datasource = undefined;
      this.onlyUnavailable = undefined;
      this.queryWithContext = undefined;
    }, 50);
  }

  private readonly goToStreamingDataLoader = (supervisorId?: string) => {
    if (supervisorId) this.supervisorId = supervisorId;
    window.location.hash = 'streaming-data-loader';
    this.resetInitialsWithDelay();
  };

  private readonly goToClassicBatchDataLoader = (taskId?: string) => {
    if (taskId) this.taskId = taskId;
    window.location.hash = 'classic-batch-data-loader';
    this.resetInitialsWithDelay();
  };

  private readonly goToDatasources = (datasource: string) => {
    this.datasource = datasource;
    window.location.hash = 'datasources';
    this.resetInitialsWithDelay();
  };

  private readonly goToSegments = (datasource: string, onlyUnavailable = false) => {
    this.datasource = datasource;
    this.onlyUnavailable = onlyUnavailable;
    window.location.hash = 'segments';
    this.resetInitialsWithDelay();
  };

  private readonly goToIngestionWithTaskId = (taskId?: string) => {
    this.taskId = taskId;
    window.location.hash = 'ingestion';
    this.resetInitialsWithDelay();
  };

  private readonly goToIngestionWithTaskGroupId = (taskGroupId?: string, openDialog?: string) => {
    this.taskGroupId = taskGroupId;
    if (openDialog) this.openDialog = openDialog;
    window.location.hash = 'ingestion';
    this.resetInitialsWithDelay();
  };

  private readonly goToIngestionWithDatasource = (datasource?: string, openDialog?: string) => {
    this.datasource = datasource;
    if (openDialog) this.openDialog = openDialog;
    window.location.hash = 'ingestion';
    this.resetInitialsWithDelay();
  };

  private readonly goToQuery = (queryWithContext: QueryWithContext) => {
    this.queryWithContext = queryWithContext;
    window.location.hash = 'workbench';
    this.resetInitialsWithDelay();
  };

  private readonly wrapInViewContainer = (
    active: HeaderActiveTab,
    el: JSX.Element,
    classType: 'normal' | 'narrow-pad' | 'thin' = 'normal',
  ) => {
    const { capabilities } = this.state;

    return (
      <>
        <HeaderBar
          active={active}
          capabilities={capabilities}
          onUnrestrict={this.handleUnrestrict}
        />
        <div className={classNames('view-container', classType)}>{el}</div>
      </>
    );
  };

  private readonly wrappedHomeView = () => {
    const { capabilities } = this.state;
    return this.wrapInViewContainer(null, <HomeView capabilities={capabilities} />);
  };

  private readonly wrappedDataLoaderView = () => {
    const { exampleManifestsUrl } = this.props;

    return this.wrapInViewContainer(
      'data-loader',
      <LoadDataView
        mode="all"
        initTaskId={this.taskId}
        initSupervisorId={this.supervisorId}
        exampleManifestsUrl={exampleManifestsUrl}
        goToIngestion={this.goToIngestionWithTaskGroupId}
      />,
      'narrow-pad',
    );
  };

  private readonly wrappedStreamingDataLoaderView = () => {
    return this.wrapInViewContainer(
      'streaming-data-loader',
      <LoadDataView
        mode="streaming"
        initSupervisorId={this.supervisorId}
        goToIngestion={this.goToIngestionWithTaskGroupId}
      />,
      'narrow-pad',
    );
  };

  private readonly wrappedClassicBatchDataLoaderView = () => {
    const { exampleManifestsUrl } = this.props;

    return this.wrapInViewContainer(
      'classic-batch-data-loader',
      <LoadDataView
        mode="batch"
        initTaskId={this.taskId}
        exampleManifestsUrl={exampleManifestsUrl}
        goToIngestion={this.goToIngestionWithTaskGroupId}
      />,
      'narrow-pad',
    );
  };

  private readonly wrappedWorkbenchView = (p: RouteComponentProps<any>) => {
    const { defaultQueryContext, mandatoryQueryContext } = this.props;
    const { capabilities } = this.state;

    const queryEngines: DruidEngine[] = ['native'];
    if (capabilities.hasSql()) {
      queryEngines.push('sql-native');
    }
    if (capabilities.hasMultiStageQuery()) {
      queryEngines.push('sql-msq-task');
    }

    return this.wrapInViewContainer(
      'workbench',
      <WorkbenchView
        tabId={p.match.params.tabId}
        onTabChange={newTabId => {
          location.hash = `#workbench/${newTabId}`;
        }}
        initQueryWithContext={this.queryWithContext}
        defaultQueryContext={defaultQueryContext}
        mandatoryQueryContext={mandatoryQueryContext}
        queryEngines={queryEngines}
        allowExplain
        goToIngestion={this.goToIngestionWithTaskId}
      />,
      'thin',
    );
  };

  private readonly wrappedSqlDataLoaderView = () => {
    return this.wrapInViewContainer(
      'sql-data-loader',
      <SqlDataLoaderView goToQuery={this.goToQuery} goToIngestion={this.goToIngestionWithTaskId} />,
    );
  };

  private readonly wrappedDatasourcesView = () => {
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

  private readonly wrappedSegmentsView = () => {
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

  private readonly wrappedIngestionView = () => {
    const { capabilities } = this.state;
    return this.wrapInViewContainer(
      'ingestion',
      <IngestionView
        taskId={this.taskId}
        taskGroupId={this.taskGroupId}
        datasourceId={this.datasource}
        openDialog={this.openDialog}
        goToDatasource={this.goToDatasources}
        goToQuery={this.goToQuery}
        goToStreamingDataLoader={this.goToStreamingDataLoader}
        goToClassicBatchDataLoader={this.goToClassicBatchDataLoader}
        capabilities={capabilities}
      />,
    );
  };

  private readonly wrappedServicesView = () => {
    const { capabilities } = this.state;
    return this.wrapInViewContainer(
      'services',
      <ServicesView goToQuery={this.goToQuery} capabilities={capabilities} />,
    );
  };

  private readonly wrappedLookupsView = () => {
    return this.wrapInViewContainer('lookups', <LookupsView />);
  };

  render(): JSX.Element {
    const { capabilitiesLoading } = this.state;

    if (capabilitiesLoading) {
      return (
        <div className="loading-capabilities">
          <Loader />
        </div>
      );
    }

    return (
      <HotkeysProvider>
        <HashRouter hashType="noslash">
          <div className="console-application">
            <Switch>
              <Route path="/data-loader" component={this.wrappedDataLoaderView} />
              <Route
                path="/streaming-data-loader"
                component={this.wrappedStreamingDataLoaderView}
              />
              <Route
                path="/classic-batch-data-loader"
                component={this.wrappedClassicBatchDataLoaderView}
              />

              <Route path="/ingestion" component={this.wrappedIngestionView} />
              <Route path="/datasources" component={this.wrappedDatasourcesView} />
              <Route path="/segments" component={this.wrappedSegmentsView} />
              <Route path="/services" component={this.wrappedServicesView} />

              <Route path="/query">
                <Redirect to="/workbench" />
              </Route>
              <Route
                path={['/workbench/:tabId', '/workbench']}
                component={this.wrappedWorkbenchView}
              />
              <Route path="/sql-data-loader" component={this.wrappedSqlDataLoaderView} />

              <Route path="/lookups" component={this.wrappedLookupsView} />
              <Route component={this.wrappedHomeView} />
            </Switch>
          </div>
        </HashRouter>
      </HotkeysProvider>
    );
  }
}
