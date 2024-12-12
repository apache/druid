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
import type { JSX } from 'react';
import React from 'react';
import type { RouteComponentProps } from 'react-router';
import { Redirect } from 'react-router';
import { HashRouter, Route, Switch } from 'react-router-dom';
import type { Filter } from 'react-table';

import type { HeaderActiveTab } from './components';
import { HeaderBar, Loader } from './components';
import type { QueryContext, QueryWithContext } from './druid-models';
import { Capabilities, maybeGetClusterCapacity } from './helpers';
import { stringToTableFilters, tableFiltersToString } from './react-table';
import { AppToaster } from './singletons';
import { compact, localStorageGetJson, LocalStorageKeys, QueryManager } from './utils';
import {
  DatasourcesView,
  ExploreView,
  HomeView,
  LoadDataView,
  LookupsView,
  SegmentsView,
  ServicesView,
  SqlDataLoaderView,
  SupervisorsView,
  TasksView,
  WorkbenchView,
} from './views';

import './console-application.scss';

type FiltersRouteMatch = RouteComponentProps<{ filters?: string }>;

function changeTabWithFilter(tab: HeaderActiveTab, filters: Filter[]) {
  const filterString = tableFiltersToString(filters);
  location.hash = tab + (filterString ? `/${filterString}` : '');
}

function viewFilterChange(tab: HeaderActiveTab) {
  return (filters: Filter[]) => changeTabWithFilter(tab, filters);
}

function pathWithFilter(tab: HeaderActiveTab) {
  return [`/${tab}/:filters`, `/${tab}`];
}

function switchTab(tab: HeaderActiveTab) {
  location.hash = tab;
}

function switchToWorkbenchTab(tabId: string) {
  location.hash = `workbench/${tabId}`;
}

export interface ConsoleApplicationProps {
  baseQueryContext?: QueryContext;
  defaultQueryContext?: QueryContext;
  mandatoryQueryContext?: QueryContext;
  serverQueryContext?: QueryContext;
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
          Some backend druid services are not responding. The console will not function at the
          moment. Make sure that all your Druid services are up and running. Check the logs of
          individual services for troubleshooting.
        </>
      ),
    });
  }

  private supervisorId?: string;
  private taskId?: string;
  private openSupervisorDialog?: boolean;
  private openTaskDialog?: boolean;
  private queryWithContext?: QueryWithContext;

  constructor(props: ConsoleApplicationProps) {
    super(props);
    this.state = {
      capabilities: Capabilities.FULL,
      capabilitiesLoading: true,
    };

    this.capabilitiesQueryManager = new QueryManager({
      processQuery: async () => {
        const capabilitiesOverride = localStorageGetJson(LocalStorageKeys.CAPABILITIES_OVERRIDE);
        const capabilities = capabilitiesOverride
          ? new Capabilities(capabilitiesOverride)
          : await Capabilities.detectCapabilities();

        if (!capabilities) {
          ConsoleApplication.shownServiceNotification();
          return Capabilities.FULL;
        }

        return await Capabilities.detectCapacity(capabilities);
      },
      onStateChange: ({ data, loading, error }) => {
        if (error) {
          console.error('There was an error retrieving the capabilities', error);
        }
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
      this.supervisorId = undefined;
      this.openSupervisorDialog = undefined;
      this.openTaskDialog = undefined;
      this.queryWithContext = undefined;
    }, 50);
  }

  private readonly goToStreamingDataLoader = (supervisorId?: string) => {
    if (supervisorId) this.supervisorId = supervisorId;
    switchTab('streaming-data-loader');
    this.resetInitialsWithDelay();
  };

  private readonly goToClassicBatchDataLoader = (taskId?: string) => {
    if (taskId) this.taskId = taskId;
    switchTab('classic-batch-data-loader');
    this.resetInitialsWithDelay();
  };

  private readonly goToDatasources = (datasource: string) => {
    changeTabWithFilter('datasources', [{ id: 'datasource', value: `=${datasource}` }]);
  };

  private readonly goToSegments = ({
    start,
    end,
    datasource,
    realtime,
  }: {
    start?: Date;
    end?: Date;
    datasource?: string;
    realtime?: boolean;
  }) => {
    changeTabWithFilter(
      'segments',
      compact([
        start && { id: 'start', value: `>=${start.toISOString()}` },
        end && { id: 'end', value: `<${end.toISOString()}` },
        datasource && { id: 'datasource', value: `=${datasource}` },
        typeof realtime === 'boolean' ? { id: 'is_realtime', value: `=${realtime}` } : undefined,
      ]),
    );
  };

  private readonly goToSupervisor = (supervisorId: string) => {
    changeTabWithFilter('supervisors', [{ id: 'supervisor_id', value: `=${supervisorId}` }]);
  };

  private readonly goToTasksWithTaskId = (taskId: string) => {
    changeTabWithFilter('tasks', [{ id: 'task_id', value: `=${taskId}` }]);
  };

  private readonly goToTasksWithTaskGroupId = (taskGroupId: string) => {
    changeTabWithFilter('tasks', [{ id: 'group_id', value: `=${taskGroupId}` }]);
  };

  private readonly goToTasksWithDatasource = (datasource: string, type?: string) => {
    changeTabWithFilter(
      'tasks',
      compact([
        { id: 'datasource', value: `=${datasource}` },
        type ? { id: 'type', value: `=${type}` } : undefined,
      ]),
    );
  };

  private readonly openSupervisorSubmit = () => {
    this.openSupervisorDialog = true;
    switchTab('supervisors');
    this.resetInitialsWithDelay();
  };

  private readonly openTaskSubmit = () => {
    this.openTaskDialog = true;
    switchTab('tasks');
    this.resetInitialsWithDelay();
  };

  private readonly goToQuery = (queryWithContext: QueryWithContext) => {
    this.queryWithContext = queryWithContext;
    switchTab('workbench');
    this.resetInitialsWithDelay();
  };

  private readonly wrapInViewContainer = (
    active: HeaderActiveTab | null,
    el: JSX.Element,
    classType: 'normal' | 'narrow-pad' | 'thin' | 'thinner' = 'normal',
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
    return this.wrapInViewContainer(
      'data-loader',
      <LoadDataView
        mode="all"
        initTaskId={this.taskId}
        initSupervisorId={this.supervisorId}
        goToSupervisor={this.goToSupervisor}
        goToTasks={this.goToTasksWithTaskGroupId}
        openSupervisorSubmit={this.openSupervisorSubmit}
        openTaskSubmit={this.openTaskSubmit}
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
        goToSupervisor={this.goToSupervisor}
        goToTasks={this.goToTasksWithTaskGroupId}
        openSupervisorSubmit={this.openSupervisorSubmit}
        openTaskSubmit={this.openTaskSubmit}
      />,
      'narrow-pad',
    );
  };

  private readonly wrappedClassicBatchDataLoaderView = () => {
    return this.wrapInViewContainer(
      'classic-batch-data-loader',
      <LoadDataView
        mode="batch"
        initTaskId={this.taskId}
        goToSupervisor={this.goToSupervisor}
        goToTasks={this.goToTasksWithTaskGroupId}
        openSupervisorSubmit={this.openSupervisorSubmit}
        openTaskSubmit={this.openTaskSubmit}
      />,
      'narrow-pad',
    );
  };

  private readonly wrappedWorkbenchView = (p: RouteComponentProps<{ tabId?: string }>) => {
    const { defaultQueryContext, mandatoryQueryContext, baseQueryContext, serverQueryContext } =
      this.props;
    const { capabilities } = this.state;

    return this.wrapInViewContainer(
      'workbench',
      <WorkbenchView
        capabilities={capabilities}
        tabId={p.match.params.tabId}
        onTabChange={switchToWorkbenchTab}
        initQueryWithContext={this.queryWithContext}
        defaultQueryContext={defaultQueryContext}
        mandatoryQueryContext={mandatoryQueryContext}
        baseQueryContext={baseQueryContext}
        serverQueryContext={serverQueryContext}
        queryEngines={capabilities.getSupportedQueryEngines()}
        goToTask={this.goToTasksWithTaskId}
        getClusterCapacity={maybeGetClusterCapacity}
      />,
      'thin',
    );
  };

  private readonly wrappedSqlDataLoaderView = () => {
    const { serverQueryContext } = this.props;
    const { capabilities } = this.state;
    return this.wrapInViewContainer(
      'sql-data-loader',
      <SqlDataLoaderView
        capabilities={capabilities}
        goToQuery={this.goToQuery}
        goToTask={this.goToTasksWithTaskId}
        goToTaskGroup={this.goToTasksWithTaskGroupId}
        getClusterCapacity={maybeGetClusterCapacity}
        serverQueryContext={serverQueryContext}
      />,
    );
  };

  private readonly wrappedDatasourcesView = (p: FiltersRouteMatch) => {
    const { capabilities } = this.state;
    return this.wrapInViewContainer(
      'datasources',
      <DatasourcesView
        filters={stringToTableFilters(p.match.params.filters)}
        onFiltersChange={viewFilterChange('datasources')}
        goToQuery={this.goToQuery}
        goToTasks={this.goToTasksWithDatasource}
        goToSegments={this.goToSegments}
        capabilities={capabilities}
      />,
    );
  };

  private readonly wrappedSegmentsView = (p: FiltersRouteMatch) => {
    const { capabilities } = this.state;
    return this.wrapInViewContainer(
      'segments',
      <SegmentsView
        filters={stringToTableFilters(p.match.params.filters)}
        onFiltersChange={viewFilterChange('segments')}
        goToQuery={this.goToQuery}
        capabilities={capabilities}
      />,
    );
  };

  private readonly wrappedSupervisorsView = (p: FiltersRouteMatch) => {
    const { capabilities } = this.state;
    return this.wrapInViewContainer(
      'supervisors',
      <SupervisorsView
        filters={stringToTableFilters(p.match.params.filters)}
        onFiltersChange={viewFilterChange('supervisors')}
        openSupervisorDialog={this.openSupervisorDialog}
        goToDatasource={this.goToDatasources}
        goToQuery={this.goToQuery}
        goToStreamingDataLoader={this.goToStreamingDataLoader}
        goToTasks={this.goToTasksWithDatasource}
        capabilities={capabilities}
      />,
    );
  };

  private readonly wrappedTasksView = (p: FiltersRouteMatch) => {
    const { capabilities } = this.state;
    return this.wrapInViewContainer(
      'tasks',
      <TasksView
        filters={stringToTableFilters(p.match.params.filters)}
        onFiltersChange={viewFilterChange('tasks')}
        openTaskDialog={this.openTaskDialog}
        goToDatasource={this.goToDatasources}
        goToQuery={this.goToQuery}
        goToClassicBatchDataLoader={this.goToClassicBatchDataLoader}
        capabilities={capabilities}
      />,
    );
  };

  private readonly wrappedServicesView = (p: FiltersRouteMatch) => {
    const { capabilities } = this.state;
    return this.wrapInViewContainer(
      'services',
      <ServicesView
        filters={stringToTableFilters(p.match.params.filters)}
        onFiltersChange={viewFilterChange('services')}
        goToQuery={this.goToQuery}
        capabilities={capabilities}
      />,
    );
  };

  private readonly wrappedLookupsView = (p: FiltersRouteMatch) => {
    return this.wrapInViewContainer(
      'lookups',
      <LookupsView
        filters={stringToTableFilters(p.match.params.filters)}
        onFiltersChange={viewFilterChange('lookups')}
      />,
    );
  };

  private readonly wrappedExploreView = () => {
    return this.wrapInViewContainer('explore', <ExploreView />, 'thinner');
  };

  render() {
    const { capabilities, capabilitiesLoading } = this.state;

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
              {capabilities.hasCoordinatorAccess() && (
                <Route path="/data-loader" component={this.wrappedDataLoaderView} />
              )}
              {capabilities.hasCoordinatorAccess() && (
                <Route
                  path="/streaming-data-loader"
                  component={this.wrappedStreamingDataLoaderView}
                />
              )}
              {capabilities.hasCoordinatorAccess() && (
                <Route
                  path="/classic-batch-data-loader"
                  component={this.wrappedClassicBatchDataLoaderView}
                />
              )}
              {capabilities.hasCoordinatorAccess() && capabilities.hasMultiStageQueryTask() && (
                <Route path="/sql-data-loader" component={this.wrappedSqlDataLoaderView} />
              )}

              <Route path={pathWithFilter('supervisors')} component={this.wrappedSupervisorsView} />
              <Route path={pathWithFilter('tasks')} component={this.wrappedTasksView} />
              <Route path="/ingestion">
                <Redirect to="/tasks" />
              </Route>

              <Route path={pathWithFilter('datasources')} component={this.wrappedDatasourcesView} />
              <Route path={pathWithFilter('segments')} component={this.wrappedSegmentsView} />
              <Route path={pathWithFilter('services')} component={this.wrappedServicesView} />

              <Route path="/query">
                <Redirect to="/workbench" />
              </Route>
              <Route
                path={['/workbench/:tabId', '/workbench']}
                component={this.wrappedWorkbenchView}
              />

              {capabilities.hasCoordinatorAccess() && (
                <Route path={pathWithFilter('lookups')} component={this.wrappedLookupsView} />
              )}

              {capabilities.hasSql() && (
                <Route path="/explore" component={this.wrappedExploreView} />
              )}

              <Route component={this.wrappedHomeView} />
            </Switch>
          </div>
        </HashRouter>
      </HotkeysProvider>
    );
  }
}
