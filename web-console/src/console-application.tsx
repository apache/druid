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

import { HeaderBar, Loader } from './components';
import { SqlFunctionsProvider } from './contexts/sql-functions-context';
import type { ConsoleViewId, QueryContext, QueryWithContext } from './druid-models';
import type { AvailableFunctions } from './helpers';
import { Capabilities, maybeGetClusterCapacity } from './helpers';
import { AppToaster } from './singletons';
import { localStorageGetJson, LocalStorageKeys, QueryManager } from './utils';
import { getHashPath, parseHashRoute, replaceHashPath } from './utils/hash-routing';
import { TableFilters } from './utils/table-filters';
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

interface FiltersRouteProps {
  filters?: string;
}

interface WorkbenchRouteProps {
  tabId?: string;
}

// Old routes that now live elsewhere
const REDIRECTS: Record<string, ConsoleViewId> = {
  ingestion: 'tasks',
  query: 'workbench',
};

function goToView(tab: ConsoleViewId, filters?: TableFilters) {
  if (!filters || filters.isEmpty()) {
    location.hash = tab;
    return;
  }
  const filterString = filters.toString();
  location.hash = tab + (filterString ? `/${filterString}` : '');
}

function viewFilterChange(tab: ConsoleViewId) {
  return (filters: TableFilters) => goToView(tab, filters);
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
  availableSqlFunctions?: AvailableFunctions;
  capabilitiesLoading: boolean;
  hashPath: string;
}

export class ConsoleApplication extends React.PureComponent<
  ConsoleApplicationProps,
  ConsoleApplicationState
> {
  private readonly capabilitiesQueryManager: QueryManager<
    null,
    [Capabilities, AvailableFunctions | undefined]
  >;

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
      hashPath: getHashPath(),
    };

    this.capabilitiesQueryManager = new QueryManager({
      processQuery: async () => {
        const capabilitiesOverride = localStorageGetJson(LocalStorageKeys.CAPABILITIES_OVERRIDE);
        const capabilities = capabilitiesOverride
          ? new Capabilities(capabilitiesOverride)
          : await Capabilities.detectCapabilities();

        if (!capabilities) {
          ConsoleApplication.shownServiceNotification();
          return [Capabilities.FULL, undefined];
        }

        return Promise.all([
          Capabilities.detectCapacity(capabilities),
          Capabilities.detectAvailableSqlFunctions(capabilities),
        ]);
      },
      onStateChange: ({ data, loading, error }) => {
        if (error) {
          console.error('There was an error retrieving the capabilities', error);
        }
        const capabilities = data?.[0] || Capabilities.FULL;
        const availableSqlFunctions = data?.[1];
        this.setState({
          capabilities,
          availableSqlFunctions,
          capabilitiesLoading: loading,
        });
      },
    });
  }

  componentDidMount(): void {
    window.addEventListener('hashchange', this.handleHashChange);
    this.handleHashChange();
    this.capabilitiesQueryManager.runQuery(null);
  }

  componentWillUnmount(): void {
    window.removeEventListener('hashchange', this.handleHashChange);
    this.capabilitiesQueryManager.terminate();
  }

  private readonly handleHashChange = () => {
    const hashPath = getHashPath();

    // Normalize #/view to #view and follow redirects, the resulting hashchange will render the view
    if (hashPath.startsWith('/')) {
      replaceHashPath(hashPath.slice(1));
      return;
    }
    const redirect = REDIRECTS[parseHashRoute(hashPath).view];
    if (redirect) {
      replaceHashPath(redirect);
      return;
    }

    this.setState({ hashPath });
  };

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
    goToView('streaming-data-loader');
    this.resetInitialsWithDelay();
  };

  private readonly goToClassicBatchDataLoader = (taskId?: string) => {
    if (taskId) this.taskId = taskId;
    goToView('classic-batch-data-loader');
    this.resetInitialsWithDelay();
  };

  private readonly openSupervisorSubmit = () => {
    this.openSupervisorDialog = true;
    goToView('supervisors');
    this.resetInitialsWithDelay();
  };

  private readonly openTaskSubmit = () => {
    this.openTaskDialog = true;
    goToView('tasks');
    this.resetInitialsWithDelay();
  };

  private readonly goToQuery = (queryWithContext: QueryWithContext) => {
    this.queryWithContext = queryWithContext;
    goToView('workbench');
    this.resetInitialsWithDelay();
  };

  private readonly wrapInViewContainer = (
    active: ConsoleViewId | null,
    el: JSX.Element,
    classType: 'normal' | 'narrow-pad' | 'thin' | 'thinner' = 'normal',
  ) => {
    const { capabilities } = this.state;

    return (
      <>
        <HeaderBar
          activeView={active}
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
        goToView={goToView}
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
        goToView={goToView}
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
        goToView={goToView}
        openSupervisorSubmit={this.openSupervisorSubmit}
        openTaskSubmit={this.openTaskSubmit}
      />,
      'narrow-pad',
    );
  };

  private readonly wrappedWorkbenchView = ({ tabId }: WorkbenchRouteProps) => {
    const { defaultQueryContext, mandatoryQueryContext, baseQueryContext, serverQueryContext } =
      this.props;
    const { capabilities } = this.state;

    return this.wrapInViewContainer(
      'workbench',
      <WorkbenchView
        capabilities={capabilities}
        tabId={tabId}
        onTabChange={switchToWorkbenchTab}
        initQueryWithContext={this.queryWithContext}
        defaultQueryContext={defaultQueryContext}
        mandatoryQueryContext={mandatoryQueryContext}
        baseQueryContext={baseQueryContext}
        serverQueryContext={serverQueryContext}
        queryEngines={capabilities.getSupportedQueryEngines()}
        goToView={goToView}
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
        goToView={goToView}
        getClusterCapacity={maybeGetClusterCapacity}
        serverQueryContext={serverQueryContext}
      />,
    );
  };

  private readonly wrappedDatasourcesView = ({ filters }: FiltersRouteProps) => {
    const { capabilities } = this.state;
    return this.wrapInViewContainer(
      'datasources',
      <DatasourcesView
        filters={TableFilters.fromString(filters)}
        onFiltersChange={viewFilterChange('datasources')}
        goToQuery={this.goToQuery}
        goToView={goToView}
        capabilities={capabilities}
      />,
    );
  };

  private readonly wrappedSegmentsView = ({ filters }: FiltersRouteProps) => {
    const { capabilities } = this.state;
    return this.wrapInViewContainer(
      'segments',
      <SegmentsView
        filters={TableFilters.fromString(filters)}
        onFiltersChange={viewFilterChange('segments')}
        goToQuery={this.goToQuery}
        capabilities={capabilities}
      />,
    );
  };

  private readonly wrappedSupervisorsView = ({ filters }: FiltersRouteProps) => {
    const { capabilities } = this.state;
    return this.wrapInViewContainer(
      'supervisors',
      <SupervisorsView
        filters={TableFilters.fromString(filters)}
        onFiltersChange={viewFilterChange('supervisors')}
        openSupervisorDialog={this.openSupervisorDialog}
        goToView={goToView}
        goToQuery={this.goToQuery}
        goToStreamingDataLoader={this.goToStreamingDataLoader}
        capabilities={capabilities}
      />,
    );
  };

  private readonly wrappedTasksView = ({ filters }: FiltersRouteProps) => {
    const { capabilities } = this.state;
    return this.wrapInViewContainer(
      'tasks',
      <TasksView
        filters={TableFilters.fromString(filters)}
        onFiltersChange={viewFilterChange('tasks')}
        openTaskDialog={this.openTaskDialog}
        goToView={goToView}
        goToQuery={this.goToQuery}
        goToClassicBatchDataLoader={this.goToClassicBatchDataLoader}
        capabilities={capabilities}
      />,
    );
  };

  private readonly wrappedServicesView = ({ filters }: FiltersRouteProps) => {
    const { capabilities } = this.state;
    return this.wrapInViewContainer(
      'services',
      <ServicesView
        filters={TableFilters.fromString(filters)}
        onFiltersChange={viewFilterChange('services')}
        goToQuery={this.goToQuery}
        capabilities={capabilities}
      />,
    );
  };

  private readonly wrappedLookupsView = ({ filters }: FiltersRouteProps) => {
    return this.wrapInViewContainer(
      'lookups',
      <LookupsView
        filters={TableFilters.fromString(filters)}
        onFiltersChange={viewFilterChange('lookups')}
      />,
    );
  };

  private readonly wrappedExploreView = () => {
    const { capabilities } = this.state;
    return <ExploreView capabilities={capabilities} />;
  };

  private renderView() {
    const { capabilities, hashPath } = this.state;
    const { view, param } = parseHashRoute(hashPath);

    switch (view) {
      case 'data-loader':
        if (capabilities.hasCoordinatorAccess()) return <this.wrappedDataLoaderView />;
        break;

      case 'streaming-data-loader':
        if (capabilities.hasCoordinatorAccess()) return <this.wrappedStreamingDataLoaderView />;
        break;

      case 'classic-batch-data-loader':
        if (capabilities.hasCoordinatorAccess()) return <this.wrappedClassicBatchDataLoaderView />;
        break;

      case 'sql-data-loader':
        if (capabilities.hasCoordinatorAccess() && capabilities.hasMultiStageQueryTask()) {
          return <this.wrappedSqlDataLoaderView />;
        }
        break;

      case 'supervisors':
        return <this.wrappedSupervisorsView filters={param} />;

      case 'tasks':
        return <this.wrappedTasksView filters={param} />;

      case 'datasources':
        return <this.wrappedDatasourcesView filters={param} />;

      case 'segments':
        return <this.wrappedSegmentsView filters={param} />;

      case 'services':
        return <this.wrappedServicesView filters={param} />;

      case 'workbench':
        return <this.wrappedWorkbenchView tabId={param} />;

      case 'lookups':
        if (capabilities.hasCoordinatorAccess()) return <this.wrappedLookupsView filters={param} />;
        break;

      case 'explore':
        if (capabilities.hasSql()) return <this.wrappedExploreView />;
        break;
    }

    return <this.wrappedHomeView />;
  }

  render() {
    const { availableSqlFunctions, capabilitiesLoading } = this.state;

    if (capabilitiesLoading) {
      return (
        <div className="loading-capabilities">
          <Loader />
        </div>
      );
    }

    return (
      <HotkeysProvider>
        <SqlFunctionsProvider availableSqlFunctions={availableSqlFunctions}>
          <div className="console-application">{this.renderView()}</div>
        </SqlFunctionsProvider>
      </HotkeysProvider>
    );
  }
}
