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

import {
  Button,
  ButtonGroup,
  Intent,
  Menu,
  MenuDivider,
  MenuItem,
  Popover,
} from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import classNames from 'classnames';
import copy from 'copy-to-clipboard';
import type { SqlQuery } from 'druid-query-toolkit';
import { SqlExpression } from 'druid-query-toolkit';
import React from 'react';

import { MenuCheckbox, SplitterLayout } from '../../components';
import { SpecDialog, StringInputDialog } from '../../dialogs';
import type {
  CapacityInfo,
  DruidEngine,
  Execution,
  QueryContext,
  QueryWithContext,
  TabEntry,
} from '../../druid-models';
import {
  DEFAULT_SERVER_QUERY_CONTEXT,
  guessDataSourceNameFromInputSource,
  WorkbenchQuery,
} from '../../druid-models';
import type { Capabilities } from '../../helpers';
import { convertSpecToSql, getSpecDatasourceName, getTaskExecution } from '../../helpers';
import { getLink } from '../../links';
import { AppToaster } from '../../singletons';
import { AceEditorStateCache } from '../../singletons/ace-editor-state-cache';
import { ExecutionStateCache } from '../../singletons/execution-state-cache';
import { WorkbenchRunningPromises } from '../../singletons/workbench-running-promises';
import type { ColumnMetadata } from '../../utils';
import {
  deepSet,
  generate8HexId,
  localStorageGet,
  localStorageGetJson,
  LocalStorageKeys,
  localStorageSet,
  localStorageSetJson,
  queryDruidSql,
  QueryManager,
  QueryState,
} from '../../utils';

import { ColumnTree } from './column-tree/column-tree';
import { ConnectExternalDataDialog } from './connect-external-data-dialog/connect-external-data-dialog';
import { CurrentDartPanel } from './current-dart-panel/current-dart-panel';
import { getDemoQueries } from './demo-queries';
import { ExecutionDetailsDialog } from './execution-details-dialog/execution-details-dialog';
import type { ExecutionDetailsTab } from './execution-details-pane/execution-details-pane';
import { ExecutionSubmitDialog } from './execution-submit-dialog/execution-submit-dialog';
import { ExplainDialog } from './explain-dialog/explain-dialog';
import { MetadataChangeDetector } from './metadata-change-detector';
import type { QueryTabProps } from './query-tab/query-tab';
import { QueryTab } from './query-tab/query-tab';
import { RecentQueryTaskPanel } from './recent-query-task-panel/recent-query-task-panel';
import { TabRenameDialog } from './tab-rename-dialog/tab-rename-dialog';
import { WorkbenchHistoryDialog } from './workbench-history-dialog/workbench-history-dialog';

import './workbench-view.scss';

const LAST_DAY = SqlExpression.parse(`__time >= CURRENT_TIMESTAMP - INTERVAL '1' DAY`);

function handleLeftPaneSizeChange(paneSize: number) {
  localStorageSetJson(LocalStorageKeys.WORKBENCH_LEFT_SIZE, paneSize);
}

function handleRightPaneSizeChange(paneSize: number) {
  localStorageSetJson(LocalStorageKeys.WORKBENCH_RIGHT_SIZE, paneSize);
}

function cleanupTabEntry(tabEntry: TabEntry): void {
  const discardedId = tabEntry.id;
  WorkbenchRunningPromises.deletePromise(discardedId);
  ExecutionStateCache.deleteState(discardedId);
  AceEditorStateCache.deleteState(discardedId);
}

function externalDataTabId(tabId: string | undefined): boolean {
  return String(tabId).startsWith('connect-external-data');
}

type MoreMenuItem =
  | 'explain'
  | 'history'
  | 'prettify'
  | 'convert-ingestion-to-sql'
  | 'attach-tab-from-task-id'
  | 'open-query-detail-archive'
  | 'druid-sql-documentation'
  | 'load-demo-queries';

export interface WorkbenchViewProps
  extends Pick<
    QueryTabProps,
    'maxTasksMenuHeader' | 'enginesLabelFn' | 'maxTasksLabelFn' | 'fullClusterCapacityLabelFn'
  > {
  capabilities: Capabilities;
  tabId: string | undefined;
  onTabChange(newTabId: string): void;
  initQueryWithContext: QueryWithContext | undefined;
  baseQueryContext?: QueryContext;
  defaultQueryContext?: QueryContext;
  mandatoryQueryContext?: QueryContext;
  serverQueryContext?: QueryContext;
  queryEngines: DruidEngine[];
  hiddenMoreMenuItems?: MoreMenuItem[] | ((engine: DruidEngine) => MoreMenuItem[]);
  goToTask(taskId: string): void;
  getClusterCapacity: (() => Promise<CapacityInfo | undefined>) | undefined;
  hideToolbar?: boolean;
  maxTasksOptions?:
    | QueryTabProps['maxTasksOptions']
    | ((engine: DruidEngine) => QueryTabProps['maxTasksOptions']);
  hiddenOptions?:
    | QueryTabProps['hiddenOptions']
    | ((engine: DruidEngine) => QueryTabProps['hiddenOptions']);
}

export interface WorkbenchViewState {
  tabEntries: TabEntry[];

  columnMetadataState: QueryState<readonly ColumnMetadata[]>;

  details?: { id: string; initTab?: ExecutionDetailsTab; initExecution?: Execution };

  connectExternalDataDialogOpen: boolean;
  explainDialogOpen: boolean;
  historyDialogOpen: boolean;
  specDialogOpen: boolean;
  executionSubmitDialogOpen: boolean;
  taskIdSubmitDialogOpen: boolean;
  renamingTab?: TabEntry;

  showRecentQueryTaskPanel: boolean;
  showCurrentDartPanel: boolean;
}

export class WorkbenchView extends React.PureComponent<WorkbenchViewProps, WorkbenchViewState> {
  private metadataQueryManager: QueryManager<null, ColumnMetadata[]> | undefined;

  constructor(props: WorkbenchViewProps) {
    super(props);
    const { queryEngines, initQueryWithContext } = props;

    const possibleTabEntries: TabEntry[] = localStorageGetJson(LocalStorageKeys.WORKBENCH_QUERIES);

    WorkbenchQuery.setQueryEngines(queryEngines);

    const hasSqlTask = queryEngines.includes('sql-msq-task');
    const showRecentQueryTaskPanel = Boolean(
      hasSqlTask && localStorageGetJson(LocalStorageKeys.WORKBENCH_TASK_PANEL),
    );

    const showCurrentDartPanel = Boolean(
      queryEngines.includes('sql-msq-dart') &&
        localStorageGetJson(LocalStorageKeys.WORKBENCH_DART_PANEL),
    );

    const tabEntries =
      Array.isArray(possibleTabEntries) && possibleTabEntries.length
        ? possibleTabEntries.map(q => ({ ...q, query: new WorkbenchQuery(q.query) }))
        : [];

    if (initQueryWithContext) {
      // Put it in the front so that it is the opened tab
      tabEntries.unshift({
        id: generate8HexId(),
        tabName: 'Opened query',
        query: WorkbenchQuery.blank()
          .changeQueryString(initQueryWithContext.queryString)
          .changeQueryContext(initQueryWithContext.queryContext || props.defaultQueryContext || {}),
      });
    }

    if (!tabEntries.length) {
      tabEntries.push(this.getInitTab());
    }

    this.state = {
      tabEntries,
      columnMetadataState: QueryState.INIT,

      connectExternalDataDialogOpen: externalDataTabId(props.tabId) && hasSqlTask,
      explainDialogOpen: false,
      historyDialogOpen: false,
      specDialogOpen: false,
      executionSubmitDialogOpen: false,
      taskIdSubmitDialogOpen: false,

      showRecentQueryTaskPanel,
      showCurrentDartPanel,
    };
  }

  componentDidMount(): void {
    this.metadataQueryManager = new QueryManager({
      processQuery: async (_, cancelToken) => {
        return await queryDruidSql<ColumnMetadata>(
          {
            query: `SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS`,
          },
          cancelToken,
        );
      },
      onStateChange: columnMetadataState => {
        if (columnMetadataState.error) {
          AppToaster.show({
            message: 'Could not load SQL metadata',
            intent: Intent.DANGER,
          });
        }
        this.setState({
          columnMetadataState,
        });
      },
    });

    this.metadataQueryManager.runQuery(null);
  }

  componentWillUnmount(): void {
    this.metadataQueryManager?.terminate();
  }

  componentDidUpdate(prevProps: Readonly<WorkbenchViewProps>) {
    const tabId = this.props.tabId;
    if (
      prevProps.tabId !== tabId &&
      externalDataTabId(tabId) &&
      WorkbenchQuery.getQueryEngines().includes('sql-msq-task')
    ) {
      this.setState({ connectExternalDataDialogOpen: true });
    }
  }

  private readonly openExplainDialog = () => {
    this.setState({ explainDialogOpen: true });
  };

  private readonly openHistoryDialog = () => {
    this.setState({ historyDialogOpen: true });
  };

  private readonly openSpecDialog = () => {
    this.setState({ specDialogOpen: true });
  };

  private readonly openExecutionSubmitDialog = () => {
    this.setState({ executionSubmitDialogOpen: true });
  };

  private readonly openTaskIdSubmitDialog = () => {
    this.setState({ taskIdSubmitDialogOpen: true });
  };

  private readonly handleRecentQueryTaskPanelClose = () => {
    this.setState({ showRecentQueryTaskPanel: false });
    localStorageSetJson(LocalStorageKeys.WORKBENCH_TASK_PANEL, false);
  };

  private readonly handleCurrentDartPanelClose = () => {
    this.setState({ showCurrentDartPanel: false });
    localStorageSetJson(LocalStorageKeys.WORKBENCH_DART_PANEL, false);
  };

  private readonly handleDetailsWithId = (id: string, initTab?: ExecutionDetailsTab) => {
    this.setState({
      details: { id, initTab },
    });
  };

  private getInitWorkbenchQuery(): WorkbenchQuery {
    return WorkbenchQuery.blank().changeQueryContext(this.props.defaultQueryContext || {});
  }

  private readonly handleDetailsWithExecution = (
    execution: Execution,
    initTab?: ExecutionDetailsTab,
  ) => {
    this.setState({
      details: { id: execution.id, initExecution: execution, initTab },
    });
  };

  private getInitTab(): TabEntry {
    return {
      id: generate8HexId(),
      tabName: 'Tab 1',
      query: this.getInitWorkbenchQuery(),
    };
  }

  private getTabId(): string | undefined {
    const { tabId, initQueryWithContext } = this.props;
    if (tabId) return tabId;
    if (initQueryWithContext) return; // If initialized from a query go to the first tab, forget about the last opened tab
    return localStorageGet(LocalStorageKeys.WORKBENCH_LAST_TAB);
  }

  private getCurrentTabEntry() {
    const { tabEntries } = this.state;
    const tabId = this.getTabId();
    return tabEntries.find(({ id }) => id === tabId) || tabEntries[0];
  }

  private getCurrentQuery() {
    return this.getCurrentTabEntry().query;
  }

  private renderExecutionDetailsDialog() {
    const { goToTask } = this.props;
    const { details } = this.state;
    if (!details) return;

    return (
      <ExecutionDetailsDialog
        id={details.id}
        initTab={details.initTab}
        initExecution={details.initExecution}
        goToTask={goToTask}
        onClose={() => this.setState({ details: undefined })}
      />
    );
  }

  private renderExplainDialog() {
    const { queryEngines } = this.props;
    const { explainDialogOpen } = this.state;
    if (!explainDialogOpen) return;

    const query = this.getCurrentQuery();

    const { engine, query: apiQuery } = query.getApiQuery();
    if (typeof apiQuery.query !== 'string') return;

    return (
      <ExplainDialog
        queryWithContext={{
          engine,
          queryString: apiQuery.query,
          queryContext: apiQuery.context,
        }}
        mandatoryQueryContext={{}}
        onClose={() => {
          this.setState({ explainDialogOpen: false });
        }}
        openQueryLabel={
          engine === 'sql-native' && queryEngines.includes('native') ? 'Open in new tab' : undefined
        }
        onOpenQuery={queryString => {
          this.handleNewTab(
            WorkbenchQuery.blank().changeQueryString(queryString),
            'Explained query',
          );
        }}
      />
    );
  }

  private renderHistoryDialog() {
    const { historyDialogOpen } = this.state;
    if (!historyDialogOpen) return;

    return (
      <WorkbenchHistoryDialog
        onSelectQuery={query => this.handleNewTab(query, 'Query from history')}
        onClose={() => this.setState({ historyDialogOpen: false })}
      />
    );
  }

  private renderConnectExternalDataDialog() {
    const { connectExternalDataDialogOpen } = this.state;
    if (!connectExternalDataDialogOpen) return;

    return (
      <ConnectExternalDataDialog
        onSetExternalConfig={(externalConfig, timeExpression, partitionedByHint, arrayMode) => {
          this.handleNewTab(
            WorkbenchQuery.fromInitExternalConfig(
              externalConfig,
              timeExpression,
              partitionedByHint,
              arrayMode,
            ),
            'Ext ' + guessDataSourceNameFromInputSource(externalConfig.inputSource),
          );
        }}
        onClose={() => {
          this.setState({ connectExternalDataDialogOpen: false });
        }}
      />
    );
  }

  private renderTabRenameDialog() {
    const { renamingTab } = this.state;
    if (!renamingTab) return;

    return (
      <TabRenameDialog
        initialTabName={renamingTab.tabName}
        onSave={newTabName => {
          const { tabEntries } = this.state;
          if (!renamingTab) return;
          this.handleQueriesChange(
            tabEntries.map(tabEntry =>
              tabEntry.id === renamingTab.id ? { ...renamingTab, tabName: newTabName } : tabEntry,
            ),
          );
        }}
        onClose={() => this.setState({ renamingTab: undefined })}
      />
    );
  }

  private renderSpecDialog() {
    const { specDialogOpen } = this.state;
    if (!specDialogOpen) return;

    return (
      <SpecDialog
        onSubmit={spec => {
          let converted: QueryWithContext;
          try {
            converted = convertSpecToSql(spec as any);
          } catch (e) {
            AppToaster.show({
              message: `Could not convert spec: ${e.message}`,
              intent: Intent.DANGER,
            });
            return;
          }

          AppToaster.show({
            message: `Spec converted, please double check`,
            intent: Intent.SUCCESS,
          });
          this.handleNewTab(
            WorkbenchQuery.blank()
              .changeQueryString(converted.queryString)
              .changeQueryContext(converted.queryContext || {}),
            `Convert ${getSpecDatasourceName(spec as any) || 'spec'}`,
          );
        }}
        onClose={() => this.setState({ specDialogOpen: false })}
        title="Ingestion spec to convert"
      />
    );
  }

  private renderExecutionSubmit() {
    const { executionSubmitDialogOpen } = this.state;
    if (!executionSubmitDialogOpen) return;

    return (
      <ExecutionSubmitDialog
        onSubmit={execution => {
          this.setState({
            details: {
              id: execution.id,
              initExecution: execution,
            },
          });
        }}
        onClose={() => this.setState({ executionSubmitDialogOpen: false })}
      />
    );
  }

  private renderTaskIdSubmit() {
    const { taskIdSubmitDialogOpen } = this.state;
    if (!taskIdSubmitDialogOpen) return;

    return (
      <StringInputDialog
        title="Enter task ID"
        placeholder="taskId"
        // eslint-disable-next-line @typescript-eslint/no-misused-promises
        onSubmit={async taskId => {
          let execution: Execution;
          try {
            execution = await getTaskExecution(taskId);
          } catch {
            AppToaster.show({
              message: 'Could not get task report or payload',
              intent: Intent.DANGER,
            });
            return;
          }

          if (!execution.sqlQuery || !execution.queryContext) {
            AppToaster.show({
              message: 'Could not get query',
              intent: Intent.DANGER,
            });
            return;
          }

          this.handleNewTab(
            WorkbenchQuery.fromTaskQueryAndContext(
              execution.sqlQuery,
              execution.queryContext,
            ).changeLastExecution({ engine: 'sql-msq-task', id: taskId }),
            taskId,
          );
        }}
        onClose={() => this.setState({ taskIdSubmitDialogOpen: false })}
      />
    );
  }

  private renderTabBar() {
    const { onTabChange } = this.props;
    const { tabEntries } = this.state;
    const currentTabEntry = this.getCurrentTabEntry();

    return (
      <div className="tab-bar">
        {tabEntries.map((tabEntry, i) => {
          const currentId = tabEntry.id;
          const active = currentTabEntry === tabEntry;
          return (
            <div
              key={i}
              className={classNames('tab-button', { active })}
              data-tooltip={tabEntry.tabName}
            >
              {active ? (
                <Popover
                  position="bottom"
                  content={
                    <Menu>
                      <MenuItem
                        icon={IconNames.EDIT}
                        text="Rename tab"
                        onClick={() => this.setState({ renamingTab: tabEntry })}
                      />
                      <MenuItem
                        icon={IconNames.CLIPBOARD}
                        text="Copy tab"
                        onClick={() => {
                          copy(currentTabEntry.query.toString(), { format: 'text/plain' });
                          AppToaster.show({
                            message: `Tab '${currentTabEntry.tabName}' copied to clipboard.`,
                            intent: Intent.SUCCESS,
                          });
                        }}
                      />
                      <MenuItem
                        icon={IconNames.DUPLICATE}
                        text="Duplicate tab"
                        onClick={() => {
                          const id = generate8HexId();
                          const newTabEntry: TabEntry = {
                            id,
                            tabName: tabEntry.tabName + ' (copy)',
                            query: tabEntry.query.changeLastExecution(undefined),
                          };
                          this.handleQueriesChange(
                            tabEntries.slice(0, i + 1).concat(newTabEntry, tabEntries.slice(i + 1)),
                            () => {
                              onTabChange(newTabEntry.id);
                            },
                          );
                        }}
                      />
                      {tabEntries.length > 1 && (
                        <MenuItem
                          icon={IconNames.CROSS}
                          text="Close other tabs"
                          intent={Intent.DANGER}
                          onClick={() => {
                            tabEntries.forEach(tabEntry => {
                              if (tabEntry.id === currentId) return;
                              cleanupTabEntry(tabEntry);
                            });
                            this.handleQueriesChange(
                              tabEntries.filter(({ id }) => id === currentId),
                              () => {
                                if (!active) return;
                                onTabChange(tabEntry.id);
                              },
                            );
                          }}
                        />
                      )}
                      <MenuItem
                        icon={IconNames.CROSS}
                        text="Close all tabs"
                        intent={Intent.DANGER}
                        onClick={() => {
                          tabEntries.forEach(cleanupTabEntry);
                          this.handleQueriesChange([], () => {
                            onTabChange(this.state.tabEntries[0].id);
                          });
                        }}
                      />
                    </Menu>
                  }
                >
                  <Button
                    className="tab-name"
                    text={tabEntry.tabName}
                    minimal
                    onDoubleClick={() => this.setState({ renamingTab: tabEntry })}
                  />
                </Popover>
              ) : (
                <Button
                  className="tab-name"
                  text={tabEntry.tabName}
                  minimal
                  onClick={() => {
                    localStorageSet(LocalStorageKeys.WORKBENCH_LAST_TAB, currentId);
                    onTabChange(currentId);
                  }}
                />
              )}
              <Button
                className="tab-close"
                icon={IconNames.CROSS}
                data-tooltip={`Close tab: ${tabEntry.tabName}`}
                small
                minimal
                onClick={() => {
                  cleanupTabEntry(tabEntry);
                  this.handleQueriesChange(
                    tabEntries.filter(({ id }) => id !== currentId),
                    () => {
                      if (!active) return;
                      onTabChange(this.state.tabEntries[Math.max(0, i - 1)].id);
                    },
                  );
                }}
              />
            </div>
          );
        })}
        <Button
          className="new-tab"
          icon={IconNames.PLUS}
          data-tooltip="New tab"
          minimal
          onClick={() => {
            this.handleNewTab(this.getInitWorkbenchQuery());
          }}
        />
      </div>
    );
  }

  private renderToolbar() {
    const { queryEngines, hideToolbar } = this.props;
    if (!queryEngines.includes('sql-msq-task')) return;
    if (hideToolbar) return;

    const { showRecentQueryTaskPanel, showCurrentDartPanel } = this.state;
    return (
      <ButtonGroup className="toolbar">
        <Button
          icon={IconNames.TH_DERIVED}
          text="Connect external data"
          onClick={() => {
            this.setState({
              connectExternalDataDialogOpen: true,
            });
          }}
          minimal
        />
        <Popover
          position="bottom-right"
          content={
            <Menu>
              <MenuCheckbox
                text="Recent query task panel"
                checked={showRecentQueryTaskPanel}
                shouldDismissPopover
                onChange={() => {
                  const n = !showRecentQueryTaskPanel;
                  this.setState({ showRecentQueryTaskPanel: n });
                  localStorageSetJson(LocalStorageKeys.WORKBENCH_TASK_PANEL, n);
                }}
              />
              <MenuCheckbox
                text="Current Dart query panel"
                checked={showCurrentDartPanel}
                shouldDismissPopover
                onChange={() => {
                  const n = !showCurrentDartPanel;
                  this.setState({ showCurrentDartPanel: n });
                  localStorageSetJson(LocalStorageKeys.WORKBENCH_DART_PANEL, n);
                }}
              />
            </Menu>
          }
        >
          <Button icon={IconNames.DRAWER_RIGHT} minimal data-tooltip="Open helper panels" />
        </Popover>
      </ButtonGroup>
    );
  }

  private renderCenterPanel() {
    const {
      capabilities,
      mandatoryQueryContext,
      baseQueryContext,
      serverQueryContext = DEFAULT_SERVER_QUERY_CONTEXT,
      queryEngines,
      goToTask,
      getClusterCapacity,
      maxTasksMenuHeader,
      enginesLabelFn,
      maxTasksLabelFn,
      maxTasksOptions,
      fullClusterCapacityLabelFn,
      hiddenOptions,
    } = this.props;
    const { columnMetadataState } = this.state;
    const currentTabEntry = this.getCurrentTabEntry();
    const effectiveEngine = currentTabEntry.query.getEffectiveEngine();

    const hiddenMoreMenuItems =
      typeof this.props.hiddenMoreMenuItems === 'function'
        ? this.props.hiddenMoreMenuItems(effectiveEngine)
        : this.props.hiddenMoreMenuItems || [];

    return (
      <div className="center-panel">
        <div className="tab-and-tool-bar">
          {this.renderTabBar()}
          {this.renderToolbar()}
        </div>
        <QueryTab
          key={currentTabEntry.id}
          query={currentTabEntry.query}
          id={currentTabEntry.id}
          mandatoryQueryContext={mandatoryQueryContext}
          baseQueryContext={baseQueryContext}
          serverQueryContext={serverQueryContext}
          columnMetadata={columnMetadataState.getSomeData()}
          onQueryChange={this.handleQueryChange}
          onQueryTab={this.handleNewTab}
          onDetails={this.handleDetailsWithExecution}
          queryEngines={queryEngines}
          clusterCapacity={capabilities.getMaxTaskSlots()}
          goToTask={goToTask}
          getClusterCapacity={getClusterCapacity}
          maxTasksMenuHeader={maxTasksMenuHeader}
          enginesLabelFn={enginesLabelFn}
          maxTasksLabelFn={maxTasksLabelFn}
          maxTasksOptions={
            typeof maxTasksOptions === 'function'
              ? maxTasksOptions(effectiveEngine)
              : maxTasksOptions
          }
          fullClusterCapacityLabelFn={fullClusterCapacityLabelFn}
          hiddenOptions={
            typeof hiddenOptions === 'function' ? hiddenOptions(effectiveEngine) : hiddenOptions
          }
          runMoreMenu={
            <Menu>
              {!hiddenMoreMenuItems.includes('explain') &&
                (effectiveEngine === 'sql-native' ||
                  effectiveEngine === 'sql-msq-task' ||
                  effectiveEngine === 'sql-msq-dart') && (
                  <MenuItem
                    icon={IconNames.CLEAN}
                    text="Explain SQL query"
                    onClick={this.openExplainDialog}
                  />
                )}
              {effectiveEngine !== 'sql-msq-task' && !hiddenMoreMenuItems.includes('history') && (
                <MenuItem
                  icon={IconNames.HISTORY}
                  text="Query history"
                  onClick={this.openHistoryDialog}
                />
              )}
              {currentTabEntry.query.canPrettify() && !hiddenMoreMenuItems.includes('prettify') && (
                <MenuItem
                  icon={IconNames.ALIGN_LEFT}
                  text="Prettify query"
                  onClick={() => this.handleQueryChange(currentTabEntry.query.prettify())}
                />
              )}
              {queryEngines.includes('sql-msq-task') && (
                <>
                  {!hiddenMoreMenuItems.includes('convert-ingestion-to-sql') && (
                    <MenuItem
                      icon={IconNames.TEXT_HIGHLIGHT}
                      text="Convert ingestion spec to SQL"
                      onClick={this.openSpecDialog}
                    />
                  )}
                  {!hiddenMoreMenuItems.includes('attach-tab-from-task-id') && (
                    <MenuItem
                      icon={IconNames.DOCUMENT_OPEN}
                      text="Attach tab from task ID"
                      onClick={this.openTaskIdSubmitDialog}
                    />
                  )}
                  {!hiddenMoreMenuItems.includes('open-query-detail-archive') && (
                    <MenuItem
                      icon={IconNames.UNARCHIVE}
                      text="Open query detail archive"
                      onClick={this.openExecutionSubmitDialog}
                    />
                  )}
                </>
              )}
              <MenuDivider />
              {!hiddenMoreMenuItems.includes('druid-sql-documentation') && (
                <MenuItem
                  icon={IconNames.HELP}
                  text="DruidSQL documentation"
                  href={getLink('DOCS_SQL')}
                  target="_blank"
                />
              )}
              {queryEngines.includes('sql-msq-task') &&
                !hiddenMoreMenuItems.includes('load-demo-queries') && (
                  <MenuItem
                    icon={IconNames.ROCKET_SLANT}
                    text="Load demo queries"
                    label="(replaces current tabs)"
                    onClick={() => this.handleQueriesChange(getDemoQueries())}
                  />
                )}
            </Menu>
          }
        />
      </div>
    );
  }

  private readonly handleQueriesChange = (tabEntries: TabEntry[], callback?: () => void) => {
    if (!tabEntries.length) {
      tabEntries.push(this.getInitTab());
    }
    localStorageSetJson(LocalStorageKeys.WORKBENCH_QUERIES, tabEntries);
    this.setState({ tabEntries }, callback);
  };

  private readonly handleQueryChange = (newQuery: WorkbenchQuery) => {
    const { tabEntries } = this.state;
    const tabId = this.getTabId();
    const tabIndex = Math.max(
      tabEntries.findIndex(({ id }) => id === tabId),
      0,
    );
    const newQueries = deepSet(tabEntries, `${tabIndex}.query`, newQuery);
    this.handleQueriesChange(newQueries);
  };

  private readonly handleQueryStringChange = (queryString: string): void => {
    this.handleQueryChange(this.getCurrentQuery().changeQueryString(queryString));
  };

  private readonly handleSqlQueryChange = (sqlQuery: SqlQuery): void => {
    this.handleQueryStringChange(sqlQuery.toString());
  };

  private readonly getParsedQuery = () => {
    return this.getCurrentQuery().getParsedQuery();
  };

  private readonly handleNewTab = (query: WorkbenchQuery, tabName?: string) => {
    const { onTabChange } = this.props;
    const { tabEntries } = this.state;
    const id = generate8HexId();
    const newTabEntry: TabEntry = {
      id,
      tabName: tabName || `Tab ${tabEntries.length + 1}`,
      query,
    };
    this.handleQueriesChange(tabEntries.concat(newTabEntry), () => {
      onTabChange(newTabEntry.id);
    });
  };

  render() {
    const { columnMetadataState, showRecentQueryTaskPanel, showCurrentDartPanel } = this.state;
    const query = this.getCurrentQuery();

    let defaultSchema: string | undefined;
    let defaultTables: string[] | undefined;
    const parsedQuery = query.getParsedQuery();
    if (parsedQuery) {
      defaultSchema = parsedQuery.getFirstSchema();
      defaultTables = parsedQuery.getUsedTableNames();
    }

    const showRightPanel = showRecentQueryTaskPanel || showCurrentDartPanel;
    return (
      <div className="workbench-view app-view">
        <SplitterLayout
          className="workbench-splitter"
          primaryIndex={0}
          secondaryMinSize={250}
          secondaryMaxSize={500}
          secondaryInitialSize={
            Number(localStorageGetJson(LocalStorageKeys.WORKBENCH_RIGHT_SIZE)) || 250
          }
          onSecondaryPaneSizeChange={handleRightPaneSizeChange}
          primaryMinSize={600}
        >
          <SplitterLayout
            className="workbench-splitter"
            primaryIndex={1}
            secondaryMinSize={250}
            secondaryMaxSize={500}
            secondaryInitialSize={
              Number(localStorageGetJson(LocalStorageKeys.WORKBENCH_LEFT_SIZE)) || 250
            }
            onSecondaryPaneSizeChange={handleLeftPaneSizeChange}
            primaryMinSize={400}
          >
            {!columnMetadataState.isError() && (
              <ColumnTree
                getParsedQuery={this.getParsedQuery}
                columnMetadata={columnMetadataState.getSomeData()}
                columnMetadataLoading={columnMetadataState.loading}
                onQueryChange={this.handleSqlQueryChange}
                defaultSchema={defaultSchema ? defaultSchema : 'druid'}
                defaultTables={defaultTables}
                defaultWhere={LAST_DAY}
                highlightTable={undefined}
              />
            )}
            {this.renderCenterPanel()}
          </SplitterLayout>
          {showRightPanel && (
            <div className="recent-panel">
              {showRecentQueryTaskPanel && (
                <RecentQueryTaskPanel
                  onClose={this.handleRecentQueryTaskPanelClose}
                  onExecutionDetails={this.handleDetailsWithId}
                  onChangeQuery={this.handleQueryStringChange}
                  onNewTab={this.handleNewTab}
                />
              )}
              {showCurrentDartPanel && (
                <CurrentDartPanel onClose={this.handleCurrentDartPanelClose} />
              )}
            </div>
          )}
        </SplitterLayout>

        {this.renderExecutionDetailsDialog()}
        {this.renderExplainDialog()}
        {this.renderHistoryDialog()}
        {this.renderConnectExternalDataDialog()}
        {this.renderTabRenameDialog()}
        {this.renderSpecDialog()}
        {this.renderExecutionSubmit()}
        {this.renderTaskIdSubmit()}
        <MetadataChangeDetector onChange={() => this.metadataQueryManager?.runQuery(null)} />
      </div>
    );
  }
}
