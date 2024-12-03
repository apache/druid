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
  Icon,
  Intent,
  Menu,
  MenuDivider,
  MenuItem,
  Popover,
  Position,
  Tag,
  useHotkeys,
} from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import type { JSX } from 'react';
import React, { useCallback, useMemo, useState } from 'react';

import { ENABLE_DISABLE_OPTIONS_TEXT, MenuBoolean, MenuCheckbox } from '../../../components';
import { EditContextDialog, StringInputDialog } from '../../../dialogs';
import { IndexSpecDialog } from '../../../dialogs/index-spec-dialog/index-spec-dialog';
import type {
  ArrayIngestMode,
  DruidEngine,
  IndexSpec,
  QueryContext,
  SelectDestination,
  SqlJoinAlgorithm,
  WorkbenchQuery,
} from '../../../druid-models';
import { getQueryContextKey, summarizeIndexSpec } from '../../../druid-models';
import { getLink } from '../../../links';
import { deepGet, pluralIfNeeded, removeUndefinedValues, tickIcon } from '../../../utils';
import type { MaxTasksButtonProps } from '../max-tasks-button/max-tasks-button';
import { MaxTasksButton } from '../max-tasks-button/max-tasks-button';
import { QueryParametersDialog } from '../query-parameters-dialog/query-parameters-dialog';
import { TimezoneMenuItems } from '../timezone-menu-items/timezone-menu-items';

import './run-panel.scss';

const ARRAY_INGEST_MODE_LABEL: Record<ArrayIngestMode, string> = {
  array: 'Array',
  mvd: 'MVD',
};
const ARRAY_INGEST_MODE_DESCRIPTION: Record<ArrayIngestMode, JSX.Element> = {
  array: (
    <>
      Array: Load SQL <Tag minimal>VARCHAR ARRAY</Tag> as Druid{' '}
      <Tag minimal>ARRAY&lt;STRING&gt;</Tag>
    </>
  ),
  mvd: (
    <>
      MVD: Load SQL <Tag minimal>VARCHAR ARRAY</Tag> as Druid multi-value <Tag minimal>STRING</Tag>
    </>
  ),
};

const SQL_JOIN_ALGORITHM_LABEL: Record<SqlJoinAlgorithm, string> = {
  broadcast: 'Broadcast',
  sortMerge: 'Sort merge',
};

const DEFAULT_ENGINES_LABEL_FN = (engine: DruidEngine | undefined) => {
  if (!engine) return { text: 'Auto' };
  switch (engine) {
    case 'native':
      return { text: 'JSON (native)' };

    case 'sql-native':
      return { text: 'SQL (native)' };

    case 'sql-msq-task':
      return { text: 'SQL (task)', label: 'multi-stage-query' };

    case 'sql-msq-dart':
      return { text: 'SQL (Dart)', label: 'multi-stage-query' };

    default:
      return { text: engine };
  }
};

const SELECT_DESTINATION_LABEL: Record<SelectDestination, string> = {
  taskReport: 'Task report',
  durableStorage: 'Durable storage',
};

export type EnginesMenuOption =
  | 'edit-query-context'
  | 'define-parameters'
  | 'timezone'
  | 'insert-replace-specific-context'
  | 'max-parse-exceptions'
  | 'join-algorithm'
  | 'select-destination'
  | 'approximate-count-distinct'
  | 'finalize-aggregations'
  | 'group-by-enable-multi-value-unnesting'
  | 'durable-shuffle-storage'
  | 'use-cache'
  | 'approximate-top-n'
  | 'limit-inline-results';

function optionVisible(
  option: EnginesMenuOption,
  engine: DruidEngine,
  hiddenOptions: EnginesMenuOption[],
): boolean {
  if (hiddenOptions.includes(option)) return false;

  switch (option) {
    case 'edit-query-context':
    case 'define-parameters':
      return true;

    case 'insert-replace-specific-context':
    case 'max-parse-exceptions':
    case 'select-destination':
    case 'finalize-aggregations':
    case 'group-by-enable-multi-value-unnesting':
    case 'durable-shuffle-storage':
      return engine === 'sql-msq-task';

    case 'join-algorithm':
      return engine === 'sql-msq-task' || engine === 'sql-msq-dart';

    case 'timezone':
    case 'approximate-count-distinct':
      return engine === 'sql-native' || engine === 'sql-msq-task' || engine === 'sql-msq-dart';

    case 'use-cache':
      return engine === 'native' || engine === 'sql-native';

    case 'approximate-top-n':
      return engine === 'sql-native';

    case 'limit-inline-results':
      return engine === 'sql-native' || engine === 'sql-msq-dart';

    default:
      console.warn(`Unknown option: ${option}`);
      return false;
  }
}

export interface RunPanelProps
  extends Pick<
    MaxTasksButtonProps,
    'maxTasksLabelFn' | 'fullClusterCapacityLabelFn' | 'maxTasksOptions'
  > {
  query: WorkbenchQuery;
  onQueryChange(query: WorkbenchQuery): void;
  running: boolean;
  onRun(preview: boolean): void | Promise<void>;
  queryEngines: DruidEngine[];
  clusterCapacity: number | undefined;
  defaultQueryContext: QueryContext;
  moreMenu?: JSX.Element;
  maxTasksMenuHeader?: JSX.Element;
  enginesLabelFn?: (engine: DruidEngine | undefined) => { text: string; label?: string };
  hiddenOptions?: EnginesMenuOption[];
}

export const RunPanel = React.memo(function RunPanel(props: RunPanelProps) {
  const {
    query,
    onQueryChange,
    onRun,
    moreMenu,
    running,
    queryEngines,
    clusterCapacity,
    defaultQueryContext,
    maxTasksMenuHeader,
    enginesLabelFn = DEFAULT_ENGINES_LABEL_FN,
    maxTasksLabelFn,
    maxTasksOptions,
    fullClusterCapacityLabelFn,
    hiddenOptions = [],
  } = props;
  const [editContextDialogOpen, setEditContextDialogOpen] = useState(false);
  const [editParametersDialogOpen, setEditParametersDialogOpen] = useState(false);
  const [customTimezoneDialogOpen, setCustomTimezoneDialogOpen] = useState(false);
  const [indexSpecDialogSpec, setIndexSpecDialogSpec] = useState<IndexSpec | undefined>();

  const emptyQuery = query.isEmptyQuery();
  const ingestMode = query.isIngestQuery();
  const queryContext = query.queryContext;
  const numContextKeys = Object.keys(queryContext).length;
  const queryParameters = query.queryParameters;

  // Extract the context parts that have UI
  const sqlTimeZone = queryContext.sqlTimeZone;

  const useCache = getQueryContextKey('useCache', queryContext, defaultQueryContext);
  const useApproximateTopN = getQueryContextKey(
    'useApproximateTopN',
    queryContext,
    defaultQueryContext,
  );
  const useApproximateCountDistinct = getQueryContextKey(
    'useApproximateCountDistinct',
    queryContext,
    defaultQueryContext,
  );

  const arrayIngestMode = queryContext.arrayIngestMode;
  const maxParseExceptions = getQueryContextKey(
    'maxParseExceptions',
    queryContext,
    defaultQueryContext,
  );
  const failOnEmptyInsert = getQueryContextKey(
    'failOnEmptyInsert',
    queryContext,
    defaultQueryContext,
  );
  const finalizeAggregations = queryContext.finalizeAggregations;
  const waitUntilSegmentsLoad = queryContext.waitUntilSegmentsLoad;
  const groupByEnableMultiValueUnnesting = queryContext.groupByEnableMultiValueUnnesting;
  const sqlJoinAlgorithm = getQueryContextKey(
    'sqlJoinAlgorithm',
    queryContext,
    defaultQueryContext,
  );
  const selectDestination = getQueryContextKey(
    'selectDestination',
    queryContext,
    defaultQueryContext,
  );
  const durableShuffleStorage = getQueryContextKey(
    'durableShuffleStorage',
    queryContext,
    defaultQueryContext,
  );

  const indexSpec: IndexSpec | undefined = deepGet(queryContext, 'indexSpec');

  const handleRun = useCallback(() => {
    if (!onRun) return;
    void onRun(false);
  }, [onRun]);

  const handlePreview = useCallback(() => {
    if (!onRun) return;
    void onRun(true);
  }, [onRun]);

  const hotkeys = useMemo(() => {
    return [
      {
        allowInInput: true,
        global: true,
        group: 'Query',
        combo: 'mod + enter',
        label: 'Run the current query',
        onKeyDown: handleRun,
      },
      {
        allowInInput: true,
        global: true,
        group: 'Query',
        combo: 'mod + shift + enter',
        label: 'Preview the current query',
        onKeyDown: handlePreview,
      },
    ];
  }, [handleRun, handlePreview]);

  useHotkeys(hotkeys);

  const queryEngine = query.engine;

  function changeQueryContext(queryContext: QueryContext) {
    onQueryChange(query.changeQueryContext(removeUndefinedValues(queryContext)));
  }

  const overloadWarning =
    query.unlimited &&
    (queryEngine === 'sql-native' ||
      (queryEngine === 'sql-msq-task' && selectDestination === 'taskReport'));
  const intent = overloadWarning ? Intent.WARNING : undefined;

  const effectiveEngine = query.getEffectiveEngine();

  const autoEngineLabel = enginesLabelFn(undefined);

  function show(option: EnginesMenuOption) {
    return optionVisible(option, effectiveEngine, hiddenOptions);
  }

  return (
    <div className="run-panel">
      <Button
        className={effectiveEngine === 'native' ? 'rune-button' : undefined}
        disabled={running}
        icon={IconNames.CARET_RIGHT}
        onClick={() => void onRun(false)}
        text="Run"
        intent={!emptyQuery ? Intent.PRIMARY : undefined}
      />
      {ingestMode && (
        <Button
          disabled={running}
          icon={IconNames.EYE_OPEN}
          onClick={() => void onRun(true)}
          text="Preview"
        />
      )}
      {onQueryChange && (
        <ButtonGroup>
          <Popover
            position={Position.BOTTOM_LEFT}
            content={
              <Menu>
                {queryEngines.length > 1 && (
                  <>
                    <MenuDivider title="Select language and engine" />
                    <MenuItem
                      key="auto"
                      icon={tickIcon(queryEngine === undefined)}
                      text={autoEngineLabel.text}
                      label={autoEngineLabel.label}
                      onClick={() => onQueryChange(query.changeEngine(undefined))}
                      shouldDismissPopover={false}
                    />
                    {queryEngines.map(engine => {
                      const { text, label } = enginesLabelFn(engine);

                      return (
                        <MenuItem
                          key={String(engine)}
                          icon={tickIcon(engine === queryEngine)}
                          text={text}
                          label={label}
                          onClick={() => onQueryChange(query.changeEngine(engine))}
                          shouldDismissPopover={false}
                        />
                      );
                    })}

                    <MenuDivider />
                  </>
                )}
                {show('edit-query-context') && (
                  <MenuItem
                    icon={IconNames.PROPERTIES}
                    text="Edit query context..."
                    onClick={() => setEditContextDialogOpen(true)}
                    label={pluralIfNeeded(numContextKeys, 'key')}
                  />
                )}
                {show('define-parameters') && (
                  <MenuItem
                    icon={IconNames.HELP}
                    text="Define parameters..."
                    onClick={() => setEditParametersDialogOpen(true)}
                    label={
                      queryParameters ? pluralIfNeeded(queryParameters.length, 'parameter') : ''
                    }
                  />
                )}
                {show('timezone') && (
                  <MenuItem
                    icon={IconNames.GLOBE_NETWORK}
                    text="Timezone"
                    label={sqlTimeZone ?? defaultQueryContext.sqlTimeZone}
                  >
                    <TimezoneMenuItems
                      sqlTimeZone={sqlTimeZone}
                      setSqlTimeZone={sqlTimeZone =>
                        changeQueryContext({ ...queryContext, sqlTimeZone })
                      }
                      defaultSqlTimeZone={defaultQueryContext.sqlTimeZone}
                    />
                    <MenuItem
                      icon={IconNames.BLANK}
                      text="Custom"
                      onClick={() => setCustomTimezoneDialogOpen(true)}
                    />
                  </MenuItem>
                )}
                {show('approximate-count-distinct') && (
                  <MenuBoolean
                    icon={IconNames.ROCKET_SLANT}
                    text="Approximate COUNT(DISTINCT)"
                    value={useApproximateCountDistinct}
                    onValueChange={useApproximateCountDistinct =>
                      changeQueryContext({
                        ...queryContext,
                        useApproximateCountDistinct,
                      })
                    }
                    optionsText={ENABLE_DISABLE_OPTIONS_TEXT}
                  />
                )}
                {show('approximate-top-n') && (
                  <MenuBoolean
                    icon={IconNames.HORIZONTAL_BAR_CHART_DESC}
                    text="Approximate TopN"
                    value={useApproximateTopN}
                    onValueChange={useApproximateTopN =>
                      changeQueryContext({
                        ...queryContext,
                        useApproximateTopN,
                      })
                    }
                    optionsText={ENABLE_DISABLE_OPTIONS_TEXT}
                  />
                )}
                {show('join-algorithm') && (
                  <MenuItem
                    icon={IconNames.INNER_JOIN}
                    text="Join algorithm"
                    label={
                      SQL_JOIN_ALGORITHM_LABEL[sqlJoinAlgorithm as SqlJoinAlgorithm] ??
                      sqlJoinAlgorithm
                    }
                  >
                    {(['broadcast', 'sortMerge'] as SqlJoinAlgorithm[]).map(o => (
                      <MenuItem
                        key={o}
                        icon={tickIcon(sqlJoinAlgorithm === o)}
                        text={SQL_JOIN_ALGORITHM_LABEL[o]}
                        shouldDismissPopover={false}
                        onClick={() => changeQueryContext({ ...queryContext, sqlJoinAlgorithm: o })}
                      />
                    ))}
                  </MenuItem>
                )}

                {show('insert-replace-specific-context') && (
                  <MenuItem
                    icon={IconNames.BRING_DATA}
                    text="INSERT / REPLACE / EXTERN specific context"
                  >
                    <MenuBoolean
                      text="Fail on empty insert"
                      value={failOnEmptyInsert}
                      showUndefined
                      undefinedEffectiveValue={false}
                      onValueChange={failOnEmptyInsert =>
                        changeQueryContext({ ...queryContext, failOnEmptyInsert })
                      }
                      optionsText={ENABLE_DISABLE_OPTIONS_TEXT}
                    />
                    <MenuBoolean
                      text="Wait until segments have loaded"
                      value={waitUntilSegmentsLoad}
                      showUndefined
                      undefinedEffectiveValue={ingestMode}
                      onValueChange={waitUntilSegmentsLoad =>
                        changeQueryContext({ ...queryContext, waitUntilSegmentsLoad })
                      }
                      optionsText={ENABLE_DISABLE_OPTIONS_TEXT}
                    />
                    <MenuItem text="Max parse exceptions" label={String(maxParseExceptions)}>
                      {[0, 1, 5, 10, 1000, 10000, -1].map(v => (
                        <MenuItem
                          key={String(v)}
                          icon={tickIcon(v === maxParseExceptions)}
                          text={v === -1 ? '∞ (-1)' : String(v)}
                          onClick={() =>
                            changeQueryContext({ ...queryContext, maxParseExceptions: v })
                          }
                          shouldDismissPopover={false}
                        />
                      ))}
                    </MenuItem>
                    <MenuItem
                      text="Edit index spec..."
                      label={summarizeIndexSpec(indexSpec)}
                      shouldDismissPopover={false}
                      onClick={() => {
                        setIndexSpecDialogSpec(indexSpec || {});
                      }}
                    />
                  </MenuItem>
                )}

                {show('finalize-aggregations') && (
                  <MenuBoolean
                    icon={IconNames.TRANSLATE}
                    text="Finalize aggregations"
                    value={finalizeAggregations}
                    showUndefined
                    undefinedEffectiveValue={!ingestMode}
                    onValueChange={finalizeAggregations =>
                      changeQueryContext({ ...queryContext, finalizeAggregations })
                    }
                    optionsText={ENABLE_DISABLE_OPTIONS_TEXT}
                  />
                )}
                {show('group-by-enable-multi-value-unnesting') && (
                  <MenuBoolean
                    icon={IconNames.FORK}
                    text="GROUP BY multi-value unnesting"
                    value={groupByEnableMultiValueUnnesting}
                    showUndefined
                    undefinedEffectiveValue={!ingestMode}
                    onValueChange={groupByEnableMultiValueUnnesting =>
                      changeQueryContext({ ...queryContext, groupByEnableMultiValueUnnesting })
                    }
                    optionsText={ENABLE_DISABLE_OPTIONS_TEXT}
                  />
                )}

                {show('use-cache') && (
                  <MenuBoolean
                    icon={IconNames.DATA_CONNECTION}
                    text="Use cache"
                    value={useCache}
                    onValueChange={useCache =>
                      changeQueryContext({
                        ...queryContext,
                        useCache,
                        populateCache: useCache,
                      })
                    }
                    optionsText={ENABLE_DISABLE_OPTIONS_TEXT}
                  />
                )}
                {show('limit-inline-results') && (
                  <MenuCheckbox
                    checked={!query.unlimited}
                    intent={query.unlimited ? Intent.WARNING : undefined}
                    text="Limit inline results"
                    labelElement={
                      query.unlimited ? <Icon icon={IconNames.WARNING_SIGN} /> : undefined
                    }
                    onChange={() => {
                      onQueryChange(query.toggleUnlimited());
                    }}
                  />
                )}

                {show('durable-shuffle-storage') && (
                  <MenuBoolean
                    icon={IconNames.CLOUD_TICK}
                    text="Durable shuffle storage"
                    value={durableShuffleStorage}
                    onValueChange={durableShuffleStorage =>
                      changeQueryContext({
                        ...queryContext,
                        durableShuffleStorage,
                      })
                    }
                    optionsText={ENABLE_DISABLE_OPTIONS_TEXT}
                  />
                )}
                {show('select-destination') && (
                  <MenuItem
                    icon={IconNames.MANUALLY_ENTERED_DATA}
                    text="SELECT destination"
                    label={
                      SELECT_DESTINATION_LABEL[selectDestination as SelectDestination] ??
                      selectDestination
                    }
                    intent={intent}
                  >
                    {(['taskReport', 'durableStorage'] as SelectDestination[]).map(o => (
                      <MenuItem
                        key={o}
                        icon={tickIcon(selectDestination === o)}
                        text={SELECT_DESTINATION_LABEL[o]}
                        shouldDismissPopover={false}
                        onClick={() =>
                          changeQueryContext({ ...queryContext, selectDestination: o })
                        }
                      />
                    ))}
                    <MenuDivider />
                    <MenuCheckbox
                      checked={selectDestination === 'taskReport' ? !query.unlimited : false}
                      intent={intent}
                      disabled={selectDestination !== 'taskReport'}
                      text="Limit SELECT results in taskReport"
                      labelElement={
                        query.unlimited ? <Icon icon={IconNames.WARNING_SIGN} /> : undefined
                      }
                      onChange={() => {
                        onQueryChange(query.toggleUnlimited());
                      }}
                    />
                  </MenuItem>
                )}
              </Menu>
            }
          >
            <Button
              text={`Engine: ${
                queryEngine
                  ? enginesLabelFn(queryEngine).text
                  : `${autoEngineLabel.text} [${enginesLabelFn(effectiveEngine).text}]`
              }`}
              rightIcon={IconNames.CARET_DOWN}
              intent={intent}
            />
          </Popover>
          {effectiveEngine === 'sql-msq-task' && (
            <MaxTasksButton
              clusterCapacity={clusterCapacity}
              queryContext={queryContext}
              changeQueryContext={changeQueryContext}
              defaultQueryContext={defaultQueryContext}
              menuHeader={maxTasksMenuHeader}
              maxTasksLabelFn={maxTasksLabelFn}
              maxTasksOptions={maxTasksOptions}
              fullClusterCapacityLabelFn={fullClusterCapacityLabelFn}
            />
          )}
          {ingestMode && (
            <Popover
              position={Position.BOTTOM_LEFT}
              content={
                <Menu>
                  {([undefined, 'array', 'mvd'] as (ArrayIngestMode | undefined)[]).map((m, i) => (
                    <MenuItem
                      key={i}
                      icon={tickIcon(m === arrayIngestMode)}
                      text={
                        m
                          ? ARRAY_INGEST_MODE_DESCRIPTION[m]
                          : `(server default${
                              defaultQueryContext.arrayIngestMode
                                ? `: ${defaultQueryContext.arrayIngestMode}`
                                : ''
                            })`
                      }
                      onClick={() => changeQueryContext({ ...queryContext, arrayIngestMode: m })}
                    />
                  ))}
                  <MenuDivider />
                  <MenuItem
                    icon={IconNames.HELP}
                    text="Documentation"
                    href={`${getLink('DOCS')}/querying/arrays#arrayingestmode`}
                    target="_blank"
                  />
                </Menu>
              }
            >
              <Button
                text={`Array ingest mode: ${
                  arrayIngestMode ? ARRAY_INGEST_MODE_LABEL[arrayIngestMode] : '(server default)'
                }`}
                rightIcon={IconNames.CARET_DOWN}
              />
            </Popover>
          )}
        </ButtonGroup>
      )}
      {moreMenu && (
        <Popover position={Position.BOTTOM_LEFT} content={moreMenu}>
          <Button rightIcon={IconNames.MORE} data-tooltip="Engine specific tools" />
        </Popover>
      )}
      {editContextDialogOpen && (
        <EditContextDialog
          initQueryContext={queryContext}
          onQueryContextChange={changeQueryContext}
          onClose={() => {
            setEditContextDialogOpen(false);
          }}
        />
      )}
      {editParametersDialogOpen && (
        <QueryParametersDialog
          queryParameters={queryParameters}
          onQueryParametersChange={p => onQueryChange(query.changeQueryParameters(p))}
          onClose={() => {
            setEditParametersDialogOpen(false);
          }}
        />
      )}
      {customTimezoneDialogOpen && (
        <StringInputDialog
          title="Custom timezone"
          placeholder="Etc/UTC"
          maxLength={50}
          onSubmit={sqlTimeZone => changeQueryContext({ ...queryContext, sqlTimeZone })}
          onClose={() => setCustomTimezoneDialogOpen(false)}
        />
      )}
      {indexSpecDialogSpec && (
        <IndexSpecDialog
          onClose={() => setIndexSpecDialogSpec(undefined)}
          onSave={indexSpec => changeQueryContext({ ...queryContext, indexSpec })}
          indexSpec={indexSpecDialogSpec}
        />
      )}
    </div>
  );
});
