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

import { MenuCheckbox, MenuTristate } from '../../../components';
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

import './run-panel.scss';

const NAMED_TIMEZONES: string[] = [
  'America/Juneau', // -9.0
  'America/Los_Angeles', // -8.0
  'America/Yellowknife', // -7.0
  'America/Phoenix', // -7.0
  'America/Denver', // -7.0
  'America/Mexico_City', // -6.0
  'America/Chicago', // -6.0
  'America/New_York', // -5.0
  'America/Argentina/Buenos_Aires', // -4.0
  'Etc/UTC', // +0.0
  'Europe/London', // +0.0
  'Europe/Paris', // +1.0
  'Asia/Jerusalem', // +2.0
  'Asia/Shanghai', // +8.0
  'Asia/Hong_Kong', // +8.0
  'Asia/Seoul', // +9.0
  'Asia/Tokyo', // +9.0
  'Pacific/Guam', // +10.0
  'Australia/Sydney', // +11.0
];

const ARRAY_INGEST_MODE_DESCRIPTION: Record<ArrayIngestMode, JSX.Element> = {
  array: (
    <>
      array: Load SQL <Tag minimal>VARCHAR ARRAY</Tag> as Druid{' '}
      <Tag minimal>ARRAY&lt;STRING&gt;</Tag>
    </>
  ),
  mvd: (
    <>
      mvd: Load SQL <Tag minimal>VARCHAR ARRAY</Tag> as Druid multi-value <Tag minimal>STRING</Tag>
    </>
  ),
};

const DEFAULT_ENGINES_LABEL_FN = (engine: DruidEngine | undefined) => {
  if (!engine) return { text: 'auto' };
  return {
    text: engine,
    label: engine === 'sql-msq-task' ? 'multi-stage-query' : undefined,
  };
};

export interface RunPanelProps
  extends Pick<MaxTasksButtonProps, 'maxTasksLabelFn' | 'fullClusterCapacityLabelFn'> {
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
    fullClusterCapacityLabelFn,
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
  const useConcurrentLocks = getQueryContextKey(
    'useConcurrentLocks',
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

  function offsetOptions(): JSX.Element[] {
    const items: JSX.Element[] = [];

    for (let i = -12; i <= 14; i++) {
      const offset = `${i < 0 ? '-' : '+'}${String(Math.abs(i)).padStart(2, '0')}:00`;
      items.push(
        <MenuItem
          key={offset}
          icon={tickIcon(offset === sqlTimeZone)}
          text={offset}
          shouldDismissPopover={false}
          onClick={() => changeQueryContext({ ...queryContext, sqlTimeZone: offset })}
        />,
      );
    }

    return items;
  }

  const overloadWarning =
    query.unlimited &&
    (queryEngine === 'sql-native' ||
      (queryEngine === 'sql-msq-task' && selectDestination === 'taskReport'));
  const intent = overloadWarning ? Intent.WARNING : undefined;

  const effectiveEngine = query.getEffectiveEngine();

  const autoEngineLabel = enginesLabelFn(undefined);

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
                    <MenuDivider title="Select engine" />
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
                <MenuItem
                  icon={IconNames.PROPERTIES}
                  text="Edit context"
                  onClick={() => setEditContextDialogOpen(true)}
                  label={pluralIfNeeded(numContextKeys, 'key')}
                />
                <MenuItem
                  icon={IconNames.HELP}
                  text="Define parameters"
                  onClick={() => setEditParametersDialogOpen(true)}
                  label={queryParameters ? pluralIfNeeded(queryParameters.length, 'parameter') : ''}
                />
                {effectiveEngine !== 'native' && (
                  <MenuItem
                    icon={IconNames.GLOBE_NETWORK}
                    text="Timezone"
                    label={sqlTimeZone ?? defaultQueryContext.sqlTimeZone}
                  >
                    <MenuDivider title="Timezone type" />
                    <MenuItem
                      icon={tickIcon(!sqlTimeZone)}
                      text="Default"
                      label={defaultQueryContext.sqlTimeZone}
                      shouldDismissPopover={false}
                      onClick={() =>
                        changeQueryContext({ ...queryContext, sqlTimeZone: undefined })
                      }
                    />
                    <MenuItem icon={tickIcon(String(sqlTimeZone).includes('/'))} text="Named">
                      {NAMED_TIMEZONES.map(namedTimezone => (
                        <MenuItem
                          key={namedTimezone}
                          icon={tickIcon(namedTimezone === sqlTimeZone)}
                          text={namedTimezone}
                          shouldDismissPopover={false}
                          onClick={() =>
                            changeQueryContext({ ...queryContext, sqlTimeZone: namedTimezone })
                          }
                        />
                      ))}
                    </MenuItem>
                    <MenuItem icon={tickIcon(String(sqlTimeZone).includes(':'))} text="Offset">
                      {offsetOptions()}
                    </MenuItem>
                    <MenuItem
                      icon={IconNames.BLANK}
                      text="Custom"
                      onClick={() => setCustomTimezoneDialogOpen(true)}
                    />
                  </MenuItem>
                )}
                {effectiveEngine === 'sql-msq-task' ? (
                  <>
                    <MenuItem icon={IconNames.BRING_DATA} text="INSERT / REPLACE specific context">
                      <MenuCheckbox
                        checked={useConcurrentLocks}
                        text="Use concurrent locks"
                        onChange={() =>
                          changeQueryContext({
                            ...queryContext,
                            useConcurrentLocks: !useConcurrentLocks,
                          })
                        }
                      />
                      <MenuTristate
                        icon={IconNames.DISABLE}
                        text="Fail on empty insert"
                        value={failOnEmptyInsert}
                        undefinedEffectiveValue={false}
                        onValueChange={failOnEmptyInsert =>
                          changeQueryContext({ ...queryContext, failOnEmptyInsert })
                        }
                      />
                      <MenuTristate
                        icon={IconNames.STOPWATCH}
                        text="Wait until segments have loaded"
                        value={waitUntilSegmentsLoad}
                        undefinedEffectiveValue={ingestMode}
                        onValueChange={waitUntilSegmentsLoad =>
                          changeQueryContext({ ...queryContext, waitUntilSegmentsLoad })
                        }
                      />
                      <MenuItem
                        icon={IconNames.TH_DERIVED}
                        text="Edit index spec"
                        label={summarizeIndexSpec(indexSpec)}
                        shouldDismissPopover={false}
                        onClick={() => {
                          setIndexSpecDialogSpec(indexSpec || {});
                        }}
                      />
                    </MenuItem>
                    <MenuItem
                      icon={IconNames.ERROR}
                      text="Max parse exceptions"
                      label={String(maxParseExceptions)}
                    >
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
                    <MenuTristate
                      icon={IconNames.TRANSLATE}
                      text="Finalize aggregations"
                      value={finalizeAggregations}
                      undefinedEffectiveValue={!ingestMode}
                      onValueChange={finalizeAggregations =>
                        changeQueryContext({ ...queryContext, finalizeAggregations })
                      }
                    />
                    <MenuTristate
                      icon={IconNames.FORK}
                      text="Enable GroupBy multi-value unnesting"
                      value={groupByEnableMultiValueUnnesting}
                      undefinedEffectiveValue={!ingestMode}
                      onValueChange={groupByEnableMultiValueUnnesting =>
                        changeQueryContext({ ...queryContext, groupByEnableMultiValueUnnesting })
                      }
                    />
                    <MenuItem
                      icon={IconNames.INNER_JOIN}
                      text="Join algorithm"
                      label={sqlJoinAlgorithm}
                    >
                      {(['broadcast', 'sortMerge'] as SqlJoinAlgorithm[]).map(o => (
                        <MenuItem
                          key={o}
                          icon={tickIcon(sqlJoinAlgorithm === o)}
                          text={o}
                          shouldDismissPopover={false}
                          onClick={() =>
                            changeQueryContext({ ...queryContext, sqlJoinAlgorithm: o })
                          }
                        />
                      ))}
                    </MenuItem>
                    <MenuItem
                      icon={IconNames.MANUALLY_ENTERED_DATA}
                      text="SELECT destination"
                      label={selectDestination}
                      intent={intent}
                    >
                      {(['taskReport', 'durableStorage'] as SelectDestination[]).map(o => (
                        <MenuItem
                          key={o}
                          icon={tickIcon(selectDestination === o)}
                          text={o}
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
                    <MenuCheckbox
                      checked={durableShuffleStorage}
                      text="Durable shuffle storage"
                      onChange={() =>
                        changeQueryContext({
                          ...queryContext,
                          durableShuffleStorage: !durableShuffleStorage,
                        })
                      }
                    />
                  </>
                ) : (
                  <>
                    <MenuCheckbox
                      checked={useCache}
                      text="Use cache"
                      onChange={() =>
                        changeQueryContext({
                          ...queryContext,
                          useCache: !useCache,
                          populateCache: !useCache,
                        })
                      }
                    />
                    <MenuCheckbox
                      checked={useApproximateTopN}
                      text="Use approximate TopN"
                      onChange={() =>
                        changeQueryContext({
                          ...queryContext,
                          useApproximateTopN: !useApproximateTopN,
                        })
                      }
                    />
                  </>
                )}
                {effectiveEngine !== 'native' && (
                  <MenuCheckbox
                    checked={useApproximateCountDistinct}
                    text="Use approximate COUNT(DISTINCT)"
                    onChange={() =>
                      changeQueryContext({
                        ...queryContext,
                        useApproximateCountDistinct: !useApproximateCountDistinct,
                      })
                    }
                  />
                )}
                {effectiveEngine === 'sql-native' && (
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
              </Menu>
            }
          >
            <Button
              text={`Engine: ${
                queryEngine
                  ? enginesLabelFn(queryEngine).text
                  : `${autoEngineLabel.text} (${enginesLabelFn(effectiveEngine).text})`
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
                text={`Array ingest mode: ${arrayIngestMode ?? '(server default)'}`}
                rightIcon={IconNames.CARET_DOWN}
              />
            </Popover>
          )}
        </ButtonGroup>
      )}
      {moreMenu && (
        <Popover position={Position.BOTTOM_LEFT} content={moreMenu}>
          <Button rightIcon={IconNames.MORE} />
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
