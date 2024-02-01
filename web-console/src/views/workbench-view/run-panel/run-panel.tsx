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
  Position,
  useHotkeys,
} from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import { Popover2 } from '@blueprintjs/popover2';
import type { JSX } from 'react';
import React, { useCallback, useMemo, useState } from 'react';

import { MenuCheckbox, MenuTristate } from '../../../components';
import { EditContextDialog, StringInputDialog } from '../../../dialogs';
import { IndexSpecDialog } from '../../../dialogs/index-spec-dialog/index-spec-dialog';
import type { DruidEngine, IndexSpec, QueryContext, WorkbenchQuery } from '../../../druid-models';
import {
  changeDurableShuffleStorage,
  changeFailOnEmptyInsert,
  changeFinalizeAggregations,
  changeGroupByEnableMultiValueUnnesting,
  changeMaxParseExceptions,
  changeTimezone,
  changeUseApproximateCountDistinct,
  changeUseApproximateTopN,
  changeUseCache,
  changeWaitUntilSegmentsLoad,
  getDurableShuffleStorage,
  getFailOnEmptyInsert,
  getFinalizeAggregations,
  getGroupByEnableMultiValueUnnesting,
  getMaxParseExceptions,
  getTimezone,
  getUseApproximateCountDistinct,
  getUseApproximateTopN,
  getUseCache,
  getWaitUntilSegmentsLoad,
  summarizeIndexSpec,
} from '../../../druid-models';
import { deepGet, deepSet, pluralIfNeeded, tickIcon } from '../../../utils';
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

export interface RunPanelProps {
  query: WorkbenchQuery;
  onQueryChange(query: WorkbenchQuery): void;
  running: boolean;
  small?: boolean;
  onRun(preview: boolean): void | Promise<void>;
  queryEngines: DruidEngine[];
  clusterCapacity: number | undefined;
  moreMenu?: JSX.Element;
}

export const RunPanel = React.memo(function RunPanel(props: RunPanelProps) {
  const { query, onQueryChange, onRun, moreMenu, running, small, queryEngines, clusterCapacity } =
    props;
  const [editContextDialogOpen, setEditContextDialogOpen] = useState(false);
  const [editParametersDialogOpen, setEditParametersDialogOpen] = useState(false);
  const [customTimezoneDialogOpen, setCustomTimezoneDialogOpen] = useState(false);
  const [indexSpecDialogSpec, setIndexSpecDialogSpec] = useState<IndexSpec | undefined>();

  const emptyQuery = query.isEmptyQuery();
  const ingestMode = query.isIngestQuery();
  const queryContext = query.queryContext;
  const numContextKeys = Object.keys(queryContext).length;
  const queryParameters = query.queryParameters;

  const maxParseExceptions = getMaxParseExceptions(queryContext);
  const failOnEmptyInsert = getFailOnEmptyInsert(queryContext);
  const finalizeAggregations = getFinalizeAggregations(queryContext);
  const waitUntilSegmentsLoad = getWaitUntilSegmentsLoad(queryContext);
  const groupByEnableMultiValueUnnesting = getGroupByEnableMultiValueUnnesting(queryContext);
  const sqlJoinAlgorithm = queryContext.sqlJoinAlgorithm ?? 'broadcast';
  const selectDestination = queryContext.selectDestination ?? 'taskReport';
  const durableShuffleStorage = getDurableShuffleStorage(queryContext);
  const indexSpec: IndexSpec | undefined = deepGet(queryContext, 'indexSpec');
  const useApproximateCountDistinct = getUseApproximateCountDistinct(queryContext);
  const useApproximateTopN = getUseApproximateTopN(queryContext);
  const useCache = getUseCache(queryContext);
  const timezone = getTimezone(queryContext);

  const handleRun = useCallback(() => {
    if (!onRun) return;
    void onRun(false);
  }, [onRun]);

  const handlePreview = useCallback(() => {
    if (!onRun) return;
    void onRun(true);
  }, [onRun]);

  const hotkeys = useMemo(() => {
    if (small) return [];
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
  }, [small, handleRun, handlePreview]);

  useHotkeys(hotkeys);

  const queryEngine = query.engine;
  function renderQueryEngineMenuItem(e: DruidEngine | undefined) {
    return (
      <MenuItem
        key={String(e)}
        icon={tickIcon(e === queryEngine)}
        text={typeof e === 'undefined' ? 'auto' : e}
        label={e === 'sql-msq-task' ? 'multi-stage-query' : undefined}
        onClick={() => onQueryChange(query.changeEngine(e))}
        shouldDismissPopover={false}
      />
    );
  }

  function changeQueryContext(queryContext: QueryContext) {
    onQueryChange(query.changeQueryContext(queryContext));
  }

  const availableEngines = ([undefined] as (DruidEngine | undefined)[]).concat(queryEngines);

  function offsetOptions(): JSX.Element[] {
    const items: JSX.Element[] = [];

    for (let i = -12; i <= 14; i++) {
      const offset = `${i < 0 ? '-' : '+'}${String(Math.abs(i)).padStart(2, '0')}:00`;
      items.push(
        <MenuItem
          key={offset}
          icon={tickIcon(offset === timezone)}
          text={offset}
          shouldDismissPopover={false}
          onClick={() => changeQueryContext(changeTimezone(queryContext, offset))}
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
  return (
    <div className="run-panel">
      <Button
        className={effectiveEngine === 'native' ? 'rune-button' : undefined}
        disabled={running}
        icon={IconNames.CARET_RIGHT}
        onClick={() => void onRun(false)}
        text="Run"
        intent={!emptyQuery && !small ? Intent.PRIMARY : undefined}
        small={small}
        minimal={small}
      />
      {ingestMode && (
        <Button
          disabled={running}
          icon={IconNames.EYE_OPEN}
          onClick={() => void onRun(true)}
          text="Preview"
          small={small}
          minimal={small}
        />
      )}
      {!small && onQueryChange && (
        <ButtonGroup>
          <Popover2
            position={Position.BOTTOM_LEFT}
            content={
              <Menu>
                {queryEngines.length > 1 && (
                  <>
                    <MenuDivider title="Select engine" />
                    {availableEngines.map(renderQueryEngineMenuItem)}
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
                    label={timezone || 'default'}
                  >
                    <MenuDivider title="Timezone type" />
                    <MenuItem
                      icon={tickIcon(!timezone)}
                      text="Default"
                      shouldDismissPopover={false}
                      onClick={() => changeQueryContext(changeTimezone(queryContext, undefined))}
                    />
                    <MenuItem icon={tickIcon(String(timezone).includes('/'))} text="Named">
                      {NAMED_TIMEZONES.map(namedTimezone => (
                        <MenuItem
                          key={namedTimezone}
                          icon={tickIcon(namedTimezone === timezone)}
                          text={namedTimezone}
                          shouldDismissPopover={false}
                          onClick={() =>
                            changeQueryContext(changeTimezone(queryContext, namedTimezone))
                          }
                        />
                      ))}
                    </MenuItem>
                    <MenuItem icon={tickIcon(String(timezone).includes(':'))} text="Offset">
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
                            changeQueryContext(changeMaxParseExceptions(queryContext, v))
                          }
                          shouldDismissPopover={false}
                        />
                      ))}
                    </MenuItem>
                    <MenuTristate
                      icon={IconNames.DISABLE}
                      text="Fail on empty insert"
                      value={failOnEmptyInsert}
                      undefinedEffectiveValue={false}
                      onValueChange={v =>
                        changeQueryContext(changeFailOnEmptyInsert(queryContext, v))
                      }
                    />
                    <MenuTristate
                      icon={IconNames.TRANSLATE}
                      text="Finalize aggregations"
                      value={finalizeAggregations}
                      undefinedEffectiveValue={!ingestMode}
                      onValueChange={v =>
                        changeQueryContext(changeFinalizeAggregations(queryContext, v))
                      }
                    />
                    <MenuTristate
                      icon={IconNames.STOPWATCH}
                      text="Wait until segments have loaded"
                      value={waitUntilSegmentsLoad}
                      undefinedEffectiveValue={ingestMode}
                      onValueChange={v =>
                        changeQueryContext(changeWaitUntilSegmentsLoad(queryContext, v))
                      }
                    />
                    <MenuTristate
                      icon={IconNames.FORK}
                      text="Enable GroupBy multi-value unnesting"
                      value={groupByEnableMultiValueUnnesting}
                      undefinedEffectiveValue={!ingestMode}
                      onValueChange={v =>
                        changeQueryContext(changeGroupByEnableMultiValueUnnesting(queryContext, v))
                      }
                    />
                    <MenuItem
                      icon={IconNames.INNER_JOIN}
                      text="Join algorithm"
                      label={sqlJoinAlgorithm}
                    >
                      {['broadcast', 'sortMerge'].map(o => (
                        <MenuItem
                          key={o}
                          icon={tickIcon(sqlJoinAlgorithm === o)}
                          text={o}
                          shouldDismissPopover={false}
                          onClick={() =>
                            changeQueryContext(deepSet(queryContext, 'sqlJoinAlgorithm', o))
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
                      {['taskReport', 'durableStorage'].map(o => (
                        <MenuItem
                          key={o}
                          icon={tickIcon(selectDestination === o)}
                          text={o}
                          shouldDismissPopover={false}
                          onClick={() =>
                            changeQueryContext(deepSet(queryContext, 'selectDestination', o))
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
                        changeQueryContext(
                          changeDurableShuffleStorage(queryContext, !durableShuffleStorage),
                        )
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
                  </>
                ) : (
                  <>
                    <MenuCheckbox
                      checked={useCache}
                      text="Use cache"
                      onChange={() => changeQueryContext(changeUseCache(queryContext, !useCache))}
                    />
                    <MenuCheckbox
                      checked={useApproximateTopN}
                      text="Use approximate TopN"
                      onChange={() =>
                        changeQueryContext(
                          changeUseApproximateTopN(queryContext, !useApproximateTopN),
                        )
                      }
                    />
                  </>
                )}
                {effectiveEngine !== 'native' && (
                  <MenuCheckbox
                    checked={useApproximateCountDistinct}
                    text="Use approximate COUNT(DISTINCT)"
                    onChange={() =>
                      changeQueryContext(
                        changeUseApproximateCountDistinct(
                          queryContext,
                          !useApproximateCountDistinct,
                        ),
                      )
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
              text={`Engine: ${queryEngine || `auto (${effectiveEngine})`}`}
              rightIcon={IconNames.CARET_DOWN}
              intent={intent}
            />
          </Popover2>
          {effectiveEngine === 'sql-msq-task' && (
            <MaxTasksButton
              clusterCapacity={clusterCapacity}
              queryContext={queryContext}
              changeQueryContext={changeQueryContext}
            />
          )}
        </ButtonGroup>
      )}
      {moreMenu && (
        <Popover2 position={Position.BOTTOM_LEFT} content={moreMenu}>
          <Button small={small} minimal={small} rightIcon={IconNames.MORE} />
        </Popover2>
      )}
      {editContextDialogOpen && (
        <EditContextDialog
          queryContext={queryContext}
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
          onSubmit={tz => changeQueryContext(changeTimezone(queryContext, tz))}
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
