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
  MenuItem,
  Popover,
  Position,
  ResizeSensor,
} from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import type { Timezone } from 'chronoshift';
import classNames from 'classnames';
import type { Column, QueryResult, SqlExpression, SqlQuery } from 'druid-query-toolkit';
import { SqlLiteral } from 'druid-query-toolkit';
import React, { useState } from 'react';

import { useMemoWithPrevious } from '../../../../hooks';
import {
  isEmpty,
  localStorageGetJson,
  LocalStorageKeys,
  localStorageSetJson,
  mapRecord,
  Stage,
} from '../../../../utils';
import type {
  Measure,
  ModuleState,
  ParameterDefinition,
  ParameterValues,
  QuerySource,
} from '../../models';
import { effectiveParameterDefault, removeUndefinedParameterValues } from '../../models';
import { ModuleRepository } from '../../module-repository/module-repository';
import { adjustTransferValue, normalizeType } from '../../utils';
import { ControlPane } from '../control-pane/control-pane';
import { DroppableContainer } from '../droppable-container/droppable-container';
import { FilterPane } from '../filter-pane/filter-pane';
import { Issue } from '../issue/issue';
import { ModulePicker } from '../module-picker/module-picker';
import { ErrorBoundary } from '../error-boundary/error-boundary';

import './module-pane.scss';

function getStickyParameterValuesForModule(moduleId: string): ParameterValues {
  return localStorageGetJson(LocalStorageKeys.EXPLORE_STICKY)?.[moduleId] || {};
}

function fillInDefaults(
  parameterValues: ParameterValues,
  previousParameterValues: ParameterValues | undefined,
  parameters: Record<string, ParameterDefinition>,
  querySource: QuerySource,
  where: SqlExpression,
): Record<string, any> {
  const parameterValuesWithDefaults = { ...parameterValues };
  Object.entries(parameters).forEach(([propName, propDefinition]) => {
    if (typeof parameterValuesWithDefaults[propName] !== 'undefined') return;
    parameterValuesWithDefaults[propName] = effectiveParameterDefault(
      propDefinition,
      parameterValues,
      querySource,
      where,
      previousParameterValues?.[propName],
    );
  });
  return parameterValuesWithDefaults;
}

export interface ModulePaneProps {
  className: string;
  moduleState: ModuleState;
  setModuleState(moduleState: ModuleState): void;
  onDelete(): void;
  querySource: QuerySource;
  timezone: Timezone;
  where: SqlExpression;
  setWhere(where: SqlExpression): void;

  runSqlQuery(
    query: string | SqlQuery | { query: string | SqlQuery; timezone?: Timezone },
  ): Promise<QueryResult>;

  onAddToSourceQueryAsColumn?(expression: SqlExpression): void;
  onAddToSourceQueryAsMeasure?(measure: Measure): void;
}

export const ModulePane = function ModulePane(props: ModulePaneProps) {
  const {
    className,
    moduleState,
    setModuleState,
    onDelete,
    querySource,
    timezone,
    where,
    setWhere,
    runSqlQuery,
    onAddToSourceQueryAsColumn,
    onAddToSourceQueryAsMeasure,
  } = props;
  const { moduleId, moduleWhere, parameterValues, showModuleWhere, showControls } = moduleState;
  const [stage, setStage] = useState<Stage | undefined>();

  const module = ModuleRepository.getModule(moduleId);

  function updateParameterValues(newParameterValues: ParameterValues) {
    if (!module) return;

    // Evaluate sticky-ness
    const currentExploreSticky = localStorageGetJson(LocalStorageKeys.EXPLORE_STICKY) || {};
    const currentModuleSticky = currentExploreSticky[moduleId] || {};
    const newModuleSticky = {
      ...currentModuleSticky,
      ...mapRecord(newParameterValues, (v, k) => (module.parameters[k]?.sticky ? v : undefined)),
    };

    localStorageSetJson(LocalStorageKeys.EXPLORE_STICKY, {
      ...currentExploreSticky,
      [moduleId]: isEmpty(newModuleSticky) ? undefined : newModuleSticky,
    });

    setModuleState(
      moduleState.changeParameterValues(
        removeUndefinedParameterValues(
          { ...parameterValues, ...newParameterValues },
          module.parameters,
          querySource,
          where,
        ),
      ),
    );
  }

  const parameterValuesWithDefaults: ParameterValues = useMemoWithPrevious(
    previousParameterValuesWithDefaults => {
      if (!module) return {};
      return fillInDefaults(
        parameterValues,
        previousParameterValuesWithDefaults,
        module.parameters,
        querySource,
        where,
      );
    },
    [parameterValues, module, querySource],
  );

  let content: React.ReactNode;
  if (module) {
    const modelIssue = undefined; // AutoForm.issueWithModel(moduleTileConfig.config, module.configFields);
    if (modelIssue) {
      content = <Issue issue={modelIssue} />;
    } else if (stage) {
      content = React.createElement(module.component, {
        stage,
        querySource,
        timezone,
        where,
        setWhere,
        moduleWhere,
        parameterValues: parameterValuesWithDefaults,
        setParameterValues: updateParameterValues,
        runSqlQuery,
      });
    }
  } else {
    content = <Issue issue={`Unknown module id: ${moduleId}`} />;
  }

  function onShowColumn(column: Column) {
    setModuleState(moduleState.applyShowColumn(column));
  }

  function onShowMeasure(measure: Measure) {
    setModuleState(moduleState.applyShowMeasure(measure));
  }

  const moduleHasFilter = !SqlLiteral.isTrue(moduleWhere);
  return (
    <div
      className={classNames(
        'module-pane',
        className,
        showControls ? 'show-controls' : 'no-controls',
        showModuleWhere ? 'show-filter' : 'no-filter',
      )}
    >
      {showModuleWhere && (
        <FilterPane
          querySource={querySource}
          extraFilter={where}
          timezone={timezone}
          filter={moduleWhere}
          onFilterChange={newFilter => setModuleState(moduleState.changeModuleWhere(newFilter))}
          runSqlQuery={runSqlQuery}
        />
      )}
      <div className="module-control-bar">
        <ModulePicker
          selectedModuleId={moduleId}
          onSelectedModuleIdChange={newModuleId => {
            let newParameterValues = getStickyParameterValuesForModule(newModuleId);

            const oldModule = ModuleRepository.getModule(moduleId);
            const newModule = ModuleRepository.getModule(newModuleId);
            if (oldModule && newModule) {
              const oldModuleParameters = oldModule.parameters || {};
              const newModuleParameters = newModule.parameters || {};
              for (const paramName in oldModuleParameters) {
                const parameterValue = parameterValues[paramName];
                if (typeof parameterValue === 'undefined') continue;

                const oldParameterDefinition = oldModuleParameters[paramName];
                const transferGroup = oldParameterDefinition.transferGroup;
                if (typeof transferGroup !== 'string') continue;

                const normalizedType = normalizeType(oldParameterDefinition.type);
                const target = Object.entries(newModuleParameters).find(
                  ([_, def]) =>
                    def.transferGroup === transferGroup &&
                    normalizeType(def.type) === normalizedType,
                );
                if (!target) continue;

                newParameterValues = {
                  ...newParameterValues,
                  [target[0]]: adjustTransferValue(
                    parameterValue,
                    oldParameterDefinition.type,
                    target[1].type,
                  ),
                };
              }
            }

            setModuleState(
              moduleState.change({ moduleId: newModuleId, parameterValues: newParameterValues }),
            );
          }}
        />
        {module && !showControls && (
          <ControlPane
            querySource={querySource}
            where={where}
            onUpdateParameterValues={updateParameterValues}
            parameters={module.parameters}
            parameterValues={parameterValuesWithDefaults}
            compact
            onAddToSourceQueryAsColumn={onAddToSourceQueryAsColumn}
            onAddToSourceQueryAsMeasure={onAddToSourceQueryAsMeasure}
          />
        )}
      </div>
      <ButtonGroup className="corner-buttons">
        <Popover
          position={Position.BOTTOM_RIGHT}
          content={
            <Menu>
              <MenuItem
                icon={IconNames.RESET}
                text="Reset visualization parameters"
                onClick={() => {
                  setModuleState(
                    moduleState.changeParameterValues(getStickyParameterValuesForModule(moduleId)),
                  );
                }}
              />
              <MenuItem
                icon={IconNames.TRASH}
                text="Delete module"
                intent={Intent.DANGER}
                onClick={onDelete}
              />
            </Menu>
          }
        >
          <Button icon={IconNames.MORE} data-tooltip="More module options" minimal />
        </Popover>
        <Button
          icon={moduleHasFilter ? IconNames.FILTER_KEEP : IconNames.FILTER}
          data-tooltip={`${showModuleWhere ? 'Hide' : 'Show'} module filter bar${
            moduleHasFilter ? `\nCurrent filter: ${moduleWhere}` : ''
          }`}
          minimal
          active={showModuleWhere}
          onClick={() => setModuleState(moduleState.change({ showModuleWhere: !showModuleWhere }))}
        />
        <Button
          icon={IconNames.PROPERTIES}
          data-tooltip={`${showControls ? 'Hide' : 'Show'} module controls`}
          minimal
          active={showControls}
          onClick={() => setModuleState(moduleState.change({ showControls: !showControls }))}
        />
      </ButtonGroup>
      {showControls && module && (
        <div className="control-pane-container">
          <ControlPane
            querySource={querySource}
            where={where}
            onUpdateParameterValues={updateParameterValues}
            parameters={module.parameters}
            parameterValues={parameterValuesWithDefaults}
            onAddToSourceQueryAsColumn={onAddToSourceQueryAsColumn}
            onAddToSourceQueryAsMeasure={onAddToSourceQueryAsMeasure}
          />
        </div>
      )}
      <ResizeSensor
        onResize={entries => {
          if (entries.length !== 1) return;
          const newStage = new Stage(entries[0].contentRect.width, entries[0].contentRect.height);
          if (newStage.equals(stage)) return;
          setStage(newStage);
        }}
      >
        <DroppableContainer
          className="module-inner-container"
          onDropColumn={onShowColumn}
          onDropMeasure={onShowMeasure}
        >
          <ErrorBoundary>{content}</ErrorBoundary>
        </DroppableContainer>
      </ResizeSensor>
    </div>
  );
};
