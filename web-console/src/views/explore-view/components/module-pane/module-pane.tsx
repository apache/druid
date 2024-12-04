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

import { ResizeSensor } from '@blueprintjs/core';
import type { QueryResult, SqlExpression, SqlQuery } from 'druid-query-toolkit';
import React, { useMemo, useState } from 'react';

import { Stage } from '../../../../utils/stage';
import type { ParameterDefinition, ParameterValues, QuerySource } from '../../models';
import { effectiveParameterDefault } from '../../models';
import { ModuleRepository } from '../../module-repository/module-repository';
import { Issue } from '../issue/issue';

import './module-pane.scss';

function fillInDefaults(
  parameterValues: ParameterValues,
  parameters: Record<string, ParameterDefinition>,
  querySource: QuerySource,
): Record<string, any> {
  const parameterValuesWithDefaults = { ...parameterValues };
  Object.entries(parameters).forEach(([propName, propDefinition]) => {
    if (typeof parameterValuesWithDefaults[propName] !== 'undefined') return;
    parameterValuesWithDefaults[propName] = effectiveParameterDefault(propDefinition, querySource);
  });
  return parameterValuesWithDefaults;
}

export interface ModulePaneProps {
  moduleId: string;
  querySource: QuerySource;
  where: SqlExpression;
  setWhere(where: SqlExpression): void;

  parameterValues: ParameterValues;
  setParameterValues(parameters: ParameterValues): void;
  runSqlQuery(query: string | SqlQuery): Promise<QueryResult>;
}

export const ModulePane = function ModulePane(props: ModulePaneProps) {
  const {
    moduleId,
    querySource,
    where,
    setWhere,
    parameterValues,
    setParameterValues,
    runSqlQuery,
  } = props;
  const [stage, setStage] = useState<Stage | undefined>();

  const module = ModuleRepository.getModule(moduleId);

  const parameterValuesWithDefaults = useMemo(() => {
    if (!module) return {};
    return fillInDefaults(parameterValues, module.parameters, querySource);
  }, [parameterValues, module, querySource]);

  let content: React.ReactNode;
  if (module) {
    const modelIssue = undefined; // AutoForm.issueWithModel(moduleTileConfig.config, module.configFields);
    if (modelIssue) {
      content = <Issue issue={modelIssue} />;
    } else if (stage) {
      content = React.createElement(module.component, {
        stage,
        querySource,
        where,
        setWhere,
        parameterValues: parameterValuesWithDefaults,
        setParameterValues,
        runSqlQuery,
      });
    }
  } else {
    content = <Issue issue={`Unknown module id: ${moduleId}`} />;
  }

  return (
    <ResizeSensor
      onResize={entries => {
        if (entries.length !== 1) return;
        const newStage = new Stage(entries[0].contentRect.width, entries[0].contentRect.height);
        if (newStage.equals(stage)) return;
        setStage(newStage);
      }}
    >
      <div className="module-pane">{content}</div>
    </ResizeSensor>
  );
};
