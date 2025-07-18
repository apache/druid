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

import type { Column } from 'druid-query-toolkit';
import { SqlExpression, SqlLiteral } from 'druid-query-toolkit';

import { ModuleRepository } from '../module-repository/module-repository';
import type { Rename } from '../utils';

import { ExpressionMeta } from './expression-meta';
import type { Measure } from './measure';
import type { ParameterValues } from './parameter';
import { inflateParameterValues, renameColumnsInParameterValues } from './parameter';
import type { QuerySource } from './query-source';

interface ModuleStateValue {
  moduleId: string;
  moduleWhere?: SqlExpression;
  parameterValues: ParameterValues;
  showModuleWhere?: boolean;
  showControls?: boolean;
}

export class ModuleState {
  static INIT_STATE: ModuleState;

  static fromJS(js: any) {
    const inflatedParameterValues = inflateParameterValues(
      js.parameterValues,
      ModuleRepository.getModule(js.moduleId)?.parameters || {},
    );
    return new ModuleState({
      ...js,
      moduleWhere: SqlExpression.maybeParse(js.moduleWhere),
      parameterValues: inflatedParameterValues,
    });
  }

  public readonly moduleId: string;
  public readonly moduleWhere: SqlExpression;
  public readonly parameterValues: ParameterValues;
  public readonly showModuleWhere: boolean;
  public readonly showControls: boolean;

  constructor(value: ModuleStateValue) {
    this.moduleId = value.moduleId;
    this.moduleWhere = value.moduleWhere || SqlLiteral.TRUE;
    this.parameterValues = value.parameterValues;
    this.showModuleWhere = Boolean(value.showModuleWhere);
    this.showControls = Boolean(value.showControls);
  }

  valueOf(): ModuleStateValue {
    const value: ModuleStateValue = {
      moduleId: this.moduleId,
      parameterValues: this.parameterValues,
    };
    if (!SqlLiteral.isTrue(this.moduleWhere)) value.moduleWhere = this.moduleWhere;
    if (this.showModuleWhere) value.showModuleWhere = true;
    if (this.showControls) value.showControls = true;
    return value;
  }

  public change(newValues: Partial<ModuleStateValue>): ModuleState {
    return new ModuleState({
      ...this.valueOf(),
      ...newValues,
    });
  }

  public changeModuleWhere(moduleWhere: SqlExpression): ModuleState {
    return this.change({ moduleWhere });
  }

  public changeParameterValues(parameterValues: ParameterValues): ModuleState {
    return this.change({ parameterValues });
  }

  public applyRename(rename: Rename): ModuleState {
    const module = ModuleRepository.getModule(this.moduleId);
    if (!module) return this;

    return this.change({
      parameterValues: renameColumnsInParameterValues(
        this.parameterValues,
        module.parameters,
        rename,
      ),
    });
  }

  public restrictToQuerySource(querySource: QuerySource, where: SqlExpression): ModuleState {
    const { moduleId, moduleWhere, parameterValues } = this;
    const module = ModuleRepository.getModule(moduleId);
    if (!module) return this;
    const newModuleWhere = querySource.restrictWhere(moduleWhere);
    const newParameterValues = querySource.restrictParameterValues(
      parameterValues,
      module.parameters,
      where,
    );
    if (moduleWhere === newModuleWhere && parameterValues === newParameterValues) return this;

    return this.change({
      moduleWhere: newModuleWhere,
      parameterValues: newParameterValues,
    });
  }

  public applyShowColumn(column: Column): ModuleState {
    let newModuleId: string;
    let newParameterValues: ParameterValues = {};
    if (column.sqlType === 'TIMESTAMP') {
      newModuleId = 'time-chart';
    } else if (column.sqlType === 'BOOLEAN') {
      newModuleId = 'pie-chart';
      newParameterValues = {
        splitColumn: ExpressionMeta.fromColumn(column),
      };
    } else {
      newModuleId = 'grouping-table';
      newParameterValues = {
        splitColumns: [ExpressionMeta.fromColumn(column)],
      };
    }

    return this.change({
      moduleId: newModuleId,
      parameterValues: {
        ...(this.moduleId === newModuleId ? this.parameterValues : {}),
        ...newParameterValues,
      },
    });
  }

  public applyShowMeasure(measure: Measure): ModuleState {
    const module = ModuleRepository.getModule(this.moduleId);
    if (module) {
      const p = Object.entries(module.parameters).find(
        ([_, def]) => def.type === 'measure' || def.type === 'measures',
      );
      if (p) {
        const [paramName, def] = p;
        const { parameterValues } = this;
        return this.change({
          parameterValues: {
            ...parameterValues,
            [paramName]:
              def.type === 'measures'
                ? (parameterValues[paramName] || []).concat(measure)
                : measure,
          },
        });
      }
    }

    return this.change({
      moduleId: 'time-chart',
      parameterValues: {
        measures: [measure],
      },
    });
  }
}

ModuleState.INIT_STATE = new ModuleState({
  moduleId: 'record-table',
  parameterValues: {},
});
