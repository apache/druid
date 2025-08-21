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

import type { IconName } from '@blueprintjs/icons';
import { IconNames } from '@blueprintjs/icons';
import { Timezone } from 'chronoshift';
import type { Column } from 'druid-query-toolkit';
import {
  filterPatternToExpression,
  SqlExpression,
  SqlLiteral,
  SqlQuery,
} from 'druid-query-toolkit';

import {
  changeByIndex,
  deleteKeys,
  filterOrReturn,
  isEmpty,
  mapRecord,
  mapRecordOrReturn,
} from '../../../utils';
import type { Rename } from '../utils';
import { renameColumnsInExpression } from '../utils';

import { ExpressionMeta } from './expression-meta';
import type { Measure } from './measure';
import { ModuleState } from './module-state';
import { QuerySource } from './query-source';

export type ExploreModuleLayout =
  | 'single'
  | 'two-by-two'
  | 'two-rows'
  | 'two-columns'
  | 'three-rows'
  | 'three-columns'
  | 'top-row-two-tiles'
  | 'bottom-row-two-tiles'
  | 'left-column-two-tiles'
  | 'right-column-two-tiles'
  | 'top-row-three-tiles'
  | 'bottom-row-three-tiles'
  | 'left-column-three-tiles'
  | 'right-column-three-tiles';

export const LAYOUT_TO_ICON: Record<ExploreModuleLayout, IconName> = {
  'single': IconNames.SYMBOL_RECTANGLE,
  'two-by-two': IconNames.GRID_VIEW,
  'two-rows': IconNames.LAYOUT_TWO_ROWS,
  'two-columns': IconNames.LAYOUT_TWO_COLUMNS,
  'three-rows': IconNames.LAYOUT_THREE_ROWS,
  'three-columns': IconNames.LAYOUT_THREE_COLUMNS,
  'top-row-two-tiles': IconNames.LAYOUT_TOP_ROW_TWO_TILES,
  'bottom-row-two-tiles': IconNames.LAYOUT_BOTTOM_ROW_TWO_TILES,
  'left-column-two-tiles': IconNames.LAYOUT_LEFT_COLUMN_TWO_TILES,
  'right-column-two-tiles': IconNames.LAYOUT_RIGHT_COLUMN_TWO_TILES,
  'top-row-three-tiles': IconNames.LAYOUT_TOP_ROW_THREE_TILES,
  'bottom-row-three-tiles': IconNames.LAYOUT_BOTTOM_ROW_THREE_TILES,
  'left-column-three-tiles': IconNames.LAYOUT_LEFT_COLUMN_THREE_TILES,
  'right-column-three-tiles': IconNames.LAYOUT_RIGHT_COLUMN_THREE_TILES,
};

interface ExploreStateValue {
  source: string;
  showSourceQuery?: boolean;
  timezone?: Timezone;
  where: SqlExpression;
  moduleStates: Readonly<Record<string, ModuleState>>;
  layout?: ExploreModuleLayout;
  hideResources?: boolean;
  helpers?: readonly ExpressionMeta[];
  hideHelpers?: boolean;
}

export class ExploreState {
  static DEFAULT_STATE: ExploreState;
  static LAYOUTS: ExploreModuleLayout[] = [
    'single',
    'two-by-two',
    'two-rows',
    'two-columns',
    'three-rows',
    'three-columns',
    'top-row-two-tiles',
    'bottom-row-two-tiles',
    'left-column-two-tiles',
    'right-column-two-tiles',
    'top-row-three-tiles',
    'bottom-row-three-tiles',
    'left-column-three-tiles',
    'right-column-three-tiles',
  ];

  static LAYOUT_TO_NUM_TILES: Record<ExploreModuleLayout, number> = {
    'single': 1,
    'two-by-two': 4,
    'two-rows': 2,
    'two-columns': 2,
    'three-rows': 3,
    'three-columns': 3,
    'top-row-two-tiles': 3,
    'bottom-row-two-tiles': 3,
    'left-column-two-tiles': 3,
    'right-column-two-tiles': 3,
    'top-row-three-tiles': 4,
    'bottom-row-three-tiles': 4,
    'left-column-three-tiles': 4,
    'right-column-three-tiles': 4,
  };

  static fromJS(js: any) {
    let moduleStatesJS: Readonly<Record<string, any>> = {};
    if (js.moduleStates) {
      moduleStatesJS = js.moduleStates;
    } else if (js.moduleId && js.parameterValues) {
      moduleStatesJS = { '0': js };
    }
    return new ExploreState({
      ...js,
      timezone: js.timezone ? Timezone.fromJS(js.timezone) : undefined,
      where: SqlExpression.maybeParse(js.where) || SqlLiteral.TRUE,
      moduleStates: mapRecord(moduleStatesJS, ModuleState.fromJS),
      helpers: ExpressionMeta.inflateArray(js.helpers || []),
    });
  }

  public readonly source: string;
  public readonly showSourceQuery: boolean;
  public readonly timezone?: Timezone;
  public readonly where: SqlExpression;
  public readonly moduleStates: Readonly<Record<string, ModuleState>>;
  public readonly layout?: ExploreModuleLayout;
  public readonly hideResources: boolean;
  public readonly helpers: readonly ExpressionMeta[];
  public readonly hideHelpers: boolean;

  public readonly parsedSource: SqlQuery | undefined;
  public readonly parseError: string | undefined;

  constructor(value: ExploreStateValue) {
    this.source = value.source;
    this.showSourceQuery = Boolean(value.showSourceQuery);
    this.timezone = value.timezone;
    this.where = value.where;
    this.moduleStates = value.moduleStates;
    this.layout = value.layout;
    this.hideResources = Boolean(value.hideResources);
    this.helpers = value.helpers || [];
    this.hideHelpers = Boolean(value.hideHelpers);

    if (this.source === '') {
      this.parseError = 'Please select source or enter a source query';
    } else {
      try {
        this.parsedSource = SqlQuery.parse(this.source);
      } catch (e) {
        this.parseError = e.message;
      }
    }
  }

  valueOf(): ExploreStateValue {
    const value: ExploreStateValue = {
      source: this.source,
      where: this.where,
      moduleStates: this.moduleStates,
    };
    if (this.layout) value.layout = this.layout;
    if (this.showSourceQuery) value.showSourceQuery = true;
    if (this.timezone) value.timezone = this.timezone;
    if (this.hideResources) value.hideResources = true;
    if (this.helpers.length) value.helpers = this.helpers;
    if (this.hideHelpers) value.hideHelpers = true;
    return value;
  }

  public change(newValues: Partial<ExploreStateValue>): ExploreState {
    return new ExploreState({
      ...this.valueOf(),
      ...newValues,
    });
  }

  public changeSource(newSource: SqlQuery | string, rename: Rename | undefined): ExploreState {
    const toChange: Partial<ExploreStateValue> = {
      source: String(newSource),
    };

    if (rename) {
      toChange.where = renameColumnsInExpression(this.where, rename);
      toChange.moduleStates = mapRecordOrReturn(this.moduleStates, moduleState =>
        moduleState.applyRename(rename),
      );
      toChange.helpers = this.helpers.map(helper => helper.applyRename(rename));
    }

    return this.change(toChange);
  }

  public getLayout(): ExploreModuleLayout {
    return this.layout || 'single';
  }

  public changeToTable(tableName: string): ExploreState {
    return this.changeSource(SqlQuery.selectStarFrom(tableName), undefined);
  }

  public initToTable(tableName: string): ExploreState {
    const { moduleStates } = this;
    return this.change({
      source: SqlQuery.selectStarFrom(tableName).toString(),
      moduleStates: isEmpty(moduleStates) ? {} : moduleStates,
    });
  }

  public addInitTimeFilterIfNeeded(columns: readonly Column[]): ExploreState {
    if (!this.parsedSource) return this;
    if (!QuerySource.isSingleStarQuery(this.parsedSource)) return this; // Only trigger for `SELECT * FROM ...` queries
    if (!this.where.equals(SqlLiteral.TRUE)) return this;

    // Either find the `__time::TIMESTAMP` column or use the first column if it is a TIMESTAMP
    const timeColumn =
      columns.find(c => c.isTimeColumn()) ||
      (columns[0].sqlType === 'TIMESTAMP' ? columns[0] : undefined);
    if (!timeColumn) return this;

    return this.change({
      where: filterPatternToExpression({
        type: 'timeRelative',
        column: timeColumn.name,
        negated: false,
        anchor: 'maxDataTime',
        rangeDuration: 'P1D',
        startBound: '[',
        endBound: ')',
      }),
    });
  }

  public restrictToQuerySource(querySource: QuerySource): ExploreState {
    const { where, moduleStates, helpers } = this;
    const newWhere = querySource.restrictWhere(where);
    const newModuleStates = mapRecordOrReturn(moduleStates, moduleState =>
      moduleState.restrictToQuerySource(querySource, newWhere),
    );
    const newHelpers = filterOrReturn(helpers, helper =>
      querySource.validateExpressionMeta(helper),
    );
    if (where === newWhere && moduleStates === newModuleStates && helpers === newHelpers)
      return this;

    return this.change({
      where: newWhere,
      moduleStates: newModuleStates,
      helpers: newHelpers,
    });
  }

  public changeModuleState(k: number, moduleState: ModuleState): ExploreState {
    return this.change({
      moduleStates: { ...this.moduleStates, [k]: moduleState },
    });
  }

  public removeModule(k: number): ExploreState {
    return this.change({
      moduleStates: deleteKeys(this.moduleStates, [String(k)]),
    });
  }

  public changeTimezone(timezone: Timezone | undefined): ExploreState {
    return this.change({ timezone });
  }

  public getEffectiveTimezone(): Timezone {
    return this.timezone || Timezone.UTC;
  }

  public applyShowColumn(column: Column, k = 0): ExploreState {
    const { moduleStates } = this;
    return this.change({
      moduleStates: {
        ...moduleStates,
        [k]: (moduleStates[k] || ModuleState.INIT_STATE).applyShowColumn(column),
      },
    });
  }

  public applyShowMeasure(measure: Measure, k = 0): ExploreState {
    const { moduleStates } = this;
    return this.change({
      moduleStates: {
        ...moduleStates,
        [k]: (moduleStates[k] || ModuleState.INIT_STATE).applyShowMeasure(measure),
      },
    });
  }

  public removeHelper(index: number): ExploreState {
    return this.change({ helpers: changeByIndex(this.helpers, index, () => undefined) });
  }

  public addHelper(helper: ExpressionMeta): ExploreState {
    return this.change({ helpers: this.helpers.concat(helper) });
  }

  public getModuleStatesToShow(): (ModuleState | null)[] {
    const moduleStates = this.moduleStates;
    const numberToShow = ExploreState.LAYOUT_TO_NUM_TILES[this.getLayout()];
    const ret: (ModuleState | null)[] = [];
    for (let i = 0; i < numberToShow; i++) {
      ret.push(moduleStates[i] || null);
    }
    return ret;
  }

  public isInitState(): boolean {
    return (
      this.source === '' &&
      !this.timezone &&
      this.where instanceof SqlLiteral &&
      isEmpty(this.moduleStates) &&
      !this.hideResources &&
      !this.helpers.length &&
      !this.hideHelpers
    );
  }
}

ExploreState.DEFAULT_STATE = new ExploreState({
  source: '',
  where: SqlLiteral.TRUE,
  moduleStates: {},
});
