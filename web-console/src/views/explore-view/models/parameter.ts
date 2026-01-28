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

/* eslint-disable @typescript-eslint/no-empty-object-type */

import type { SqlExpression } from 'druid-query-toolkit';

import { deleteKeys, mapRecord, mapRecordOrReturn } from '../../../utils';

import { ExpressionMeta } from './expression-meta';
import { Measure } from './measure';
import type { QuerySource } from './query-source';

export type OptionValue = string | number;

function isOptionValue(v: unknown): v is OptionValue {
  return typeof v === 'string' || typeof v === 'number';
}

export type ModuleFunctor<T> =
  | T
  | ((options: {
      querySource: QuerySource | undefined;
      where: SqlExpression;
      parameterValues: ParameterValues;
    }) => T);

export function evaluateFunctor<T>(
  functor: ModuleFunctor<T> | undefined,
  parameterValues: ParameterValues,
  querySource: QuerySource | undefined,
  where: SqlExpression,
): T | undefined {
  if (typeof functor === 'function') {
    return (functor as any)({ where, parameterValues, querySource });
  } else {
    return functor;
  }
}

export interface ParameterTypes {
  string: string;
  boolean: boolean;
  number: number;
  option: OptionValue;
  options: readonly OptionValue[];
  expression: ExpressionMeta;
  expressions: readonly ExpressionMeta[];
  measure: Measure;
  measures: readonly Measure[];
}

type OptionLabels = { [key: string | number]: string } | ((x: string) => string);

interface TypedExtensions {
  boolean: {};
  string: {};
  number: {
    min?: number;
    max?: number;
  };
  option: {
    options: ModuleFunctor<readonly OptionValue[]>;
    optionLabels?: OptionLabels;
  };
  options: {
    options: ModuleFunctor<readonly OptionValue[]>;
    optionLabels?: OptionLabels;
    allowDuplicates?: boolean;
    nonEmpty?: boolean;
  };
  expression: {};
  expressions: {
    allowDuplicates?: boolean;
    nonEmpty?: boolean;
  };
  measure: {};
  measures: {
    allowDuplicates?: boolean;
    nonEmpty?: boolean;
  };
}

export type TypedParameterDefinition<Type extends keyof ParameterTypes> = TypedExtensions[Type] & {
  label?: ModuleFunctor<string>;
  type: Type;
  transferGroup?: string;
  defaultValue?: ModuleFunctor<ParameterTypes[Type] | undefined>;
  sticky?: boolean;
  required?: ModuleFunctor<boolean>;
  important?: boolean;
  description?: ModuleFunctor<string>;
  placeholder?: string;
  defined?: ModuleFunctor<boolean>;
  visible?: ModuleFunctor<boolean>;
  legacyName?: string;
};

export type ParameterDefinition =
  | TypedParameterDefinition<'string'>
  | TypedParameterDefinition<'boolean'>
  | TypedParameterDefinition<'number'>
  | TypedParameterDefinition<'option'>
  | TypedParameterDefinition<'options'>
  | TypedParameterDefinition<'expression'>
  | TypedParameterDefinition<'expressions'>
  | TypedParameterDefinition<'measure'>
  | TypedParameterDefinition<'measures'>;

/**
 * Returns the label for a plugin option.
 *
 * @param optionValue the option value to get the label for
 * @param parameterDefinition the parameter definition that the option belongs to
 * @returns the label for the option
 */
export function getModuleOptionLabel(
  optionValue: OptionValue,
  parameterDefinition: ParameterDefinition,
): string {
  const { optionLabels } = parameterDefinition as any;

  if (typeof optionLabels === 'function') {
    const l = optionLabels(optionValue);
    if (typeof l !== 'undefined') return l;
  }

  if (optionLabels && typeof optionLabels === 'object') {
    const l = optionLabels[optionValue];
    if (typeof l !== 'undefined') return l;
  }

  return typeof optionValue !== 'undefined' ? String(optionValue) : 'Malformed option';
}

export type ParameterValues = Readonly<Record<string, any>>;
export type Parameters = Readonly<Record<string, ParameterDefinition>>;

// -----------------------------------------------------

export function inflateParameterValues(
  parameterValues: ParameterValues | undefined,
  parameters: Parameters,
): ParameterValues {
  return mapRecord(parameters, (parameter, parameterName) =>
    inflateParameterValue(
      parameterValues?.[parameterName] ??
        (parameter.legacyName ? parameterValues?.[parameter.legacyName] : undefined),
      parameter,
    ),
  );
}

function inflateParameterValue(value: unknown, parameter: ParameterDefinition): any {
  if (typeof value === 'undefined') return;
  switch (parameter.type) {
    case 'boolean':
      return Boolean(value);

    case 'number': {
      const v = Number(value);
      if (isNaN(v)) return;
      return v;
    }

    case 'option': {
      if (!isOptionValue(value)) return;
      return value;
    }

    case 'options':
      if (!Array.isArray(value)) return [];
      return value.filter(isOptionValue);

    case 'expression':
      return ExpressionMeta.inflate(value);

    case 'measure':
      return Measure.inflate(value);

    case 'expressions':
      return ExpressionMeta.inflateArray(value);

    case 'measures':
      return Measure.inflateArray(value);

    default:
      return value as any;
  }
}

// -----------------------------------------------------

export function removeUndefinedParameterValues(
  parameterValues: ParameterValues,
  parameters: Parameters,
  querySource: QuerySource | undefined,
  where: SqlExpression,
): ParameterValues {
  const keysToRemove = Object.keys(parameterValues).filter(key => {
    const parameter = parameters[key];
    if (!parameter) return true;
    return (
      typeof parameter.defined !== 'undefined' &&
      !evaluateFunctor(parameter.defined, parameterValues, querySource, where)
    );
  });
  return keysToRemove.length ? deleteKeys(parameterValues, keysToRemove) : parameterValues;
}

// -----------------------------------------------------

function defaultForType(parameterType: keyof ParameterTypes): any {
  switch (parameterType) {
    case 'boolean':
      return false;

    case 'expressions':
    case 'measures':
      return [];

    default:
      return;
  }
}

export function effectiveParameterDefault(
  parameter: ParameterDefinition,
  parameterValues: ParameterValues,
  querySource: QuerySource | undefined,
  where: SqlExpression,
  previousParameterValue: any,
): any {
  if (
    typeof parameter.defined !== 'undefined' &&
    evaluateFunctor(parameter.defined, parameterValues, querySource, where) === false
  ) {
    return;
  }
  const newDefault =
    evaluateFunctor(parameter.defaultValue, parameterValues, querySource, where) ??
    defaultForType(parameter.type);

  if (previousParameterValue instanceof Measure && previousParameterValue.equals(newDefault)) {
    return previousParameterValue;
  }

  return newDefault;
}

// -----------------------------------------------------

export function renameColumnsInParameterValues(
  parameterValues: ParameterValues,
  parameters: Parameters,
  rename: Map<string, string>,
): ParameterValues {
  return mapRecordOrReturn(parameterValues, (parameterValue, k) =>
    renameColumnsInParameterValue(parameterValue, parameters[k], rename),
  );
}

function renameColumnsInParameterValue(
  parameterValue: any,
  parameter: ParameterDefinition,
  rename: Map<string, string>,
): any {
  if (typeof parameterValue !== 'undefined') {
    switch (parameter.type) {
      case 'expression':
        return (parameterValue as ExpressionMeta).applyRename(rename);

      case 'measure':
        return (parameterValue as Measure).applyRename(rename);

      case 'expressions':
      case 'measures':

      default:
        break;
    }
  }
  return parameterValue;
}
