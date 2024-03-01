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

import type { SqlQuery } from '@druid-toolkit/query';
import {
  C,
  F,
  filterMap,
  L,
  SqlColumnDeclaration,
  SqlExpression,
  SqlFunction,
  SqlLiteral,
  SqlStar,
  SqlType,
} from '@druid-toolkit/query';
import * as JSONBig from 'json-bigint-native';

import { nonEmptyArray } from '../../utils';
import type { ArrayMode } from '../ingestion-spec/ingestion-spec';
import type { InputFormat } from '../input-format/input-format';
import type { InputSource } from '../input-source/input-source';

export const MULTI_STAGE_QUERY_MAX_COLUMNS = 2000;
const MAX_LINES = 10;

function joinLinesMax(lines: string[], max: number) {
  if (lines.length > max) {
    lines = lines.slice(0, max).concat(`(and ${lines.length - max} more)`);
  }
  return lines.join('\n');
}

export interface ExternalConfig {
  inputSource: InputSource;
  inputFormat: InputFormat;
  signature: readonly SqlColumnDeclaration[];
}

export function summarizeInputSource(inputSource: InputSource, multiline: boolean): string {
  switch (inputSource.type) {
    case 'inline':
      return `inline data`;

    case 'local':
      // ToDo: make this official
      if (nonEmptyArray((inputSource as any).files)) {
        let lines: string[] = (inputSource as any).files;
        if (!multiline) lines = lines.slice(0, 1);
        return joinLinesMax(lines, MAX_LINES);
      }
      return `${inputSource.baseDir || '?'}{${inputSource.filter || '?'}}`;

    case 'http':
      if (nonEmptyArray(inputSource.uris)) {
        let lines: string[] = inputSource.uris;
        if (!multiline) lines = lines.slice(0, 1);
        return joinLinesMax(lines, MAX_LINES);
      }
      return '?';

    case 's3':
    case 'google':
    case 'azureStorage': {
      const possibleLines = inputSource.uris || inputSource.prefixes;
      if (nonEmptyArray(possibleLines)) {
        let lines: string[] = possibleLines;
        if (!multiline) lines = lines.slice(0, 1);
        return joinLinesMax(lines, MAX_LINES);
      }
      if (nonEmptyArray(inputSource.objects)) {
        let lines: string[] = inputSource.objects.map(({ bucket, path }) => `${bucket}:${path}`);
        if (!multiline) lines = lines.slice(0, 1);
        return joinLinesMax(lines, MAX_LINES);
      }
      return '?';
    }

    case 'hdfs': {
      const paths =
        typeof inputSource.paths === 'string' ? inputSource.paths.split(',') : inputSource.paths;
      if (nonEmptyArray(paths)) {
        let lines: string[] = paths;
        if (!multiline) lines = lines.slice(0, 1);
        return joinLinesMax(lines, MAX_LINES);
      }
      return '?';
    }

    default:
      return String(inputSource.type);
  }
}

export function summarizeInputFormat(inputFormat: InputFormat): string {
  return String(inputFormat.type);
}

export function summarizeExternalConfig(externalConfig: ExternalConfig): string {
  return `${summarizeInputSource(externalConfig.inputSource, false)} [${summarizeInputFormat(
    externalConfig.inputFormat,
  )}]`;
}

export function externalConfigToTableExpression(config: ExternalConfig): SqlExpression {
  return SqlExpression.parse(`TABLE(
  EXTERN(
    ${L(JSONBig.stringify(config.inputSource))},
    ${L(JSONBig.stringify(config.inputFormat))}
  )
) EXTEND (${config.signature.join(', ')})`);
}

export function externalConfigToInitDimensions(
  config: ExternalConfig,
  timeExpression: SqlExpression | undefined,
  arrayMode: ArrayMode,
): SqlExpression[] {
  return (timeExpression ? [timeExpression.as('__time')] : [])
    .concat(
      filterMap(config.signature, columnDeclaration => {
        const columnName = columnDeclaration.getColumnName();
        if (timeExpression && timeExpression.containsColumnName(columnName)) return;
        return C(columnName).applyIf(
          arrayMode === 'multi-values' && columnDeclaration.columnType.isArray(),
          ex => F('ARRAY_TO_MV', ex).as(columnName),
        );
      }),
    )
    .slice(0, MULTI_STAGE_QUERY_MAX_COLUMNS);
}

export function fitExternalConfigPattern(query: SqlQuery): ExternalConfig {
  if (!(query.getSelectExpressionForIndex(0) instanceof SqlStar)) {
    throw new Error(`External SELECT must only be a star`);
  }

  const tableFn = query.fromClause?.expressions?.first();
  if (!(tableFn instanceof SqlFunction) || tableFn.getEffectiveFunctionName() !== 'TABLE') {
    throw new Error(`External FROM must be a TABLE function`);
  }

  const externFn = tableFn.getArg(0);
  if (!(externFn instanceof SqlFunction) || externFn.getEffectiveFunctionName() !== 'EXTERN') {
    throw new Error(`Within the TABLE function there must be an extern function`);
  }

  let inputSource: any;
  try {
    const arg0 = externFn.getArg(0);
    inputSource = JSONBig.parse(arg0 instanceof SqlLiteral ? String(arg0.value) : '#');
  } catch {
    throw new Error(`The first argument to the extern function must be a string embedding JSON`);
  }

  let inputFormat: any;
  try {
    const arg1 = externFn.getArg(1);
    inputFormat = JSONBig.parse(arg1 instanceof SqlLiteral ? String(arg1.value) : '#');
  } catch {
    throw new Error(`The second argument to the extern function must be a string embedding JSON`);
  }

  let signature: readonly SqlColumnDeclaration[];
  const columnDeclarations = tableFn.getColumnDeclarations();
  if (columnDeclarations) {
    signature = columnDeclarations;
  } else {
    try {
      const arg2 = externFn.getArg(2);
      signature = JSONBig.parse(arg2 instanceof SqlLiteral ? String(arg2.value) : '#').map(
        (c: any) => SqlColumnDeclaration.create(c.name, SqlType.fromNativeType(c.type)),
      );
    } catch {
      throw new Error(`The third argument to the extern function must be a string embedding JSON`);
    }
  }

  return {
    inputSource,
    inputFormat,
    signature,
  };
}
