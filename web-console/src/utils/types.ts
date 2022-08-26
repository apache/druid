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

import { IconName } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import { Column } from 'druid-query-toolkit';

function getEffectiveColumnType(column: Column): string | undefined {
  if (column.sqlType === 'TIMESTAMP') return column.sqlType;
  return column.nativeType || column.sqlType;
}

export function sqlTypeFromDruid(druidType: string): string {
  druidType = druidType.toLowerCase();
  switch (druidType) {
    case 'string':
      return 'VARCHAR';

    case 'long':
      return 'BIGINT';

    case 'float':
    case 'double':
      return druidType.toUpperCase();

    default:
      return 'COMPLEX';
  }
}

export function columnToIcon(column: Column): IconName | undefined {
  const effectiveType = getEffectiveColumnType(column);
  return effectiveType ? dataTypeToIcon(effectiveType) : undefined;
}

export function dataTypeToIcon(dataType: string): IconName {
  const typeUpper = dataType.toUpperCase();

  switch (typeUpper) {
    case 'TIMESTAMP':
      return IconNames.TIME;

    case 'VARCHAR':
    case 'STRING':
      return IconNames.FONT;

    case 'BIGINT':
    case 'DECIMAL':
    case 'REAL':
    case 'LONG':
    case 'FLOAT':
    case 'DOUBLE':
      return IconNames.NUMERICAL;

    case 'ARRAY<STRING>':
      return IconNames.ARRAY_STRING;

    case 'ARRAY<LONG>':
    case 'ARRAY<FLOAT>':
    case 'ARRAY<DOUBLE>':
      return IconNames.ARRAY_NUMERIC;

    case 'COMPLEX<JSON>':
      return IconNames.DIAGRAM_TREE;

    default:
      if (typeUpper.startsWith('ARRAY')) return IconNames.ARRAY;
      if (typeUpper.startsWith('COMPLEX')) return IconNames.ASTERISK;
      return IconNames.HELP;
  }
}

export function columnToWidth(column: Column): number {
  const effectiveType = getEffectiveColumnType(column);
  return effectiveType ? dataTypeToWidth(effectiveType) : 180;
}

export function dataTypeToWidth(dataType: string | undefined): number {
  const typeUpper = String(dataType).toUpperCase();

  switch (typeUpper) {
    case 'TIMESTAMP':
      return 180;

    case 'VARCHAR':
    case 'STRING':
      return 150;

    case 'BIGINT':
    case 'DECIMAL':
    case 'REAL':
    case 'LONG':
    case 'FLOAT':
    case 'DOUBLE':
      return 120;

    case 'ARRAY<STRING>':
      return 200;

    case 'ARRAY<LONG>':
    case 'ARRAY<FLOAT>':
    case 'ARRAY<DOUBLE>':
      return 180;

    case 'COMPLEX<JSON>':
      return 300;

    default:
      if (typeUpper.startsWith('ARRAY')) return 200;
      if (typeUpper.startsWith('COMPLEX')) return 150;
      return 180;
  }
}
