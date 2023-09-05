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

import type { IconName } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import type { Column } from '@druid-toolkit/query';

export function columnToSummary(column: Column): string {
  const lines: string[] = [column.name];
  if (column.sqlType) lines.push(`SQL type: ${column.sqlType}`);
  if (column.nativeType) lines.push(`Native type: ${column.nativeType}`);
  return lines.join('\n');
}

function getEffectiveColumnType(column: Column): string | undefined {
  if (column.sqlType === 'TIMESTAMP' || column.sqlType === 'BOOLEAN') return column.sqlType;
  return column.nativeType || column.sqlType;
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

    case 'BOOLEAN':
      return IconNames.SEGMENTED_CONTROL;

    case 'VARCHAR':
    case 'STRING':
      return IconNames.FONT;

    case 'BIGINT':
    case 'LONG':
      return IconNames.NUMERICAL;

    case 'DECIMAL':
    case 'REAL':
    case 'FLOAT':
    case 'DOUBLE':
      return IconNames.FLOATING_POINT;

    case 'ARRAY<STRING>':
      return IconNames.ARRAY_STRING;

    case 'ARRAY<LONG>':
      return IconNames.ARRAY_NUMERIC;

    case 'ARRAY<FLOAT>':
    case 'ARRAY<DOUBLE>':
      return IconNames.ARRAY_FLOATING_POINT;

    case 'COMPLEX<JSON>':
      return IconNames.DIAGRAM_TREE;

    case 'COMPLEX<HYPERUNIQUE>':
    case 'COMPLEX<HLLSKETCHBUILD>':
    case 'COMPLEX<THETASKETCHBUILD>':
      return IconNames.SNOWFLAKE;

    case 'COMPLEX<QUANTILESDOUBLESSKETCH>':
      return IconNames.HORIZONTAL_DISTRIBUTION;

    case 'COMPLEX<VARIANCE>':
      return IconNames.ALIGNMENT_HORIZONTAL_CENTER;

    case 'COMPLEX<IPADDRESS>':
    case 'COMPLEX<IPPREFIX>':
      return IconNames.IP_ADDRESS;

    case 'COMPLEX<SERIALIZABLEPAIRLONGSTRING>':
      return IconNames.DOUBLE_CHEVRON_RIGHT;

    case 'NULL':
      return IconNames.CIRCLE;

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

    case 'BOOLEAN':
      return 100;

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
