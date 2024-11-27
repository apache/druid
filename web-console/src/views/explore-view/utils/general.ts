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

import type { SqlExpression } from '@druid-toolkit/query';
import { SqlColumn } from '@druid-toolkit/query';

export function addTableScope(expression: SqlExpression, newTableScope: string): SqlExpression {
  return expression.walk(ex => {
    if (ex instanceof SqlColumn && !ex.getTableName()) {
      return ex.changeTableName(newTableScope);
    }
    return ex;
  }) as SqlExpression;
}

export type Rename = Map<string, string>;

export function renameColumnsInExpression(
  expression: SqlExpression,
  rename: Rename,
): SqlExpression {
  return expression.walk(ex => {
    if (ex instanceof SqlColumn) {
      const renameTo = rename.get(ex.getName());
      if (renameTo) {
        return ex.changeName(renameTo);
      }
    }
    return ex;
  }) as SqlExpression;
}

export function changeOrAdd<T>(xs: readonly T[], oldValue: T | undefined, newValue: T): T[] {
  if (typeof oldValue === 'undefined') {
    return xs.concat([newValue]);
  } else {
    return xs.map(x => (x === oldValue ? newValue : x));
  }
}
