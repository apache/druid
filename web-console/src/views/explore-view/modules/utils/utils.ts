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
import { F, SqlFunction, SqlLiteral } from '@druid-toolkit/query';

export function shiftTimeInWhere(where: SqlExpression, period: string): SqlExpression {
  return where.walk(ex => {
    if (ex instanceof SqlLiteral) {
      // Works with: __time < TIMESTAMP '2022-01-02 03:04:05'
      if (ex.isDate()) {
        return F('TIME_SHIFT', ex, period, -1);
      }
    } else if (ex instanceof SqlFunction) {
      const effectiveFunctionName = ex.getEffectiveFunctionName();

      // Works with: TIME_IN_INTERVAL(__time, '<interval>')
      if (effectiveFunctionName === 'TIME_IN_INTERVAL') {
        return ex.changeArgs(ex.args!.change(0, F('TIME_SHIFT', ex.getArg(0), period, 1)));
      }

      // Works with: TIME_SHIFT(...) <= __time
      //        and: __time < MAX_DATA_TIME()
      if (effectiveFunctionName === 'TIME_SHIFT' || effectiveFunctionName === 'MAX_DATA_TIME') {
        return F('TIME_SHIFT', ex, period, -1);
      }
    }

    return ex;
  }) as SqlExpression;
}
