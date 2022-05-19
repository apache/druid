/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.segment;

import org.apache.druid.segment.column.ColumnHolder;

import java.util.function.Function;
import java.util.function.ToLongFunction;

/**
 * An adapter between arbitrary types and the needs of callers that want to read specific columns out of those
 * types (treating them as rows).
 */
public interface RowAdapter<RowType>
{
  /**
   * Returns a function that retrieves timestamps from rows.
   *
   * The default implementation delegates to {@link #columnFunction} and expects it to already contain long-typed
   * values or nulls. Nulls, if present, will be converted to zeroes.
   */
  default ToLongFunction<RowType> timestampFunction()
  {
    final Function<RowType, Object> timeColumnFunction = columnFunction(ColumnHolder.TIME_COLUMN_NAME);
    return row -> {
      final Object obj = timeColumnFunction.apply(row);

      if (obj == null) {
        return 0L;
      } else {
        return (long) obj;
      }
    };
  }

  /**
   * Returns a function that retrieves the value for column "columnName" from rows.
   */
  Function<RowType, Object> columnFunction(String columnName);
}
