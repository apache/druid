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

import org.apache.druid.data.input.Row;

import java.util.function.Function;
import java.util.function.ToLongFunction;

/**
 * Utility class for creating {@link RowAdapter}.
 */
public class RowAdapters
{
  private static final RowAdapter<? extends Row> STANDARD_ROW_ADAPTER = new RowAdapter<Row>()
  {
    @Override
    public ToLongFunction<Row> timestampFunction()
    {
      return Row::getTimestampFromEpoch;
    }

    @Override
    public Function<Row, Object> columnFunction(String columnName)
    {
      return r -> r.getRaw(columnName);
    }
  };

  private RowAdapters()
  {
    // No instantiation.
  }

  /**
   * Returns a {@link RowAdapter} that handles any kind of input {@link Row}.
   */
  public static <RowType extends Row> RowAdapter<RowType> standardRow()
  {
    //noinspection unchecked
    return (RowAdapter<RowType>) STANDARD_ROW_ADAPTER;
  }
}
