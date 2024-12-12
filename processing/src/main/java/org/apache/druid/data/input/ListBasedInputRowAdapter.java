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

package org.apache.druid.data.input;

import org.apache.druid.segment.RowAdapter;
import org.apache.druid.segment.RowAdapters;
import org.apache.druid.segment.column.RowSignature;

import java.util.function.Function;
import java.util.function.ToLongFunction;

/**
 * Adapter for reading {@link ListBasedInputRow}. The {@link RowAdapters#standardRow()} would also work, but this
 * one is faster because it avoids per-row field-name-to-index lookups. Must be instantiated with the same
 * {@link RowSignature} as the {@link ListBasedInputRow} that are to be parsed.
 */
public class ListBasedInputRowAdapter implements RowAdapter<InputRow>
{
  private final RowSignature fields;

  public ListBasedInputRowAdapter(final RowSignature fields)
  {
    this.fields = fields;
  }

  @Override
  public ToLongFunction<InputRow> timestampFunction()
  {
    return Row::getTimestampFromEpoch;
  }

  @Override
  public Function<InputRow, Object> columnFunction(String columnName)
  {
    final int i = fields.indexOf(columnName);
    if (i < 0) {
      return row -> null;
    } else {
      // Get by column number, not name.
      return row -> ((ListBasedInputRow) row).getRaw(i);
    }
  }
}
