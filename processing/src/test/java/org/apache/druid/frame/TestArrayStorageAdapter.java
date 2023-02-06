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

package org.apache.druid.frame;

import com.google.common.collect.Iterables;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.QueryableIndexStorageAdapter;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;

import java.util.Optional;

public class TestArrayStorageAdapter extends QueryableIndexStorageAdapter
{
  public TestArrayStorageAdapter(QueryableIndex index)
  {
    super(index);
  }

  @Override
  public RowSignature getRowSignature()
  {
    final RowSignature.Builder builder = RowSignature.builder();
    builder.addTimeColumn();

    for (final String column : Iterables.concat(getAvailableDimensions(), getAvailableMetrics())) {
      Optional<ColumnCapabilities> columnCapabilities = Optional.ofNullable(getColumnCapabilities(column));
      ColumnType columnType = columnCapabilities.isPresent() ? columnCapabilities.get().toColumnType() : null;
      //change MV columns to Array<String>
      if (columnCapabilities.isPresent() && columnCapabilities.get().hasMultipleValues().isMaybeTrue()) {
        columnType = ColumnType.STRING_ARRAY;
      }
      builder.add(column, columnType);
    }

    return builder.build();
  }
}
