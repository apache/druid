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

package org.apache.druid.segment.join;

import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.InlineDataSource;
import org.apache.druid.segment.join.table.IndexedTable;
import org.apache.druid.segment.join.table.IndexedTableJoinable;
import org.apache.druid.segment.join.table.RowBasedIndexedTable;

import java.util.Optional;
import java.util.Set;

/**
 * A {@link JoinableFactory} for {@link InlineDataSource}. It works by building an {@link IndexedTable}.
 *
 * It is not valid to pass any other DataSource type to the "build" method.
 */
public class InlineJoinableFactory implements JoinableFactory
{
  @Override
  public boolean isDirectlyJoinable(DataSource dataSource)
  {
    // this should always be true if this is access through MapJoinableFactory, but check just in case...
    // further, this should not ever be legitimately called, because this method is used to avoid subquery joins
    // which use the InlineJoinableFactory
    return dataSource instanceof InlineDataSource;
  }

  @Override
  public Optional<Joinable> build(final DataSource dataSource, final JoinConditionAnalysis condition)
  {
    final InlineDataSource inlineDataSource = (InlineDataSource) dataSource;

    if (condition.canHashJoin()) {
      final Set<String> rightKeyColumns = condition.getRightEquiConditionKeys();

      return Optional.of(
          new IndexedTableJoinable(
              new RowBasedIndexedTable<>(
                  inlineDataSource.getRowsAsList(),
                  inlineDataSource.rowAdapter(),
                  inlineDataSource.getRowSignature(),
                  rightKeyColumns,
                  DateTimes.nowUtc().toString()
              )
          )
      );
    } else {
      return Optional.empty();
    }
  }
}
