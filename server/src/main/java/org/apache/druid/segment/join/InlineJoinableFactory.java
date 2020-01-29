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

import org.apache.druid.query.DataSource;
import org.apache.druid.query.InlineDataSource;
import org.apache.druid.segment.join.table.IndexedTable;
import org.apache.druid.segment.join.table.IndexedTableJoinable;
import org.apache.druid.segment.join.table.RowBasedIndexedTable;

import java.util.List;
import java.util.Optional;

/**
 * A {@link JoinableFactory} for {@link InlineDataSource}. It works by building an {@link IndexedTable}.
 */
public class InlineJoinableFactory implements JoinableFactory
{
  @Override
  public Optional<Joinable> build(final DataSource dataSource, final JoinConditionAnalysis condition)
  {
    if (condition.canHashJoin() && dataSource instanceof InlineDataSource) {
      final InlineDataSource inlineDataSource = (InlineDataSource) dataSource;
      final List<String> rightKeyColumns = condition.getRightKeyColumns();

      return Optional.of(
          new IndexedTableJoinable(
              new RowBasedIndexedTable<>(
                  inlineDataSource.getRowsAsList(),
                  inlineDataSource.rowAdapter(),
                  inlineDataSource.getRowSignature(),
                  rightKeyColumns
              )
          )
      );
    } else {
      return Optional.empty();
    }
  }
}
