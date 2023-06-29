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

package org.apache.druid.query.operator;

import com.google.common.collect.Iterables;
import org.apache.druid.query.QueryProcessingPool;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerFactory;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.rowsandcols.RowsAndColumns;
import org.apache.druid.segment.Segment;

public class WindowOperatorQueryQueryRunnerFactory implements QueryRunnerFactory<RowsAndColumns, WindowOperatorQuery>
{
  public static final WindowOperatorQueryQueryToolChest TOOLCHEST = new WindowOperatorQueryQueryToolChest();

  @Override
  public QueryRunner<RowsAndColumns> createRunner(Segment segment)
  {
    return (queryPlus, responseContext) ->
        new OperatorSequence(() -> new SegmentToRowsAndColumnsOperator(segment));
  }

  @Override
  public QueryRunner<RowsAndColumns> mergeRunners(
      QueryProcessingPool queryProcessingPool,
      Iterable<QueryRunner<RowsAndColumns>> queryRunners
  )
  {
    return Iterables.getOnlyElement(queryRunners);
  }

  @Override
  public QueryToolChest<RowsAndColumns, WindowOperatorQuery> getToolchest()
  {
    return TOOLCHEST;
  }
}
