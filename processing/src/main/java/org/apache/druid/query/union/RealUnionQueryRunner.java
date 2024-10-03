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

package org.apache.druid.query.union;

import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QuerySegmentWalker;
import org.apache.druid.query.Result;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.rowsandcols.RowsAndColumns;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.query.timeseries.TimeseriesQueryQueryToolChest;
import org.apache.druid.query.timeseries.TimeseriesResultValue;

import java.util.List;

public class RealUnionQueryRunner implements QueryRunner<RealUnionResult>
{
  private QuerySegmentWalker walker;

  public RealUnionQueryRunner(QuerySegmentWalker walker)
  {
    this.walker = walker;
  }

  @Override
  public Sequence<RealUnionResult> run(QueryPlus<RealUnionResult> queryPlus, ResponseContext responseContext)
  {
    UnionQuery unionQuery = queryPlus.unwrapQuery(UnionQuery.class);

//    for (Query<?> q: unionQuery.queries) {
//      if(q instanceof TimeseriesQuery) {
//        runTsQuery(queryPlus,(TimeseriesQuery)q, responseContext);
//      }
//    }
    return Sequences.empty();
  }

  private void runTsQuery(QueryPlus<RowsAndColumns> queryPlus, TimeseriesQuery q, ResponseContext responseContext)
  {
    QueryRunner<Result<TimeseriesResultValue>> runner = q.getRunner(walker);
    Sequence<Result<TimeseriesResultValue>> res = runner.run(queryPlus.withQuery(q), responseContext);

    TimeseriesQueryQueryToolChest tsToolChest = new TimeseriesQueryQueryToolChest();
    Sequence<Object[]> res1 = tsToolChest.resultsAsArrays(q, res);
    List<Object[]> li = res1.toList();
//    tsToolChest.mergeResults()

  }



}
