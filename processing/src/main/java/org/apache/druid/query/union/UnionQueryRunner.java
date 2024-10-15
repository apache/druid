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
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QuerySegmentWalker;
import org.apache.druid.query.context.ResponseContext;

import java.util.ArrayList;
import java.util.List;

class UnionQueryRunner implements QueryRunner<RealUnionResult>
{
  private final QuerySegmentWalker walker;
  private final UnionQuery query;
  private final List<QueryRunner> runners;

  public UnionQueryRunner(
      UnionQuery query,
      QuerySegmentWalker walker
  )
  {
    this.query = query;
    this.walker = walker;
    this.runners = makeSubQueryRunners(query);
  }

  private List<QueryRunner> makeSubQueryRunners(UnionQuery unionQuery)
  {
    List<QueryRunner> runners = new ArrayList<>();
    for (Query<?> query : unionQuery.queries) {
      runners.add(query.getRunner(walker));
    }
    return runners;

  }

  @Override
  public Sequence<RealUnionResult> run(QueryPlus<RealUnionResult> queryPlus, ResponseContext responseContext)
  {
    UnionQuery unionQuery = queryPlus.unwrapQuery(UnionQuery.class);

    List<RealUnionResult> seqs = new ArrayList<RealUnionResult>();
    for (int i = 0; i < runners.size(); i++) {
      Query<?> q = unionQuery.queries.get(i);
      QueryRunner r = runners.get(i);
      seqs.add(makeUnionResult(r, queryPlus.withQuery(q), responseContext));
    }
    return Sequences.simple(seqs);
  }

  private <T> RealUnionResult makeUnionResult(QueryRunner runner, QueryPlus<T> withQuery,
      ResponseContext responseContext)
  {
    Sequence<T> seq = runner.run(withQuery, responseContext);
    return new RealUnionResult(seq);
  }
}
