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

import org.apache.druid.error.DruidException;
import org.apache.druid.frame.allocation.ArenaMemoryAllocatorFactory;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.FrameSignaturePair;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.query.QuerySegmentWalker;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.ResultSerializationMode;
import org.apache.druid.query.context.ResponseContext;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

class UnionQueryRunner implements QueryRunner<Object>
{
  private final QuerySegmentWalker walker;
  private final List<QueryRunner> runners;
  private QueryRunnerFactoryConglomerate conglomerate;
  private ResultSerializationMode serializationMode;
  private boolean useNestedForUnknownTypeInSubquery;

  public UnionQueryRunner(
      UnionQuery query,
      QuerySegmentWalker walker,
      QueryRunnerFactoryConglomerate conglomerate)
  {
    this.walker = walker;
    this.conglomerate = conglomerate;

    serializationMode = getResultSerializationMode(query);
    // FIXME: this was more complicated; it dependend on ServerConfig from the server module
    useNestedForUnknownTypeInSubquery = query.context().isUseNestedForUnknownTypeInSubquery(false);
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
  public Sequence<Object> run(QueryPlus<Object> queryPlus, ResponseContext responseContext)
  {
    UnionQuery unionQuery = queryPlus.unwrapQuery(UnionQuery.class);

    List<Sequence<Object>> seqs = new ArrayList<>();
    for (int i = 0; i < runners.size(); i++) {
      Query<?> q = unionQuery.queries.get(i);
      QueryRunner r = runners.get(i);
      seqs.add(makeResultSeq(r, queryPlus.withQuery(q), responseContext, serializationMode));
    }
    return Sequences.concat(seqs);
  }

  private ResultSerializationMode getResultSerializationMode(Query<Object> query)
  {
    ResultSerializationMode serializationMode = query.context().getEnum(
        ResultSerializationMode.CTX_SERIALIZATION_PARAMETER,
        ResultSerializationMode.class,
        null
    );
    if (serializationMode == null) {
      throw DruidException.defensive(
          "Serialization mode [%s] is not setup correctly!", ResultSerializationMode.CTX_SERIALIZATION_PARAMETER
      );
    }
    return serializationMode;
  }

  private <T> Sequence<Object> makeResultSeq(QueryRunner runner, QueryPlus<T> withQuery,
      ResponseContext responseContext, ResultSerializationMode serializationMode)
  {
    Query<T> query = withQuery.getQuery();
    QueryToolChest<T, Query<T>> toolChest = conglomerate.getToolChest(query);
    Sequence<T> seq = runner.run(withQuery, responseContext);
    Sequence<?> resultSeq;
    switch (serializationMode)
    {
      case ROWS:
        resultSeq = toolChest.resultsAsArrays(query, seq);
        break;
      case FRAMES:
        Optional<Sequence<FrameSignaturePair>> resultsAsFrames = toolChest.resultsAsFrames(
            query,
            seq,
            ArenaMemoryAllocatorFactory.makeDefault(),
            false
        );
        if (resultsAsFrames.isEmpty()) {
          throw DruidException.defensive("Unable to materialize the results as frames.");
        }
        resultSeq = resultsAsFrames.get();
        break;
      default:
        throw DruidException.defensive("Not supported serializationMode [%s].", serializationMode);
    }
    return (Sequence<Object>) resultSeq;
  }
}
