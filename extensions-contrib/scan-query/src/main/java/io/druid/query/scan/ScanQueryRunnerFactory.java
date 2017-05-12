/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.druid.query.scan;

import com.google.common.base.Function;
import com.google.inject.Inject;
import io.druid.common.utils.JodaUtils;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.Sequences;
import io.druid.query.Query;
import io.druid.query.QueryContexts;
import io.druid.query.QueryPlus;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QueryToolChest;
import io.druid.segment.Segment;

import java.util.Map;
import java.util.concurrent.ExecutorService;

public class ScanQueryRunnerFactory implements QueryRunnerFactory<ScanResultValue, ScanQuery>
{
  // This variable indicates when a running query should be expired,
  // and is effective only when 'timeout' of queryContext has a positive value.
  public static final String CTX_TIMEOUT_AT = "timeoutAt";
  public static final String CTX_COUNT = "count";
  private final ScanQueryQueryToolChest toolChest;
  private final ScanQueryEngine engine;

  @Inject
  public ScanQueryRunnerFactory(
      ScanQueryQueryToolChest toolChest,
      ScanQueryEngine engine
  )
  {
    this.toolChest = toolChest;
    this.engine = engine;
  }

  @Override
  public QueryRunner<ScanResultValue> createRunner(Segment segment)
  {
    return new ScanQueryRunner(engine, segment);
  }

  @Override
  public QueryRunner<ScanResultValue> mergeRunners(
      ExecutorService queryExecutor,
      final Iterable<QueryRunner<ScanResultValue>> queryRunners
  )
  {
    // in single thread and in jetty thread instead of processing thread
    return new QueryRunner<ScanResultValue>()
    {
      @Override
      public Sequence<ScanResultValue> run(
          final QueryPlus<ScanResultValue> queryPlus, final Map<String, Object> responseContext
      )
      {
        // Note: this variable is effective only when queryContext has a timeout.
        // See the comment of CTX_TIMEOUT_AT.
        final long timeoutAt = System.currentTimeMillis() + QueryContexts.getTimeout(queryPlus.getQuery());
        responseContext.put(CTX_TIMEOUT_AT, timeoutAt);
        return Sequences.concat(
            Sequences.map(
                Sequences.simple(queryRunners),
                new Function<QueryRunner<ScanResultValue>, Sequence<ScanResultValue>>()
                {
                  @Override
                  public Sequence<ScanResultValue> apply(final QueryRunner<ScanResultValue> input)
                  {
                    return input.run(queryPlus, responseContext);
                  }
                }
            )
        );
      }
    };
  }

  @Override
  public QueryToolChest<ScanResultValue, ScanQuery> getToolchest()
  {
    return toolChest;
  }

  private static class ScanQueryRunner implements QueryRunner<ScanResultValue>
  {
    private final ScanQueryEngine engine;
    private final Segment segment;

    public ScanQueryRunner(ScanQueryEngine engine, Segment segment)
    {
      this.engine = engine;
      this.segment = segment;
    }

    @Override
    public Sequence<ScanResultValue> run(
        QueryPlus<ScanResultValue> queryPlus, Map<String, Object> responseContext
    )
    {
      Query<ScanResultValue> query = queryPlus.getQuery();
      if (!(query instanceof ScanQuery)) {
        throw new ISE("Got a [%s] which isn't a %s", query.getClass(), ScanQuery.class);
      }

      // it happens in unit tests
      final Number timeoutAt = (Number) responseContext.get(CTX_TIMEOUT_AT);
      if (timeoutAt == null || timeoutAt.longValue() == 0L) {
        responseContext.put(CTX_TIMEOUT_AT, JodaUtils.MAX_INSTANT);
      }
      return engine.process((ScanQuery) query, segment, responseContext);
    }
  }
}
