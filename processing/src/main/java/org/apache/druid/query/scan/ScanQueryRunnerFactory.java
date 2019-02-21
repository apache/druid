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

package org.apache.druid.query.scan;

import com.google.common.base.Function;
import com.google.inject.Inject;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.JodaUtils;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerFactory;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.segment.Segment;

import java.util.Iterator;
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
  private final ScanQueryConfig scanQueryConfig;

  @Inject
  public ScanQueryRunnerFactory(
      ScanQueryQueryToolChest toolChest,
      ScanQueryEngine engine,
      ScanQueryConfig scanQueryConfig
  )
  {
    this.toolChest = toolChest;
    this.engine = engine;
    this.scanQueryConfig = scanQueryConfig;
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
    return (queryPlus, responseContext) -> {
      ScanQuery query = (ScanQuery) queryPlus.getQuery();
      int numSegments = 0;
      final Iterator<QueryRunner<ScanResultValue>> segmentIt = queryRunners.iterator();
      for (; segmentIt.hasNext(); numSegments++) {
        segmentIt.next();
      }
      // Note: this variable is effective only when queryContext has a timeout.
      // See the comment of CTX_TIMEOUT_AT.
      final long timeoutAt = System.currentTimeMillis() + QueryContexts.getTimeout(queryPlus.getQuery());
      responseContext.put(CTX_TIMEOUT_AT, timeoutAt);
      if (query.getTimeOrder().equals(ScanQuery.TimeOrder.NONE)) {
        // Use existing strategy
      } else if (query.getLimit() <= scanQueryConfig.getMaxRowsQueuedForTimeOrdering()) {
        // Use priority queue strategy
      } else if (numSegments <= scanQueryConfig.getMaxSegmentsTimeOrderedInMemory()) {
        // Use flatMerge strategy
      } else {
        throw new UOE(
            "Time ordering for result set limit of %,d is not supported.  Try lowering the "
            + "result set size to less than or equal to the time ordering limit of %,d.",
            query.getLimit(),
            scanQueryConfig.getMaxRowsQueuedForTimeOrdering()
        );
      }

      return Sequences.concat(
          Sequences.map(
              Sequences.simple(queryRunners),
              input -> input.run(queryPlus, responseContext)
          )
      );

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
    public Sequence<ScanResultValue> run(QueryPlus<ScanResultValue> queryPlus, Map<String, Object> responseContext)
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
