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

package org.apache.druid.server.coordination;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.druid.client.cache.Cache;
import org.apache.druid.client.cache.CacheConfig;
import org.apache.druid.client.cache.CachePopulator;
import org.apache.druid.guice.annotations.Processing;
import org.apache.druid.guice.annotations.Smile;
import org.apache.druid.java.util.common.guava.Accumulator;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.common.guava.YieldingAccumulator;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryCapacityExceededException;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerFactory;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.query.QueryTimeoutException;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.QueryUnsupportedException;
import org.apache.druid.query.ReportTimelineMissingSegmentQueryRunner;
import org.apache.druid.query.ResourceLimitExceededException;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.segment.ReferenceCountingSegment;
import org.apache.druid.segment.SegmentReference;
import org.apache.druid.segment.join.JoinableFactory;
import org.apache.druid.server.SegmentManager;
import org.apache.druid.server.initialization.ServerConfig;
import org.apache.druid.timeline.VersionedIntervalTimeline;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

/**
 * This server manager is designed to test various query failures.
 *
 * - Missing segments. A segment can be missing during a query if a historical drops the segment
 *   after the broker issues the query to the historical. To mimic this situation, the historical
 *   with this server manager announces all segments assigned, but reports missing segments for the
 *   first 3 segments specified in the query. See ITQueryRetryTestOnMissingSegments.
 * - Other query errors. This server manager returns a sequence that always throws an exception
 *   based on a given query context value. See ITQueryErrorTest.
 *
 * @see org.apache.druid.query.RetryQueryRunner for query retrying.
 * @see org.apache.druid.client.JsonParserIterator for handling query errors from historicals.
 */
public class ServerManagerForQueryErrorTest extends ServerManager
{
  // Query context key that indicates this query is for query retry testing.
  public static final String QUERY_RETRY_TEST_CONTEXT_KEY = "query-retry-test";
  public static final String QUERY_TIMEOUT_TEST_CONTEXT_KEY = "query-timeout-test";
  public static final String QUERY_CAPACITY_EXCEEDED_TEST_CONTEXT_KEY = "query-capacity-exceeded-test";
  public static final String QUERY_UNSUPPORTED_TEST_CONTEXT_KEY = "query-unsupported-test";
  public static final String RESOURCE_LIMIT_EXCEEDED_TEST_CONTEXT_KEY = "resource-limit-exceeded-test";
  public static final String QUERY_FAILURE_TEST_CONTEXT_KEY = "query-failure-test";

  private static final Logger LOG = new Logger(ServerManagerForQueryErrorTest.class);
  private static final int MAX_NUM_FALSE_MISSING_SEGMENTS_REPORTS = 3;

  private final ConcurrentHashMap<String, Set<SegmentDescriptor>> queryToIgnoredSegments = new ConcurrentHashMap<>();

  @Inject
  public ServerManagerForQueryErrorTest(
      QueryRunnerFactoryConglomerate conglomerate,
      ServiceEmitter emitter,
      @Processing ExecutorService exec,
      CachePopulator cachePopulator,
      @Smile ObjectMapper objectMapper,
      Cache cache,
      CacheConfig cacheConfig,
      SegmentManager segmentManager,
      JoinableFactory joinableFactory,
      ServerConfig serverConfig
  )
  {
    super(
        conglomerate,
        emitter,
        exec,
        cachePopulator,
        objectMapper,
        cache,
        cacheConfig,
        segmentManager,
        joinableFactory,
        serverConfig
    );
  }

  @Override
  <T> QueryRunner<T> buildQueryRunnerForSegment(
      Query<T> query,
      SegmentDescriptor descriptor,
      QueryRunnerFactory<T, Query<T>> factory,
      QueryToolChest<T, Query<T>> toolChest,
      VersionedIntervalTimeline<String, ReferenceCountingSegment> timeline,
      Function<SegmentReference, SegmentReference> segmentMapFn,
      AtomicLong cpuTimeAccumulator,
      Optional<byte[]> cacheKeyPrefix
  )
  {
    if (query.getContextBoolean(QUERY_RETRY_TEST_CONTEXT_KEY, false)) {
      final MutableBoolean isIgnoreSegment = new MutableBoolean(false);
      queryToIgnoredSegments.compute(
          query.getMostSpecificId(),
          (queryId, ignoredSegments) -> {
            if (ignoredSegments == null) {
              ignoredSegments = new HashSet<>();
            }
            if (ignoredSegments.size() < MAX_NUM_FALSE_MISSING_SEGMENTS_REPORTS) {
              ignoredSegments.add(descriptor);
              isIgnoreSegment.setTrue();
            }
            return ignoredSegments;
          }
      );

      if (isIgnoreSegment.isTrue()) {
        LOG.info("Pretending I don't have segment[%s]", descriptor);
        return new ReportTimelineMissingSegmentQueryRunner<>(descriptor);
      }
    } else if (query.getContextBoolean(QUERY_TIMEOUT_TEST_CONTEXT_KEY, false)) {
      return (queryPlus, responseContext) -> new Sequence<T>()
      {
        @Override
        public <OutType> OutType accumulate(OutType initValue, Accumulator<OutType, T> accumulator)
        {
          throw new QueryTimeoutException("query timeout test");
        }

        @Override
        public <OutType> Yielder<OutType> toYielder(OutType initValue, YieldingAccumulator<OutType, T> accumulator)
        {
          throw new QueryTimeoutException("query timeout test");
        }
      };
    } else if (query.getContextBoolean(QUERY_CAPACITY_EXCEEDED_TEST_CONTEXT_KEY, false)) {
      return (queryPlus, responseContext) -> new Sequence<T>()
      {
        @Override
        public <OutType> OutType accumulate(OutType initValue, Accumulator<OutType, T> accumulator)
        {
          throw QueryCapacityExceededException.withErrorMessageAndResolvedHost("query capacity exceeded test");
        }

        @Override
        public <OutType> Yielder<OutType> toYielder(OutType initValue, YieldingAccumulator<OutType, T> accumulator)
        {
          throw QueryCapacityExceededException.withErrorMessageAndResolvedHost("query capacity exceeded test");
        }
      };
    } else if (query.getContextBoolean(QUERY_UNSUPPORTED_TEST_CONTEXT_KEY, false)) {
      return (queryPlus, responseContext) -> new Sequence<T>()
      {
        @Override
        public <OutType> OutType accumulate(OutType initValue, Accumulator<OutType, T> accumulator)
        {
          throw new QueryUnsupportedException("query unsupported test");
        }

        @Override
        public <OutType> Yielder<OutType> toYielder(OutType initValue, YieldingAccumulator<OutType, T> accumulator)
        {
          throw new QueryUnsupportedException("query unsupported test");
        }
      };
    } else if (query.getContextBoolean(RESOURCE_LIMIT_EXCEEDED_TEST_CONTEXT_KEY, false)) {
      return (queryPlus, responseContext) -> new Sequence<T>()
      {
        @Override
        public <OutType> OutType accumulate(OutType initValue, Accumulator<OutType, T> accumulator)
        {
          throw new ResourceLimitExceededException("resource limit exceeded test");
        }

        @Override
        public <OutType> Yielder<OutType> toYielder(OutType initValue, YieldingAccumulator<OutType, T> accumulator)
        {
          throw new ResourceLimitExceededException("resource limit exceeded test");
        }
      };
    } else if (query.getContextBoolean(QUERY_FAILURE_TEST_CONTEXT_KEY, false)) {
      return (queryPlus, responseContext) -> new Sequence<T>()
      {
        @Override
        public <OutType> OutType accumulate(OutType initValue, Accumulator<OutType, T> accumulator)
        {
          throw new RuntimeException("query failure test");
        }

        @Override
        public <OutType> Yielder<OutType> toYielder(OutType initValue, YieldingAccumulator<OutType, T> accumulator)
        {
          throw new RuntimeException("query failure test");
        }
      };
    }

    return super.buildQueryRunnerForSegment(
        query,
        descriptor,
        factory,
        toolChest,
        timeline,
        segmentMapFn,
        cpuTimeAccumulator,
        cacheKeyPrefix
    );
  }
}
