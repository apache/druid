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
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerFactory;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.ReportTimelineMissingSegmentQueryRunner;
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
 * This server manager is designed to test query retry on missing segments. A segment can be missing during a query
 * if a historical drops the segment after the broker issues the query to the historical. To mimic this situation,
 * the historical with this server manager announces all segments assigned, but reports missing segments for the
 * first 3 segments specified in the query.
 *
 * @see org.apache.druid.query.RetryQueryRunner
 */
public class ServerManagerForQueryRetryTest extends ServerManager
{
  // Query context key that indicates this query is for query retry testing.
  public static final String QUERY_RETRY_TEST_CONTEXT_KEY = "query-retry-test";

  private static final Logger LOG = new Logger(ServerManagerForQueryRetryTest.class);
  private static final int MAX_NUM_FALSE_MISSING_SEGMENTS_REPORTS = 3;

  private final ConcurrentHashMap<String, Set<SegmentDescriptor>> queryToIgnoredSegments = new ConcurrentHashMap<>();

  @Inject
  public ServerManagerForQueryRetryTest(
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
