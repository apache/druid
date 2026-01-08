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

package org.apache.druid.testing.tools;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.druid.client.cache.Cache;
import org.apache.druid.client.cache.CacheConfig;
import org.apache.druid.client.cache.CachePopulator;
import org.apache.druid.error.DruidException;
import org.apache.druid.guice.annotations.Smile;
import org.apache.druid.java.util.common.guava.Accumulator;
import org.apache.druid.java.util.common.guava.FunctionalIterable;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.common.guava.YieldingAccumulator;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.query.DataSegmentAndDescriptor;
import org.apache.druid.query.LeafSegmentsBundle;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryCapacityExceededException;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.QueryProcessingPool;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerFactory;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.query.QueryTimeoutException;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.QueryUnsupportedException;
import org.apache.druid.query.ResourceLimitExceededException;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.query.planning.ExecutionVertex;
import org.apache.druid.query.policy.NoopPolicyEnforcer;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.SegmentMapFunction;
import org.apache.druid.segment.SegmentReference;
import org.apache.druid.server.SegmentManager;
import org.apache.druid.server.ServerManager;
import org.apache.druid.server.initialization.ServerConfig;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.VersionedIntervalTimeline;
import org.apache.druid.timeline.partition.PartitionChunk;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This server manager is designed to test various query failures.
 * <ul>
 *  <li> Missing segments. A segment can be missing during a query if a historical drops the segment
 *   after the broker issues the query to the historical. To mimic this situation, the historical
 *   with this server manager announces all segments assigned, and reports missing segments based on the following:
 *   <ul>
 *     <li> If {@link #QUERY_RETRY_UNAVAILABLE_SEGMENT_IDX_KEY} and {@link #QUERY_RETRY_TEST_CONTEXT_KEY} are set,
 *           the segment at that index is reported as missing exactly once.</li>
 *     <li> If {@link #QUERY_RETRY_UNAVAILABLE_SEGMENT_IDX_KEY} is not set or is -1, it simulates missing segments
 *     starting from the beginning, up to {@link #MAX_NUM_FALSE_MISSING_SEGMENTS_REPORTS}.</li>
 *   </ul>
 *   The missing report is only generated once for the first time. Post that report, upon retry, all segments are served
 *   for the datasource. See ITQueryRetryTestOnMissingSegments. </li>
 * <li> Other query errors. This server manager returns a sequence that always throws an exception
 *   based on a given query context value. See ITQueryErrorTest. </li>
 * </ul>
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
  /**
   * Query context that indicates which segment should be marked as unavilable/missing.
   * This should be used in conjunction with {@link #QUERY_RETRY_TEST_CONTEXT_KEY}.
   * <p>
   * A value of {@code 0} means the first segment will be reported as missing, {@code 1} for the second, and so on.
   * If this key is not set (default = -1), the test will instead simulate missing up to
   * {@link #MAX_NUM_FALSE_MISSING_SEGMENTS_REPORTS} segments from the beginning.
   * </p>
   */
  public static final String QUERY_RETRY_UNAVAILABLE_SEGMENT_IDX_KEY = "unavailable-segment-idx";
  private static final int MAX_NUM_FALSE_MISSING_SEGMENTS_REPORTS = 1;

  private static final Logger LOG = new Logger(ServerManagerForQueryErrorTest.class);

  private final ConcurrentHashMap<String, Integer> queryToIgnoredSegments = new ConcurrentHashMap<>();

  @Inject
  public ServerManagerForQueryErrorTest(
      QueryRunnerFactoryConglomerate conglomerate,
      ServiceEmitter emitter,
      QueryProcessingPool queryProcessingPool,
      CachePopulator cachePopulator,
      @Smile ObjectMapper objectMapper,
      Cache cache,
      CacheConfig cacheConfig,
      SegmentManager segmentManager,
      ServerConfig serverConfig
  )
  {
    super(
        conglomerate,
        emitter,
        queryProcessingPool,
        cachePopulator,
        objectMapper,
        cache,
        cacheConfig,
        segmentManager,
        serverConfig,
        NoopPolicyEnforcer.instance()
    );
  }

  @Override
  public <T> QueryRunner<T> getQueryRunnerForSegments(Query<T> query, Iterable<SegmentDescriptor> specs)
  {
    final ExecutionVertex ev = ExecutionVertex.of(query);
    final Optional<VersionedIntervalTimeline<String, DataSegment>> maybeTimeline =
        segmentManager.getTimeline(ev.getBaseTableDataSource());
    if (maybeTimeline.isEmpty()) {
      return (queryPlus, responseContext) -> {
        responseContext.addMissingSegments(Lists.newArrayList(specs));
        return Sequences.empty();
      };
    }

    final QueryRunnerFactory<T, Query<T>> factory = getQueryRunnerFactory(query);
    final QueryToolChest<T, Query<T>> toolChest = getQueryToolChest(query, factory);
    final VersionedIntervalTimeline<String, DataSegment> timeline = maybeTimeline.get();

    return new ResourceManagingQueryRunner<>(timeline, factory, toolChest, ev, specs)
    {
      @Override
      protected LeafSegmentsBundle getLeafSegmentsBundle(Query<T> query, SegmentMapFunction segmentMapFunction)
      {
        // override this method so that we can artifically create missing segments
        final List<DataSegmentAndDescriptor> segments = new ArrayList<>();
        final ArrayList<SegmentDescriptor> missingSegments = new ArrayList<>();
        final ArrayList<SegmentReference> segmentReferences = new ArrayList<>();
        final ArrayList<DataSegmentAndDescriptor> loadableSegments = new ArrayList<>();

        final QueryContext queryContext = query.context();

        for (SegmentDescriptor descriptor : specs) {
          final MutableBoolean isIgnoreSegment = new MutableBoolean(false);
          if (queryContext.getBoolean(QUERY_RETRY_TEST_CONTEXT_KEY, false)) {
            final int unavailableSegmentIdx = queryContext.getInt(QUERY_RETRY_UNAVAILABLE_SEGMENT_IDX_KEY, -1);
            queryToIgnoredSegments.compute(
                query.getMostSpecificId(),
                (queryId, ignoreCounter) -> {
                  if (ignoreCounter == null) {
                    ignoreCounter = 0;
                  }

                  if (unavailableSegmentIdx >= 0 && unavailableSegmentIdx == ignoreCounter) {
                    // Fail exactly once when counter matches the configured retry index
                    ignoreCounter++;
                    isIgnoreSegment.setTrue();
                  } else if (ignoreCounter < MAX_NUM_FALSE_MISSING_SEGMENTS_REPORTS) {
                    // Fail up to N times for this query
                    ignoreCounter++;
                    isIgnoreSegment.setTrue();
                  }
                  return ignoreCounter;
                }
            );

            if (isIgnoreSegment.isTrue()) {
              LOG.info(
                  "Pretending I don't have segment[%s]",
                  descriptor
              );
              missingSegments.add(descriptor);
              continue;
            }
          }

          final PartitionChunk<DataSegment> chunk = timeline.findChunk(
              descriptor.getInterval(),
              descriptor.getVersion(),
              descriptor.getPartitionNumber()
          );

          if (chunk != null) {
            segments.add(new DataSegmentAndDescriptor(chunk.getObject(), descriptor));
          } else {
            missingSegments.add(descriptor);
          }
        }

        // inlined logic of ServerManager.getSegmentsBundle
        for (DataSegmentAndDescriptor segment : segments) {
          final DataSegment dataSegment = segment.getDataSegment();
          if (dataSegment == null) {
            missingSegments.add(segment.getDescriptor());
            continue;
          }
          Optional<Segment> ref = segmentManager.acquireCachedSegment(dataSegment);
          if (ref.isPresent()) {
            segmentReferences.add(
                new SegmentReference(
                    segment.getDescriptor(),
                    segmentMapFunction.apply(ref),
                    null
                )
            );
          } else if (segmentManager.canLoadSegmentOnDemand(dataSegment)) {
            loadableSegments.add(segment);
          } else {
            missingSegments.add(segment.getDescriptor());
          }
        }
        return new LeafSegmentsBundle(segmentReferences, loadableSegments, missingSegments);
      }
    };
  }

  @Override
  protected <T> FunctionalIterable<QueryRunner<T>> getQueryRunnersForSegments(
      Query<T> query,
      QueryRunnerFactory<T, Query<T>> factory,
      QueryToolChest<T, Query<T>> toolChest,
      List<SegmentReference> segmentReferences,
      AtomicLong cpuTimeAccumulator,
      Optional<byte[]> cacheKeyPrefix
  )
  {
    return FunctionalIterable
        .create(segmentReferences)
        .transform(
            ref ->
                ref.getSegmentReference()
                   .map(segment -> {
                          final QueryContext queryContext = query.context();
                          if (queryContext.getBoolean(QUERY_TIMEOUT_TEST_CONTEXT_KEY, false)) {
                            return (QueryRunner<T>) (queryPlus, responseContext) -> new Sequence<>()
                            {
                              @Override
                              public <OutType> OutType accumulate(
                                  OutType initValue,
                                  Accumulator<OutType, T> accumulator
                              )
                              {
                                throw new QueryTimeoutException("query timeout test");
                              }

                              @Override
                              public <OutType> Yielder<OutType> toYielder(
                                  OutType initValue,
                                  YieldingAccumulator<OutType, T> accumulator
                              )
                              {
                                throw new QueryTimeoutException("query timeout test");
                              }
                            };
                          } else if (queryContext.getBoolean(QUERY_CAPACITY_EXCEEDED_TEST_CONTEXT_KEY, false)) {
                            return (QueryRunner<T>) (queryPlus, responseContext) -> new Sequence<>()
                            {
                              @Override
                              public <OutType> OutType accumulate(
                                  OutType initValue,
                                  Accumulator<OutType, T> accumulator
                              )
                              {
                                throw QueryCapacityExceededException.withErrorMessageAndResolvedHost(
                                    "query capacity exceeded test"
                                );
                              }

                              @Override
                              public <OutType> Yielder<OutType> toYielder(
                                  OutType initValue,
                                  YieldingAccumulator<OutType, T> accumulator
                              )
                              {
                                throw QueryCapacityExceededException.withErrorMessageAndResolvedHost(
                                    "query capacity exceeded test"
                                );
                              }
                            };
                          } else if (queryContext.getBoolean(QUERY_UNSUPPORTED_TEST_CONTEXT_KEY, false)) {
                            return (QueryRunner<T>) (queryPlus, responseContext) -> new Sequence<>()
                            {
                              @Override
                              public <OutType> OutType accumulate(
                                  OutType initValue,
                                  Accumulator<OutType, T> accumulator
                              )
                              {
                                throw new QueryUnsupportedException("query unsupported test");
                              }

                              @Override
                              public <OutType> Yielder<OutType> toYielder(
                                  OutType initValue,
                                  YieldingAccumulator<OutType, T> accumulator
                              )
                              {
                                throw new QueryUnsupportedException("query unsupported test");
                              }
                            };
                          } else if (queryContext.getBoolean(RESOURCE_LIMIT_EXCEEDED_TEST_CONTEXT_KEY, false)) {
                            return (QueryRunner<T>) (queryPlus, responseContext) -> new Sequence<>()
                            {
                              @Override
                              public <OutType> OutType accumulate(
                                  OutType initValue,
                                  Accumulator<OutType, T> accumulator
                              )
                              {
                                throw new ResourceLimitExceededException("resource limit exceeded test");
                              }

                              @Override
                              public <OutType> Yielder<OutType> toYielder(
                                  OutType initValue,
                                  YieldingAccumulator<OutType, T> accumulator
                              )
                              {
                                throw new ResourceLimitExceededException("resource limit exceeded test");
                              }
                            };
                          } else if (queryContext.getBoolean(QUERY_FAILURE_TEST_CONTEXT_KEY, false)) {
                            return (QueryRunner<T>) (queryPlus, responseContext) -> new Sequence<>()
                            {
                              @Override
                              public <OutType> OutType accumulate(
                                  OutType initValue,
                                  Accumulator<OutType, T> accumulator
                              )
                              {
                                throw new RuntimeException("query failure test");
                              }

                              @Override
                              public <OutType> Yielder<OutType> toYielder(
                                  OutType initValue,
                                  YieldingAccumulator<OutType, T> accumulator
                              )
                              {
                                throw new RuntimeException("query failure test");
                              }
                            };
                          }

                          return buildQueryRunnerForSegment(
                              ref,
                              factory,
                              toolChest,
                              cpuTimeAccumulator,
                              cacheKeyPrefix
                          );
                        }
                   ).orElseThrow(
                       () -> DruidException.defensive("Unexpected missing segment[%s]", ref.getSegmentDescriptor())
                   )
        );
  }
}
