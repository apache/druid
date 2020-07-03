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

package org.apache.druid.sql.calcite.util;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Ordering;
import com.google.common.io.Closeables;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.InlineDataSource;
import org.apache.druid.query.LookupDataSource;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.query.QuerySegmentWalker;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.query.lookup.LookupExtractorFactoryContainer;
import org.apache.druid.query.lookup.LookupExtractorFactoryContainerProvider;
import org.apache.druid.segment.InlineSegmentWrangler;
import org.apache.druid.segment.LookupSegmentWrangler;
import org.apache.druid.segment.MapSegmentWrangler;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.QueryableIndexSegment;
import org.apache.druid.segment.ReferenceCountingSegment;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.SegmentWrangler;
import org.apache.druid.segment.join.JoinableFactory;
import org.apache.druid.server.ClientQuerySegmentWalker;
import org.apache.druid.server.QueryScheduler;
import org.apache.druid.server.QueryStackTests;
import org.apache.druid.server.initialization.ServerConfig;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.VersionedIntervalTimeline;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * A self-contained class that executes queries similarly to the normal Druid query stack.
 *
 * {@link ClientQuerySegmentWalker}, the same class that Brokers use as the entry point for their query stack, is
 * used directly. It, and the sub-walkers it needs, are created by {@link QueryStackTests}.
 */
public class SpecificSegmentsQuerySegmentWalker implements QuerySegmentWalker, Closeable
{
  private final QuerySegmentWalker walker;
  private final Map<String, VersionedIntervalTimeline<String, ReferenceCountingSegment>> timelines = new HashMap<>();
  private final List<Closeable> closeables = new ArrayList<>();
  private final List<DataSegment> segments = new ArrayList<>();

  /**
   * Create an instance using the provided query runner factory conglomerate and lookup provider.
   * If a JoinableFactory is provided, it will be used instead of the default. If a scheduler is included,
   * the runner will schedule queries according to the scheduling config.
   */
  public SpecificSegmentsQuerySegmentWalker(
      final QueryRunnerFactoryConglomerate conglomerate,
      final LookupExtractorFactoryContainerProvider lookupProvider,
      @Nullable final JoinableFactory joinableFactory,
      final QueryScheduler scheduler
  )
  {
    final JoinableFactory joinableFactoryToUse;

    if (joinableFactory == null) {
      joinableFactoryToUse = QueryStackTests.makeJoinableFactoryForLookup(lookupProvider);
    } else {
      joinableFactoryToUse = joinableFactory;
    }

    this.walker = QueryStackTests.createClientQuerySegmentWalker(
        QueryStackTests.createClusterQuerySegmentWalker(
            timelines,
            joinableFactoryToUse,
            conglomerate,
            scheduler
        ),
        QueryStackTests.createLocalQuerySegmentWalker(
            conglomerate,
            new MapSegmentWrangler(
                ImmutableMap.<Class<? extends DataSource>, SegmentWrangler>builder()
                    .put(InlineDataSource.class, new InlineSegmentWrangler())
                    .put(LookupDataSource.class, new LookupSegmentWrangler(lookupProvider))
                    .build()
            ),
            joinableFactoryToUse,
            scheduler
        ),
        conglomerate,
        joinableFactoryToUse,
        new ServerConfig()
    );
  }

  /**
   * Create an instance without any lookups and with a default {@link JoinableFactory} that handles only inline
   * datasources.
   */
  public SpecificSegmentsQuerySegmentWalker(final QueryRunnerFactoryConglomerate conglomerate)
  {
    this(
        conglomerate,
        new LookupExtractorFactoryContainerProvider()
        {
          @Override
          public Set<String> getAllLookupNames()
          {
            return Collections.emptySet();
          }

          @Override
          public Optional<LookupExtractorFactoryContainer> get(String lookupName)
          {
            return Optional.empty();
          }
        },
        null,
        QueryStackTests.DEFAULT_NOOP_SCHEDULER
    );
  }

  public SpecificSegmentsQuerySegmentWalker add(final DataSegment descriptor, final QueryableIndex index)
  {
    final Segment segment = new QueryableIndexSegment(index, descriptor.getId());
    final VersionedIntervalTimeline<String, ReferenceCountingSegment> timeline = timelines.computeIfAbsent(
        descriptor.getDataSource(),
        datasource -> new VersionedIntervalTimeline<>(Ordering.natural())
    );
    timeline.add(
        descriptor.getInterval(),
        descriptor.getVersion(),
        descriptor.getShardSpec().createChunk(ReferenceCountingSegment.wrapSegment(segment, descriptor.getShardSpec()))
    );
    segments.add(descriptor);
    closeables.add(index);
    return this;
  }

  public List<DataSegment> getSegments()
  {
    return segments;
  }

  @Override
  public <T> QueryRunner<T> getQueryRunnerForIntervals(final Query<T> query, final Iterable<Interval> intervals)
  {
    return walker.getQueryRunnerForIntervals(query, intervals);
  }

  @Override
  public <T> QueryRunner<T> getQueryRunnerForSegments(final Query<T> query, final Iterable<SegmentDescriptor> specs)
  {
    return walker.getQueryRunnerForSegments(query, specs);
  }

  @Override
  public void close() throws IOException
  {
    for (Closeable closeable : closeables) {
      Closeables.close(closeable, true);
    }
  }
}
