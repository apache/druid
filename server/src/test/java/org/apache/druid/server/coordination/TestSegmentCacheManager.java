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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.java.util.common.MapUtils;
import org.apache.druid.segment.ReferenceCountingSegment;
import org.apache.druid.segment.SegmentLazyLoadFailCallback;
import org.apache.druid.segment.loading.NoopSegmentCacheManager;
import org.apache.druid.segment.loading.TombstoneSegmentizerFactory;
import org.apache.druid.server.TestSegmentUtils;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.Interval;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A local cache manager to test the bootstrapping and segment add/remove operations. It stubs only the necessary
 * methods to support these operations; any other method invoked will throw an exception from the base class,
 * {@link NoopSegmentCacheManager}.
 */
class TestSegmentCacheManager extends NoopSegmentCacheManager
{
  private final List<DataSegment> cachedSegments;

  private final List<DataSegment> observedBootstrapSegments;
  private final List<DataSegment> observedBootstrapSegmentsLoadedIntoPageCache;
  private final List<DataSegment> observedSegments;
  private final List<DataSegment> observedSegmentsLoadedIntoPageCache;
  private final List<DataSegment> observedSegmentsRemovedFromCache;
  private final AtomicInteger observedShutdownBootstrapCount;

  TestSegmentCacheManager()
  {
    this(ImmutableSet.of());
  }

  TestSegmentCacheManager(final Set<DataSegment> segmentsToCache)
  {
    this.cachedSegments = ImmutableList.copyOf(segmentsToCache);
    this.observedBootstrapSegments = new ArrayList<>();
    this.observedBootstrapSegmentsLoadedIntoPageCache = new ArrayList<>();
    this.observedSegments = new ArrayList<>();
    this.observedSegmentsLoadedIntoPageCache = new ArrayList<>();
    this.observedSegmentsRemovedFromCache = new ArrayList<>();
    this.observedShutdownBootstrapCount = new AtomicInteger(0);
  }

  @Override
  public boolean canHandleSegments()
  {
    return true;
  }

  @Override
  public List<DataSegment> getCachedSegments()
  {
    return cachedSegments;
  }

  @Override
  public ReferenceCountingSegment getBootstrapSegment(DataSegment segment, SegmentLazyLoadFailCallback loadFailed)
  {
    observedBootstrapSegments.add(segment);
    return getSegmentInternal(segment);
  }

  @Override
  public ReferenceCountingSegment getSegment(final DataSegment segment)
  {
    observedSegments.add(segment);
    return getSegmentInternal(segment);
  }

  private ReferenceCountingSegment getSegmentInternal(final DataSegment segment)
  {
    if (segment.isTombstone()) {
      return ReferenceCountingSegment
          .wrapSegment(TombstoneSegmentizerFactory.segmentForTombstone(segment), segment.getShardSpec());
    } else {
      return ReferenceCountingSegment.wrapSegment(
          new TestSegmentUtils.SegmentForTesting(
              segment.getDataSource(),
              (Interval) segment.getLoadSpec().get("interval"),
              MapUtils.getString(segment.getLoadSpec(), "version")
          ), segment.getShardSpec()
      );
    }
  }

  @Override
  public void loadSegmentIntoPageCache(DataSegment segment)
  {
    observedSegmentsLoadedIntoPageCache.add(segment);
  }

  @Override
  public void loadSegmentIntoPageCacheOnBootstrap(DataSegment segment)
  {
    observedBootstrapSegmentsLoadedIntoPageCache.add(segment);
  }

  @Override
  public void shutdownBootstrap()
  {
    observedShutdownBootstrapCount.incrementAndGet();
  }

  @Override
  public void storeInfoFile(DataSegment segment)
  {
  }

  @Override
  public void removeInfoFile(DataSegment segment)
  {
  }

  @Override
  public void cleanup(DataSegment segment)
  {
    observedSegmentsRemovedFromCache.add(segment);
  }

  public List<DataSegment> getObservedBootstrapSegments()
  {
    return observedBootstrapSegments;
  }

  public List<DataSegment> getObservedBootstrapSegmentsLoadedIntoPageCache()
  {
    return observedBootstrapSegmentsLoadedIntoPageCache;
  }

  public List<DataSegment> getObservedSegments()
  {
    return observedSegments;
  }

  public List<DataSegment> getObservedSegmentsLoadedIntoPageCache()
  {
    return observedSegmentsLoadedIntoPageCache;
  }

  public List<DataSegment> getObservedSegmentsRemovedFromCache()
  {
    return observedSegmentsRemovedFromCache;
  }

  public AtomicInteger getObservedShutdownBootstrapCount()
  {
    return observedShutdownBootstrapCount;
  }
}
