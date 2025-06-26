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

package org.apache.druid.test.utils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.java.util.common.MapUtils;
import org.apache.druid.segment.ReferenceCountedSegmentProvider;
import org.apache.druid.segment.SegmentLazyLoadFailCallback;
import org.apache.druid.segment.TestSegmentUtils;
import org.apache.druid.segment.loading.NoopSegmentCacheManager;
import org.apache.druid.segment.loading.TombstoneSegmentizerFactory;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.Interval;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A local cache manager to test the bootstrapping and segment add/remove operations. It stubs only the necessary
 * methods to support these operations; any other method invoked will throw an exception from the base class,
 * {@link NoopSegmentCacheManager}.
 */
public class TestSegmentCacheManager extends NoopSegmentCacheManager
{
  private final List<DataSegment> cachedSegments;

  private final List<DataSegment> observedBootstrapSegments;
  private final List<DataSegment> observedSegments;
  private final List<DataSegment> observedSegmentsRemovedFromCache;
  private final AtomicInteger observedShutdownBootstrapCount;

  public TestSegmentCacheManager()
  {
    this(ImmutableSet.of());
  }

  public TestSegmentCacheManager(final Set<DataSegment> segmentsToCache)
  {
    this.cachedSegments = ImmutableList.copyOf(segmentsToCache);

    // While inneficient, these CopyOnWriteArrayList objects greatly simplify meeting the thread
    // safety mandate from SegmentCacheManager. For testing, this should be ok.
    this.observedBootstrapSegments = new CopyOnWriteArrayList<>();
    this.observedSegments = new CopyOnWriteArrayList<>();
    this.observedSegmentsRemovedFromCache = new CopyOnWriteArrayList<>();

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
  public ReferenceCountedSegmentProvider getBootstrapSegment(DataSegment segment, SegmentLazyLoadFailCallback loadFailed)
  {
    observedBootstrapSegments.add(segment);
    return getSegmentInternal(segment);
  }

  @Override
  public ReferenceCountedSegmentProvider getSegment(final DataSegment segment)
  {
    observedSegments.add(segment);
    return getSegmentInternal(segment);
  }

  private ReferenceCountedSegmentProvider getSegmentInternal(final DataSegment segment)
  {
    if (segment.isTombstone()) {
      return ReferenceCountedSegmentProvider
          .wrapSegment(TombstoneSegmentizerFactory.segmentForTombstone(segment), segment.getShardSpec());
    } else {
      return ReferenceCountedSegmentProvider.wrapSegment(
          new TestSegmentUtils.SegmentForTesting(
              segment.getDataSource(),
              (Interval) segment.getLoadSpec().get("interval"),
              MapUtils.getString(segment.getLoadSpec(), "version")
          ), segment.getShardSpec()
      );
    }
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

  public List<DataSegment> getObservedSegments()
  {
    return observedSegments;
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
