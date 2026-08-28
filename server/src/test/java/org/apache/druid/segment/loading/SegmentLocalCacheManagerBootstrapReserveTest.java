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

package org.apache.druid.segment.loading;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.segment.SegmentLazyLoadFailCallback;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.column.ColumnConfig;
import org.apache.druid.testing.TemporaryFolderExtension;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.partition.NoneShardSpec;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Covers what a virtual-storage bootstrap reserves, and what happens when it cannot. Both the partial and the complete
 * layout are reserved by {@code bootstrap()} rather than by {@code getCachedSegments()}, so that every mount runs under
 * a hold: reclaim passes over held entries only, so an unheld entry can be chosen as an eviction victim by a parallel
 * bootstrap thread's reservation while this one is still mounting it.
 */
class SegmentLocalCacheManagerBootstrapReserveTest
{
  private static final SegmentId SEGMENT_ID = SegmentId.of("test", Intervals.of("2025/2026"), "v1", 0);
  private static final long SEGMENT_SIZE = 4096L;

  @RegisterExtension
  public final TemporaryFolderExtension temporaryFolder = TemporaryFolderExtension.testCaseScoped();

  private File cacheRoot;
  private SegmentLocalCacheManager manager;
  private DataSegment segment;

  @BeforeEach
  void setup() throws IOException
  {
    cacheRoot = temporaryFolder.newFolder("cache");
    segment = DataSegment.builder(SEGMENT_ID)
                         .shardSpec(NoneShardSpec.instance())
                         .loadSpec(Map.of("type", "local", "path", temporaryFolder.newFolder("deep").getAbsolutePath()))
                         .size(SEGMENT_SIZE)
                         .build();
  }

  @AfterEach
  void tearDown()
  {
    if (manager != null) {
      manager.shutdown();
    }
  }

  /**
   * Build a virtual-storage manager over a location of the given size, with the on-disk shape of an eagerly
   * downloaded segment: a directory named for the segment, plus its info file. The directory's contents don't matter
   * to either test, since neither gets as far as a successful mount.
   */
  private void setUpManagerWithLocationSize(long locationSize) throws IOException
  {
    final ObjectMapper jsonMapper = TestHelper.makeJsonMapper();
    final StorageLocationConfig locationConfig = new StorageLocationConfig(cacheRoot, locationSize, null);
    final SegmentLoaderConfig loaderConfig = SegmentLoaderConfig.builder()
                                                                .locations(locationConfig)
                                                                .virtualStorage(true)
                                                                .build();
    final List<StorageLocation> storageLocations = loaderConfig.toStorageLocations();
    manager = new SegmentLocalCacheManager(
        storageLocations,
        loaderConfig,
        StorageLoadingThreadPool.createFromConfig(loaderConfig),
        new LeastBytesUsedStorageLocationSelectorStrategy(storageLocations),
        TestHelper.getTestIndexIO(jsonMapper, ColumnConfig.DEFAULT),
        jsonMapper
    );
    FileUtils.mkdirp(new File(cacheRoot, SEGMENT_ID.toString()));
    manager.storeInfoFile(segment);
  }

  @Test
  void testCompleteLayoutIsReservedByBootstrapNotByGetCachedSegments() throws IOException
  {
    // Room to spare, so a reservation here would succeed if getCachedSegments still made one.
    setUpManagerWithLocationSize(SEGMENT_SIZE * 4);

    Assertions.assertEquals(List.of(segment), manager.getCachedSegments());
    Assertions.assertNull(
        manager.getLocations().get(0).getCacheEntry(new SegmentCacheEntryIdentifier(SEGMENT_ID)),
        "getCachedSegments must only recognize the layout; reserving is bootstrap's job"
    );
    Assertions.assertEquals(0, manager.getLocations().get(0).currentSizeBytes());
  }

  @Test
  void testBootstrapFailsSegmentWhenLocationCannotReserveIt() throws IOException
  {
    // Far too small for the segment, so the bootstrap reservation cannot be satisfied.
    setUpManagerWithLocationSize(SEGMENT_SIZE / 4);
    manager.getCachedSegments();

    // Previously the too-small reservation was an alert, after which the segment was mounted anyway with nothing
    // reserved for it, leaving the location under-counting the disk it was really using. Now the segment fails to
    // bootstrap, so it is not announced and the coordinator re-issues a load for it.
    Assertions.assertThrows(
        SegmentLoadingException.class,
        () -> manager.bootstrap(segment, SegmentLazyLoadFailCallback.NOOP)
    );
    Assertions.assertNull(
        manager.getLocations().get(0).getCacheEntry(new SegmentCacheEntryIdentifier(SEGMENT_ID)),
        "a segment that could not be reserved must not be left registered"
    );
    Assertions.assertEquals(0, manager.getLocations().get(0).currentSizeBytes());
  }
}
