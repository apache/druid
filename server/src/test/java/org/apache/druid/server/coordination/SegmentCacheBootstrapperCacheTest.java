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
import com.google.common.collect.ImmutableList;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.metrics.StubServiceEmitter;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.TestIndex;
import org.apache.druid.segment.TestSegmentUtils;
import org.apache.druid.segment.loading.LeastBytesUsedStorageLocationSelectorStrategy;
import org.apache.druid.segment.loading.SegmentLoaderConfig;
import org.apache.druid.segment.loading.SegmentLoadingException;
import org.apache.druid.segment.loading.SegmentLocalCacheManager;
import org.apache.druid.segment.loading.StorageLoadingThreadPool;
import org.apache.druid.segment.loading.StorageLocation;
import org.apache.druid.segment.loading.StorageLocationConfig;
import org.apache.druid.server.SegmentManager;
import org.apache.druid.server.metrics.DefaultLoadSpecHolder;
import org.apache.druid.timeline.DataSegment;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Similar to {@link SegmentCacheBootstrapperTest}. This class includes tests that cover the
 * storage location layer as well.
 */
public class SegmentCacheBootstrapperCacheTest
{
  private static final long MAX_SIZE = 1000L;
  private static final long SEGMENT_SIZE = 100L;
  @TempDir
  public File temporaryFolder;

  private File infoDir;
  private File cacheDir;
  private TestDataSegmentAnnouncer segmentAnnouncer;
  private SegmentManager segmentManager;
  private SegmentLoaderConfig loaderConfig;
  private SegmentLocalCacheManager cacheManager;
  private TestCoordinatorClient coordinatorClient;
  private ServiceEmitter emitter;
  private ObjectMapper objectMapper;

  @BeforeEach
  public void setup() throws IOException
  {
    infoDir = newFolder(temporaryFolder, "junit");
    cacheDir = newFolder(temporaryFolder, "junit");
    loaderConfig = SegmentLoaderConfig.builder()
        .infoDir(infoDir)
        .locations(new StorageLocationConfig(cacheDir, MAX_SIZE, null))
        .build();

    objectMapper = TestHelper.makeJsonMapper();
    objectMapper.registerSubtypes(TestSegmentUtils.TestLoadSpec.class);
    objectMapper.registerSubtypes(TestSegmentUtils.TestSegmentizerFactory.class);

    final List<StorageLocation> storageLocations = loaderConfig.toStorageLocations();
    cacheManager = new SegmentLocalCacheManager(
        storageLocations,
        loaderConfig,
        StorageLoadingThreadPool.createFromConfig(loaderConfig),
        new LeastBytesUsedStorageLocationSelectorStrategy(storageLocations),
        TestIndex.INDEX_IO,
        objectMapper
    );
    segmentManager = new SegmentManager(cacheManager);
    segmentAnnouncer = new TestDataSegmentAnnouncer();
    coordinatorClient = new TestCoordinatorClient();
    emitter = new StubServiceEmitter();
    EmittingLogger.registerEmitter(emitter);
  }

  @Test
  public void testLoadStartStopWithEmptyLocations() throws IOException
  {
    final List<StorageLocation> emptyLocations = ImmutableList.of();
    final SegmentLoaderConfig loaderConfig = SegmentLoaderConfig.builder().build();
    segmentManager = new SegmentManager(
        new SegmentLocalCacheManager(
            emptyLocations,
            loaderConfig,
            StorageLoadingThreadPool.createFromConfig(loaderConfig),
            new LeastBytesUsedStorageLocationSelectorStrategy(emptyLocations),
            TestIndex.INDEX_IO,
            objectMapper
        )
    );

    final SegmentLoadDropHandler loadDropHandler = new SegmentLoadDropHandler(
        loaderConfig,
        segmentAnnouncer,
        segmentManager
    );

    final SegmentCacheBootstrapper bootstrapper = new SegmentCacheBootstrapper(
        loadDropHandler,
        loaderConfig,
        segmentAnnouncer,
        segmentManager,
        coordinatorClient,
        emitter,
        new DefaultLoadSpecHolder()
    );

    bootstrapper.start();

    bootstrapper.stop();
  }

  @Test
  public void testLoadStartStop() throws IOException
  {
    final SegmentLoadDropHandler loadDropHandler = new SegmentLoadDropHandler(
        loaderConfig,
        segmentAnnouncer,
        segmentManager
    );

    final SegmentCacheBootstrapper bootstrapper = new SegmentCacheBootstrapper(
        loadDropHandler,
        loaderConfig,
        segmentAnnouncer,
        segmentManager,
        coordinatorClient,
        emitter,
        new DefaultLoadSpecHolder()
    );

    bootstrapper.start();

    bootstrapper.stop();
  }

  @Test
  public void testLoadLocalCache() throws IOException, SegmentLoadingException
  {
    // write some segments to file bypassing loadDropHandler
    int numSegments = (int) (MAX_SIZE / SEGMENT_SIZE);
    List<DataSegment> expectedSegments = new ArrayList<>();
    for (int i = 0; i < numSegments; i++) {
      String version = "segment-" + i;
      DataSegment segment = TestSegmentUtils.makeSegment("test", version, SEGMENT_SIZE);
      cacheManager.storeInfoFile(segment);
      File segmentDir = new File(cacheDir, segment.getId().toString());
      new TestSegmentUtils.TestLoadSpec((int) SEGMENT_SIZE, version).loadSegment(segmentDir);
      expectedSegments.add(segment);
    }

    final SegmentLoadDropHandler loadDropHandler = new SegmentLoadDropHandler(
        loaderConfig,
        segmentAnnouncer,
        segmentManager
    );

    final SegmentCacheBootstrapper bootstrapper = new SegmentCacheBootstrapper(
        loadDropHandler,
        loaderConfig,
        segmentAnnouncer,
        segmentManager,
        coordinatorClient,
        emitter,
        new DefaultLoadSpecHolder()
    );

    bootstrapper.start();

    // Verify the expected announcements
    Assertions.assertTrue(segmentAnnouncer.getObservedSegments().containsAll(expectedSegments));

    // Make sure adding segments beyond allowed size fails
    DataSegment newSegment = TestSegmentUtils.makeSegment("test", "new-segment", SEGMENT_SIZE);
    loadDropHandler.addSegment(newSegment, null, null);
    Assertions.assertFalse(segmentAnnouncer.getObservedSegments().contains(newSegment));

    // Clearing some segment should allow for new segments
    loadDropHandler.removeSegment(expectedSegments.get(0), null, false);
    loadDropHandler.addSegment(newSegment, null, null);
    Assertions.assertTrue(segmentAnnouncer.getObservedSegments().contains(newSegment));

    bootstrapper.stop();
  }

  private static File newFolder(File root, String... subDirs) throws IOException
  {
    if (subDirs.length == 0 || (subDirs.length == 1 && "junit".equals(subDirs[0]))) {
      return java.nio.file.Files.createTempDirectory(root.toPath(), "junit").toFile();
    }
    String subFolder = String.join("/", subDirs);
    File result = new File(root, subFolder);
    if (!result.mkdirs()) {
      throw new IOException("Couldn't create folders " + root);
    }
    return result;
  }
}
