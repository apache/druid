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

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.error.DruidException;
import org.apache.druid.error.DruidExceptionMatcher;
import org.apache.druid.guice.LocalDataStorageDruidModule;
import org.apache.druid.jackson.SegmentizerModule;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.segment.CursorFactory;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.SegmentLazyLoadFailCallback;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.TestIndex;
import org.apache.druid.segment.TestSegmentUtils;
import org.apache.druid.segment.column.ColumnConfig;
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.LinearShardSpec;
import org.apache.druid.timeline.partition.TombstoneShardSpec;
import org.apache.druid.utils.CompressionUtils;
import org.hamcrest.MatcherAssert;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

public class SegmentLocalCacheManagerTest extends InitializedNullHandlingTest
{
  private static final String TEST_DATA_BASE_RELATIVE_PATH =
      "test_segment_loader/2011-01-12T00:00:00.000Z_2011-04-15T00:00:00.001Z/2015-05-27T03:38:35.683Z/";
  private static final String TEST_DATA_RELATIVE_PATH = TEST_DATA_BASE_RELATIVE_PATH + "0";
  private static final String TEST_DATA_RELATIVE_PATH_2 = TEST_DATA_BASE_RELATIVE_PATH + "1";
  private static final String TEST_DATA_RELATIVE_PATH_3 = TEST_DATA_BASE_RELATIVE_PATH + "2";
  private static final String TEST_DATA_RELATIVE_PATH_4 = TEST_DATA_BASE_RELATIVE_PATH + "3";

  @Rule
  public final TemporaryFolder tmpFolder = new TemporaryFolder();

  private ObjectMapper jsonMapper;
  private File segmentDeepStorageDir;
  private File localSegmentCacheDir;
  private SegmentLocalCacheManager manager;

  @Before
  public void setUp() throws Exception
  {
    jsonMapper = TestHelper.makeJsonMapper();
    jsonMapper.registerSubtypes(
        new NamedType(LocalLoadSpec.class, "local"),
        new NamedType(TombstoneLoadSpec.class, "tombstone")
    );
    jsonMapper.registerSubtypes(TestSegmentUtils.TestLoadSpec.class);
    jsonMapper.registerSubtypes(TestSegmentUtils.TestSegmentizerFactory.class);
    jsonMapper.registerModule(new SegmentizerModule());
    jsonMapper.registerModules(new LocalDataStorageDruidModule().getJacksonModules());
    jsonMapper.setInjectableValues(
        new InjectableValues.Std()
            .addValue(LocalDataSegmentPuller.class, new LocalDataSegmentPuller())
            .addValue(IndexIO.class, TestHelper.getTestIndexIO(jsonMapper, ColumnConfig.DEFAULT))
            .addValue(ObjectMapper.class, jsonMapper)
            .addValue(DataSegment.PruneSpecsHolder.class, DataSegment.PruneSpecsHolder.DEFAULT)
            .addValue(ExprMacroTable.class, TestExprMacroTable.INSTANCE)
    );

    EmittingLogger.registerEmitter(new NoopServiceEmitter());
    segmentDeepStorageDir = tmpFolder.newFolder("segment_deep_storage");
    localSegmentCacheDir = tmpFolder.newFolder("segment_cache");

    manager = makeDefaultManager(jsonMapper);
    Assert.assertTrue(manager.canHandleSegments());
  }

  @Test
  public void testCanHandleSegmentsWithConfigLocations()
  {
    // Only injecting config locations without locations shouldn't really be the case.
    // It possibly suggests an issue with injection.
    final SegmentLoaderConfig loaderConfig = new SegmentLoaderConfig()
    {
      @Override
      public List<StorageLocationConfig> getLocations()
      {
        return Collections.singletonList(
            new StorageLocationConfig(localSegmentCacheDir, null, null)
        );
      }
    };

    manager = new SegmentLocalCacheManager(
        ImmutableList.of(),
        loaderConfig,
        new LeastBytesUsedStorageLocationSelectorStrategy(ImmutableList.of()),
        TestIndex.INDEX_IO,
        jsonMapper
    );
    Assert.assertTrue(manager.canHandleSegments());
  }

  @Test
  public void testCanHandleSegmentsWithLocations()
  {
    final ImmutableList<StorageLocation> locations = ImmutableList.of(
        new StorageLocation(localSegmentCacheDir, 10000000000L, null)
    );
    SegmentLocalCacheManager manager = new SegmentLocalCacheManager(
        locations,
        new SegmentLoaderConfig(),
        new LeastBytesUsedStorageLocationSelectorStrategy(locations),
        TestIndex.INDEX_IO,
        jsonMapper
    );
    Assert.assertTrue(manager.canHandleSegments());
  }

  @Test
  public void testCanHandleSegmentsWithEmptyLocationsAndConfigLocations()
  {
    SegmentLocalCacheManager manager = new SegmentLocalCacheManager(
        ImmutableList.of(),
        new SegmentLoaderConfig(),
        new LeastBytesUsedStorageLocationSelectorStrategy(ImmutableList.of()),
        TestIndex.INDEX_IO,
        jsonMapper
    );
    Assert.assertFalse(manager.canHandleSegments());
  }

  @Test
  public void testGetCachedSegmentsWhenCanHandleSegmentsIsFalse()
  {
    SegmentLocalCacheManager manager = new SegmentLocalCacheManager(
        null,
        new SegmentLoaderConfig(),
        new LeastBytesUsedStorageLocationSelectorStrategy(null),
        TestIndex.INDEX_IO,
        jsonMapper
    );
    MatcherAssert.assertThat(
        Assert.assertThrows(
            DruidException.class,
            () -> manager.getCachedSegments()
        ),
        DruidExceptionMatcher.defensive().expectMessageIs(
            "canHandleSegments() is false. getCachedSegments() must be invoked only"
            + " when canHandleSegments() returns true.")
    );
  }

  @Test
  public void testGetCachedSegments() throws IOException
  {
    SegmentLocalCacheManager manager = makeDefaultManager(jsonMapper);
    final File baseInfoDir = new File(manager.getLocations().get(0).getPath(), "/info_dir/");
    FileUtils.mkdirp(baseInfoDir);

    // can use fake segments since we aren't loading them for real
    final DataSegment segment1 = TestSegmentUtils.makeSegment(
        "test_segment_loader", "v0", Intervals.of("2014-10-20T00:00:00Z/P1D")
    );
    writeSegmentFile(segment1);
    manager.storeInfoFile(segment1);

    final DataSegment segment2 = TestSegmentUtils.makeSegment(
        "test_segment_loader", "v1", Intervals.of("2015-10-20T00:00:00Z/P1D")
    );
    writeSegmentFile(segment2);
    manager.storeInfoFile(segment2);

    Assert.assertTrue(manager.canHandleSegments());
    assertThat(manager.getCachedSegments(), containsInAnyOrder(segment1, segment2));
  }

  @Test
  public void testGetCachedSegmentsWithMissingSegmentFile() throws IOException
  {
    SegmentLocalCacheManager manager = makeDefaultManager(jsonMapper);
    final File baseInfoDir = new File(manager.getLocations().get(0).getPath(), "/info_dir/");
    FileUtils.mkdirp(baseInfoDir);

    // can use fake segments since we aren't loading them for real
    final DataSegment segment1 = TestSegmentUtils.makeSegment(
        "test_segment_loader", "v0", Intervals.of("2014-10-20T00:00:00Z/P1D")
    );
    writeSegmentFile(segment1);
    manager.storeInfoFile(segment1);

    final DataSegment segment2 = TestSegmentUtils.makeSegment(
        "test_segment_loader", "v1", Intervals.of("2015-10-20T00:00:00Z/P1D")
    );
    writeSegmentFile(segment2);
    manager.storeInfoFile(segment2);

    // Write another segment's info segment3InfoFile, but not the segment segment3InfoFile.
    final DataSegment segment3 = TestSegmentUtils.makeSegment(
        "test_segment_loader", "v1", Intervals.of("2016-10-20T00:00:00Z/P1D")
    );
    manager.storeInfoFile(segment3);
    final File segment3InfoFile = new File(baseInfoDir, segment3.getId().toString());
    Assert.assertTrue(segment3InfoFile.exists());

    Assert.assertTrue(manager.canHandleSegments());
    assertThat(manager.getCachedSegments(), containsInAnyOrder(segment1, segment2));
    Assert.assertFalse(segment3InfoFile.exists());
  }

  @Test
  public void testGetCachedSegmentsLegacyPathsMigrated() throws Exception
  {
    final DataSegment segmentToBootstrap = makeTestDataSegment(segmentDeepStorageDir);
    FileUtils.mkdirp(new File(localSegmentCacheDir, "info_dir"));
    manager.storeInfoFile(segmentToBootstrap);
    File segmentZip = createSegmentZipInLocation(
        segmentDeepStorageDir,
        TEST_DATA_RELATIVE_PATH
    );
    File unzippedSegmentPathInLocation = new File(
        localSegmentCacheDir,
        DataSegmentPusher.getDefaultStorageDir(segmentToBootstrap, false)
    );
    FileUtils.mkdirp(unzippedSegmentPathInLocation);
    CompressionUtils.unzip(
        segmentZip,
        unzippedSegmentPathInLocation
    );

    for (DataSegment segment : manager.getCachedSegments()) {
      manager.bootstrap(segment, SegmentLazyLoadFailCallback.NOOP);
    }

    // if bootstrapping a file that already exists it will be mounted by bootsrap
    Assert.assertNotNull(manager.getSegmentFiles(segmentToBootstrap));

    Assert.assertFalse(unzippedSegmentPathInLocation.exists());
    Assert.assertFalse(new File(localSegmentCacheDir, segmentToBootstrap.getDataSource()).exists());
    Assert.assertTrue(new File(localSegmentCacheDir, segmentToBootstrap.getId().toString()).exists());
  }

  @Test
  public void testGetCachedSegmentsLegacyPathsMigratedResilience() throws Exception
  {
    final DataSegment segmentToBootstrap = makeTestDataSegment(segmentDeepStorageDir);
    FileUtils.mkdirp(new File(localSegmentCacheDir, "info_dir"));
    manager.storeInfoFile(segmentToBootstrap);
    File segmentZip = createSegmentZipInLocation(
        segmentDeepStorageDir,
        TEST_DATA_RELATIVE_PATH
    );
    File unzippedSegmentPathInLocation = new File(
        localSegmentCacheDir,
        DataSegmentPusher.getDefaultStorageDir(segmentToBootstrap, false)
    );
    FileUtils.mkdirp(unzippedSegmentPathInLocation);
    CompressionUtils.unzip(
        segmentZip,
        unzippedSegmentPathInLocation
    );

    File unzippedSegmentInDestination = new File(
        localSegmentCacheDir,
        segmentToBootstrap.getId().toString()
    );
    FileUtils.mkdirp(unzippedSegmentInDestination);
    CompressionUtils.unzip(
        segmentZip,
        unzippedSegmentInDestination
    );

    for (DataSegment segment : manager.getCachedSegments()) {
      manager.bootstrap(segment, SegmentLazyLoadFailCallback.NOOP);
    }

    // if bootstrapping a file that already exists it will be mounted by bootsrap
    Assert.assertNotNull(manager.getSegmentFiles(segmentToBootstrap));

    Assert.assertFalse(unzippedSegmentPathInLocation.exists());
    Assert.assertFalse(new File(localSegmentCacheDir, segmentToBootstrap.getDataSource()).exists());
    Assert.assertTrue(new File(localSegmentCacheDir, segmentToBootstrap.getId().toString()).exists());
  }

  @Test
  public void testIfSegmentIsLoaded() throws IOException
  {
    // can use fake segments since we aren't loading them for real and just using utility method
    final DataSegment cachedSegment = dataSegmentWithInterval("2014-10-20T00:00:00Z/P1D");

    final File cachedSegmentFile = new File(
        localSegmentCacheDir,
        cachedSegment.getId().toString()
    );
    FileUtils.mkdirp(cachedSegmentFile);

    Assert.assertTrue("Expect cache hit", manager.isSegmentCached(cachedSegment));

    final DataSegment uncachedSegment = dataSegmentWithInterval("2014-10-21T00:00:00Z/P1D");
    Assert.assertFalse("Expect cache miss", manager.isSegmentCached(uncachedSegment));
  }

  @Test
  public void testLoadSegmentInPageCache() throws IOException, SegmentLoadingException
  {
    final SegmentLoaderConfig loaderConfig = new SegmentLoaderConfig()
    {
      @Override
      public int getNumThreadsToLoadSegmentsIntoPageCacheOnDownload()
      {
        return 1;
      }

      @Override
      public List<StorageLocationConfig> getLocations()
      {
        return Collections.singletonList(
            new StorageLocationConfig(localSegmentCacheDir, null, null)
        );
      }
    };
    final List<StorageLocation> locations = loaderConfig.toStorageLocations();
    SegmentLocalCacheManager manager = new SegmentLocalCacheManager(
        locations,
        loaderConfig,
        new LeastBytesUsedStorageLocationSelectorStrategy(locations),
        TestHelper.getTestIndexIO(jsonMapper, ColumnConfig.DEFAULT),
        jsonMapper
    );

    final DataSegment segmentToDownload = makeTestDataSegment(localSegmentCacheDir);
    final File localSegmentFile = new File(segmentDeepStorageDir, TEST_DATA_RELATIVE_PATH);
    makeSegmentZip(
        localSegmentFile,
        new File(segmentDeepStorageDir.getCanonicalPath() + "/" + TEST_DATA_RELATIVE_PATH + "/index.zip")
    );
    manager.getSegmentFiles(segmentToDownload);
  }

  @Test
  public void testGetAndCleanSegmentFiles() throws Exception
  {
    final DataSegment segmentToDownload = makeTestDataSegment(segmentDeepStorageDir);
    final File localSegmentFile = new File(segmentDeepStorageDir, TEST_DATA_RELATIVE_PATH);
    makeSegmentZip(
        localSegmentFile,
        new File(segmentDeepStorageDir.getCanonicalPath() + "/" + TEST_DATA_RELATIVE_PATH + "/index.zip")
    );

    Assert.assertFalse("Expect cache miss before downloading segment", manager.isSegmentCached(segmentToDownload));

    manager.load(segmentToDownload);
    manager.getSegmentFiles(segmentToDownload);
    Assert.assertTrue("Expect cache hit after downloading segment", manager.isSegmentCached(segmentToDownload));

    manager.drop(segmentToDownload);
    Assert.assertFalse("Expect cache miss after dropping segment", manager.isSegmentCached(segmentToDownload));
  }

  @Test
  public void testRetrySuccessAtFirstLocation() throws Exception
  {
    final File localStorageFolder = tmpFolder.newFolder("local_storage_folder");

    final List<StorageLocationConfig> locations = new ArrayList<>();
    final StorageLocationConfig locationConfig = new StorageLocationConfig(localStorageFolder, 10000000000L, null);
    locations.add(locationConfig);
    final File localStorageFolder2 = tmpFolder.newFolder("local_storage_folder2");
    final StorageLocationConfig locationConfig2 = new StorageLocationConfig(localStorageFolder2, 1000000000L, null);
    locations.add(locationConfig2);

    final SegmentLoaderConfig loaderConfig = new SegmentLoaderConfig().withLocations(locations);
    final List<StorageLocation> storageLocations = loaderConfig.toStorageLocations();
    SegmentLocalCacheManager manager = new SegmentLocalCacheManager(
        storageLocations,
        loaderConfig,
        new LeastBytesUsedStorageLocationSelectorStrategy(storageLocations),
        TestHelper.getTestIndexIO(jsonMapper, ColumnConfig.DEFAULT),
        jsonMapper
    );

    final DataSegment segmentToDownload = makeTestDataSegment(segmentDeepStorageDir);
    final File localSegmentFile = new File(segmentDeepStorageDir, TEST_DATA_RELATIVE_PATH);
    makeSegmentZip(localSegmentFile, new File(localSegmentFile, "index.zip"));

    Assert.assertFalse("Expect cache miss before downloading segment", manager.isSegmentCached(segmentToDownload));

    manager.load(segmentToDownload);
    File segmentFile = manager.getSegmentFiles(segmentToDownload);
    Assert.assertTrue(segmentFile.getAbsolutePath().contains("/local_storage_folder/"));
    Assert.assertTrue("Expect cache hit after downloading segment", manager.isSegmentCached(segmentToDownload));

    manager.drop(segmentToDownload);
    Assert.assertFalse("Expect cache miss after dropping segment", manager.isSegmentCached(segmentToDownload));
  }

  @Test
  public void testRetrySuccessAtSecondLocation() throws Exception
  {
    final List<StorageLocationConfig> locations = new ArrayList<>();
    final File localStorageFolder = tmpFolder.newFolder("local_storage_folder");
    // mock can't write in first location
    localStorageFolder.setWritable(false);
    final StorageLocationConfig locationConfig = new StorageLocationConfig(localStorageFolder, 1000000000L, null);
    locations.add(locationConfig);
    final File localStorageFolder2 = tmpFolder.newFolder("local_storage_folder2");
    final StorageLocationConfig locationConfig2 = new StorageLocationConfig(localStorageFolder2, 10000000L, null);
    locations.add(locationConfig2);

    final SegmentLoaderConfig loaderConfig = new SegmentLoaderConfig().withLocations(locations);
    final List<StorageLocation> storageLocations = loaderConfig.toStorageLocations();
    SegmentLocalCacheManager manager = new SegmentLocalCacheManager(
        storageLocations,
        new SegmentLoaderConfig().withLocations(locations),
        new LeastBytesUsedStorageLocationSelectorStrategy(storageLocations),
        TestHelper.getTestIndexIO(jsonMapper, ColumnConfig.DEFAULT),
        jsonMapper
    );
    final DataSegment segmentToDownload = makeTestDataSegment(segmentDeepStorageDir);
    final File localSegmentFile = new File(segmentDeepStorageDir, TEST_DATA_RELATIVE_PATH);

    makeSegmentZip(localSegmentFile, new File(localSegmentFile, "index.zip"));

    Assert.assertFalse("Expect cache miss before downloading segment", manager.isSegmentCached(segmentToDownload));

    manager.load(segmentToDownload);
    File segmentFile = manager.getSegmentFiles(segmentToDownload);
    Assert.assertTrue(segmentFile.getAbsolutePath().contains("/local_storage_folder2/"));
    Assert.assertTrue("Expect cache hit after downloading segment", manager.isSegmentCached(segmentToDownload));

    manager.drop(segmentToDownload);
    Assert.assertFalse("Expect cache miss after dropping segment", manager.isSegmentCached(segmentToDownload));
  }

  @Test
  public void testRetryAllFail() throws Exception
  {
    final List<StorageLocationConfig> locations = new ArrayList<>();
    final File localStorageFolder = tmpFolder.newFolder("local_storage_folder");
    // mock can't write in first location
    localStorageFolder.setWritable(false);
    final StorageLocationConfig locationConfig = new StorageLocationConfig(localStorageFolder, 1000000000L, null);
    locations.add(locationConfig);
    final File localStorageFolder2 = tmpFolder.newFolder("local_storage_folder2");
    // mock can't write in second location
    localStorageFolder2.setWritable(false);
    final StorageLocationConfig locationConfig2 = new StorageLocationConfig(localStorageFolder2, 10000000L, null);
    locations.add(locationConfig2);

    final SegmentLoaderConfig loaderConfig = new SegmentLoaderConfig().withLocations(locations);
    final List<StorageLocation> storageLocations = loaderConfig.toStorageLocations();
    SegmentLocalCacheManager manager = new SegmentLocalCacheManager(
        storageLocations,
        new SegmentLoaderConfig().withLocations(locations),
        new LeastBytesUsedStorageLocationSelectorStrategy(storageLocations),
        TestHelper.getTestIndexIO(jsonMapper, ColumnConfig.DEFAULT),
        jsonMapper
    );
    final DataSegment segmentToDownload = makeTestDataSegment(segmentDeepStorageDir);
    final File localSegmentFile = new File(segmentDeepStorageDir, TEST_DATA_RELATIVE_PATH);
    FileUtils.mkdirp(localSegmentFile);
    final File indexZip = new File(localSegmentFile, "index.zip");
    indexZip.createNewFile();

    try {
      // expect failure
      manager.load(segmentToDownload);
      Assert.fail();
    }
    catch (SegmentLoadingException e) {
    }
    Assert.assertFalse("Expect cache miss after dropping segment", manager.isSegmentCached(segmentToDownload));
    manager.drop(segmentToDownload);
  }

  @Test
  public void testEmptyToFullOrder() throws Exception
  {
    final List<StorageLocationConfig> locations = new ArrayList<>();
    final File localStorageFolder = tmpFolder.newFolder("local_storage_folder");
    localStorageFolder.setWritable(true);
    final StorageLocationConfig locationConfig = new StorageLocationConfig(localStorageFolder, 10L, null);
    locations.add(locationConfig);
    final File localStorageFolder2 = tmpFolder.newFolder("local_storage_folder2");
    localStorageFolder2.setWritable(true);
    final StorageLocationConfig locationConfig2 = new StorageLocationConfig(localStorageFolder2, 10L, null);
    locations.add(locationConfig2);

    final SegmentLoaderConfig loaderConfig = new SegmentLoaderConfig().withLocations(locations);
    final List<StorageLocation> storageLocations = loaderConfig.toStorageLocations();
    SegmentLocalCacheManager manager = new SegmentLocalCacheManager(
        storageLocations,
        new SegmentLoaderConfig().withLocations(locations),
        new LeastBytesUsedStorageLocationSelectorStrategy(storageLocations),
        TestHelper.getTestIndexIO(jsonMapper, ColumnConfig.DEFAULT),
        jsonMapper
    );

    final DataSegment segmentToDownload = makeTestDataSegment(segmentDeepStorageDir);
    final File localSegmentFile = new File(segmentDeepStorageDir, TEST_DATA_RELATIVE_PATH);
    makeSegmentZip(localSegmentFile, new File(segmentDeepStorageDir + "/" + TEST_DATA_RELATIVE_PATH + "/index.zip"));

    Assert.assertFalse("Expect cache miss before downloading segment", manager.isSegmentCached(segmentToDownload));

    manager.load(segmentToDownload);
    File segmentFile = manager.getSegmentFiles(segmentToDownload);
    Assert.assertTrue(segmentFile.getAbsolutePath().contains("/local_storage_folder/"));
    Assert.assertTrue("Expect cache hit after downloading segment", manager.isSegmentCached(segmentToDownload));

    final DataSegment segmentToDownload2 = makeTestDataSegment(segmentDeepStorageDir, 1, TEST_DATA_RELATIVE_PATH_2);
    final File localSegmentFile2 = new File(segmentDeepStorageDir, TEST_DATA_RELATIVE_PATH_2);
    makeSegmentZip(localSegmentFile2, new File(segmentDeepStorageDir + "/" + TEST_DATA_RELATIVE_PATH_2 + "/index.zip"));

    manager.load(segmentToDownload2);
    File segmentFile2 = manager.getSegmentFiles(segmentToDownload2);
    Assert.assertTrue(segmentFile2.getAbsolutePath().contains("/local_storage_folder2/"));
    Assert.assertTrue("Expect cache hit after downloading segment", manager.isSegmentCached(segmentToDownload2));

    manager.drop(segmentToDownload2);
    Assert.assertFalse("Expect cache miss after dropping segment", manager.isSegmentCached(segmentToDownload2));
  }

  @Test
  public void testSegmentDistributionUsingRoundRobinStrategy() throws Exception
  {
    final List<StorageLocationConfig> locationConfigs = new ArrayList<>();
    final StorageLocationConfig locationConfig = createStorageLocationConfig("local_storage_folder", 10000000000L, true);
    final StorageLocationConfig locationConfig2 = createStorageLocationConfig("local_storage_folder2", 1000000000L, true);
    final StorageLocationConfig locationConfig3 = createStorageLocationConfig("local_storage_folder3", 1000000000L, true);
    locationConfigs.add(locationConfig);
    locationConfigs.add(locationConfig2);
    locationConfigs.add(locationConfig3);

    List<StorageLocation> locations = new ArrayList<>();
    for (StorageLocationConfig locConfig : locationConfigs) {
      locations.add(
          new StorageLocation(
          locConfig.getPath(),
          locConfig.getMaxSize(),
          locConfig.getFreeSpacePercent()
        )
      );
    }

    SegmentLocalCacheManager manager = new SegmentLocalCacheManager(
        locations,
        new SegmentLoaderConfig().withLocations(locationConfigs),
        new RoundRobinStorageLocationSelectorStrategy(locations),
        TestHelper.getTestIndexIO(jsonMapper, ColumnConfig.DEFAULT),
        jsonMapper
    );

    // Segment 1 should be downloaded in local_storage_folder
    final DataSegment segmentToDownload1 = makeTestDataSegment(segmentDeepStorageDir);
    createSegmentZipInLocation(segmentDeepStorageDir, TEST_DATA_RELATIVE_PATH);

    Assert.assertFalse("Expect cache miss before downloading segment", manager.isSegmentCached(segmentToDownload1));

    manager.load(segmentToDownload1);
    File segmentFile = manager.getSegmentFiles(segmentToDownload1);
    Assert.assertTrue(segmentFile.getAbsolutePath().contains("/local_storage_folder/"));
    Assert.assertTrue("Expect cache hit after downloading segment", manager.isSegmentCached(segmentToDownload1));

    manager.drop(segmentToDownload1);
    Assert.assertFalse("Expect cache miss after dropping segment", manager.isSegmentCached(segmentToDownload1));

    // Segment 2 should be downloaded in local_storage_folder2
    final DataSegment segmentToDownload2 = makeTestDataSegment(segmentDeepStorageDir, 1, TEST_DATA_RELATIVE_PATH_2);
    createSegmentZipInLocation(segmentDeepStorageDir, TEST_DATA_RELATIVE_PATH_2);

    Assert.assertFalse("Expect cache miss before downloading segment", manager.isSegmentCached(segmentToDownload2));

    manager.load(segmentToDownload2);
    File segmentFile2 = manager.getSegmentFiles(segmentToDownload2);
    Assert.assertTrue(segmentFile2.getAbsolutePath().contains("/local_storage_folder2/"));
    Assert.assertTrue("Expect cache hit after downloading segment", manager.isSegmentCached(segmentToDownload2));

    manager.drop(segmentToDownload2);
    Assert.assertFalse("Expect cache miss after dropping segment", manager.isSegmentCached(segmentToDownload2));

    // Segment 3 should be downloaded in local_storage_folder3
    final DataSegment segmentToDownload3 = makeTestDataSegment(segmentDeepStorageDir, 2, TEST_DATA_RELATIVE_PATH_3);
    createSegmentZipInLocation(segmentDeepStorageDir, TEST_DATA_RELATIVE_PATH_3);

    manager.load(segmentToDownload3);
    File segmentFile3 = manager.getSegmentFiles(segmentToDownload3);
    Assert.assertTrue(segmentFile3.getAbsolutePath().contains("/local_storage_folder3/"));
    Assert.assertTrue("Expect cache hit after downloading segment", manager.isSegmentCached(segmentToDownload3));

    manager.drop(segmentToDownload3);
    Assert.assertFalse("Expect cache miss after dropping segment", manager.isSegmentCached(segmentToDownload3));

    // Segment 4 should be downloaded in local_storage_folder again, asserting round robin distribution of segments
    final DataSegment segmentToDownload4 = makeTestDataSegment(segmentDeepStorageDir, 3, TEST_DATA_RELATIVE_PATH_4);
    createSegmentZipInLocation(segmentDeepStorageDir, TEST_DATA_RELATIVE_PATH_4);

    Assert.assertFalse("Expect cache miss before downloading segment", manager.isSegmentCached(segmentToDownload4));

    manager.load(segmentToDownload4);
    File segmentFile1 = manager.getSegmentFiles(segmentToDownload4);
    Assert.assertTrue(segmentFile1.getAbsolutePath().contains("/local_storage_folder/"));
    Assert.assertTrue("Expect cache hit after downloading segment", manager.isSegmentCached(segmentToDownload4));
    manager.drop(segmentToDownload4);
    Assert.assertFalse("Expect cache miss after dropping segment", manager.isSegmentCached(segmentToDownload4));
  }

  @Test
  public void testSegmentDistributionUsingLeastBytesUsedStrategy() throws Exception
  {
    final List<StorageLocationConfig> locations = new ArrayList<>();
    final StorageLocationConfig locationConfig = createStorageLocationConfig("local_storage_folder", 10000000000L,
        true);
    final StorageLocationConfig locationConfig2 = createStorageLocationConfig("local_storage_folder2", 1000000000L,
        true);
    final StorageLocationConfig locationConfig3 = createStorageLocationConfig("local_storage_folder3", 1000000000L,
        true);
    locations.add(locationConfig);
    locations.add(locationConfig2);
    locations.add(locationConfig3);

    final SegmentLoaderConfig loaderConfig = new SegmentLoaderConfig().withLocations(locations);
    final List<StorageLocation> storageLocations = loaderConfig.toStorageLocations();
    SegmentLocalCacheManager manager = new SegmentLocalCacheManager(
        storageLocations,
        new SegmentLoaderConfig().withLocations(locations),
        new LeastBytesUsedStorageLocationSelectorStrategy(storageLocations),
        TestHelper.getTestIndexIO(jsonMapper, ColumnConfig.DEFAULT),
        jsonMapper
    );

    // Segment 1 should be downloaded in local_storage_folder, segment1 size 10L
    final DataSegment segmentToDownload = makeTestDataSegment(segmentDeepStorageDir);

    createSegmentZipInLocation(segmentDeepStorageDir, TEST_DATA_RELATIVE_PATH);

    Assert.assertFalse("Expect cache miss before downloading segment", manager.isSegmentCached(segmentToDownload));

    manager.load(segmentToDownload);
    File segmentFile = manager.getSegmentFiles(segmentToDownload);
    Assert.assertTrue(segmentFile.getAbsolutePath().contains("/local_storage_folder/"));
    Assert.assertTrue("Expect cache hit after downloading segment", manager.isSegmentCached(segmentToDownload));

    // Segment 2 should be downloaded in local_storage_folder2, segment2 size 5L
    final DataSegment segmentToDownload2 = makeTestDataSegment(segmentDeepStorageDir, 5L, 1, TEST_DATA_RELATIVE_PATH_2);
    createSegmentZipInLocation(segmentDeepStorageDir, TEST_DATA_RELATIVE_PATH_2);

    Assert.assertFalse("Expect cache miss before downloading segment", manager.isSegmentCached(segmentToDownload2));

    manager.load(segmentToDownload2);
    File segmentFile2 = manager.getSegmentFiles(segmentToDownload2);
    Assert.assertTrue(segmentFile2.getAbsolutePath().contains("/local_storage_folder2/"));
    Assert.assertTrue("Expect cache hit after downloading segment", manager.isSegmentCached(segmentToDownload2));


    // Segment 3 should be downloaded in local_storage_folder3, segment3 size 20L
    final DataSegment segmentToDownload3 = makeTestDataSegment(segmentDeepStorageDir, 20L, 2, TEST_DATA_RELATIVE_PATH_3);
    createSegmentZipInLocation(segmentDeepStorageDir, TEST_DATA_RELATIVE_PATH_3);

    manager.load(segmentToDownload3);
    File segmentFile3 = manager.getSegmentFiles(segmentToDownload3);
    Assert.assertTrue(segmentFile3.getAbsolutePath().contains("/local_storage_folder3/"));
    Assert.assertTrue("Expect cache hit after downloading segment", manager.isSegmentCached(segmentToDownload3));

    // Now the storage locations local_storage_folder1, local_storage_folder2 and local_storage_folder3 have 10, 5 and
    // 20 bytes occupied respectively. The default strategy should pick location2 (as it has least bytes used) for the
    // next segment to be downloaded asserting the least bytes used distribution of segments.
    final DataSegment segmentToDownload4 = makeTestDataSegment(segmentDeepStorageDir, 10L, 3, TEST_DATA_RELATIVE_PATH_4);
    createSegmentZipInLocation(segmentDeepStorageDir, TEST_DATA_RELATIVE_PATH_4);

    Assert.assertFalse("Expect cache miss before downloading segment", manager.isSegmentCached(segmentToDownload4));

    manager.load(segmentToDownload4);
    File segmentFile1 = manager.getSegmentFiles(segmentToDownload4);
    Assert.assertTrue(segmentFile1.getAbsolutePath().contains("/local_storage_folder2/"));
    Assert.assertTrue("Expect cache hit after downloading segment", manager.isSegmentCached(segmentToDownload4));

  }

  @Test
  public void testSegmentDistributionUsingRandomStrategy() throws Exception
  {
    final List<StorageLocationConfig> locationConfigs = new ArrayList<>();
    final StorageLocationConfig locationConfig = createStorageLocationConfig(
        "local_storage_folder",
        10L,
        true
    );
    final StorageLocationConfig locationConfig2 = createStorageLocationConfig(
        "local_storage_folder2",
        100L,
        false
    );
    final StorageLocationConfig locationConfig3 = createStorageLocationConfig(
        "local_storage_folder3",
        9L,
        true
    );
    locationConfigs.add(locationConfig);
    locationConfigs.add(locationConfig2);
    locationConfigs.add(locationConfig3);

    SegmentLoaderConfig segmentLoaderConfig = new SegmentLoaderConfig().withLocations(locationConfigs);

    final List<StorageLocation> locations = segmentLoaderConfig.toStorageLocations();
    SegmentLocalCacheManager manager = new SegmentLocalCacheManager(
        locations,
        segmentLoaderConfig,
        new RandomStorageLocationSelectorStrategy(locations),
        TestHelper.getTestIndexIO(jsonMapper, ColumnConfig.DEFAULT),
        jsonMapper
    );

    // Segment 1 should be downloaded in local_storage_folder, segment1 size 10L
    final DataSegment segmentToDownload = makeTestDataSegment(segmentDeepStorageDir);

    createSegmentZipInLocation(segmentDeepStorageDir, TEST_DATA_RELATIVE_PATH);

    Assert.assertFalse("Expect cache miss before downloading segment", manager.isSegmentCached(segmentToDownload));

    manager.load(segmentToDownload);
    File segmentFile = manager.getSegmentFiles(segmentToDownload);
    Assert.assertTrue(segmentFile.getAbsolutePath().contains("/local_storage_folder/"));
    Assert.assertTrue("Expect cache hit after downloading segment", manager.isSegmentCached(segmentToDownload));

    // Segment 2 should be downloaded in local_storage_folder3, segment2 size 9L
    final DataSegment segmentToDownload2 = makeTestDataSegment(segmentDeepStorageDir, 9L, 1, TEST_DATA_RELATIVE_PATH_2);
    createSegmentZipInLocation(segmentDeepStorageDir, TEST_DATA_RELATIVE_PATH_2);

    Assert.assertFalse("Expect cache miss before downloading segment", manager.isSegmentCached(segmentToDownload2));

    manager.load(segmentToDownload2);
    File segmentFile2 = manager.getSegmentFiles(segmentToDownload2);
    Assert.assertTrue(segmentFile2.getAbsolutePath().contains("/local_storage_folder3/"));
    Assert.assertTrue("Expect cache hit after downloading segment", manager.isSegmentCached(segmentToDownload2));


    // Segment 3 should not be downloaded, segment3 size 20L
    final DataSegment segmentToDownload3 = makeTestDataSegment(segmentDeepStorageDir, 20L, 2, TEST_DATA_RELATIVE_PATH_3);
    createSegmentZipInLocation(segmentDeepStorageDir, TEST_DATA_RELATIVE_PATH_3);

    try {
      // expect failure
      manager.load(segmentToDownload3);
      Assert.fail();
    }
    catch (SegmentLoadingException e) {
    }
    Assert.assertFalse("Expect cache miss after dropping segment", manager.isSegmentCached(segmentToDownload3));
  }

  @Test
  public void testGetSegmentFilesWhenDownloadStartMarkerExists() throws Exception
  {
    final DataSegment segmentToDownload = makeTestDataSegment(segmentDeepStorageDir);

    final File localSegmentFile = new File(segmentDeepStorageDir, TEST_DATA_RELATIVE_PATH);
    makeSegmentZip(localSegmentFile, new File(localSegmentFile, "index.zip"));

    manager.load(segmentToDownload);
    final File cachedSegmentDir = manager.getSegmentFiles(segmentToDownload);
    Assert.assertTrue("Expect cache hit after downloading segment", manager.isSegmentCached(segmentToDownload));

    // Emulate a corrupted segment file
    final File downloadMarker = new File(
        new File(localSegmentCacheDir, segmentToDownload.getId().toString()),
        SegmentLocalCacheManager.DOWNLOAD_START_MARKER_FILE_NAME
    );
    Assert.assertTrue(downloadMarker.createNewFile());
    // create a new manager, expect corrupted segment to be cleaned out and freshly downloaded after startup
    SegmentLocalCacheManager manager = makeDefaultManager(jsonMapper);

    manager.load(segmentToDownload);
    manager.getSegmentFiles(segmentToDownload);
    // this is still true becuase
    Assert.assertTrue("Don't expect cache miss for corrupted segment file", manager.isSegmentCached(segmentToDownload));
    Assert.assertTrue(cachedSegmentDir.exists());
    Assert.assertFalse(downloadMarker.exists());
  }

  @Test
  public void testGetBootstrapSegment() throws SegmentLoadingException
  {
    final StorageLocationConfig locationConfig = new StorageLocationConfig(localSegmentCacheDir, 10000L, null);
    final SegmentLoaderConfig loaderConfig = new SegmentLoaderConfig().withLocations(ImmutableList.of(locationConfig));
    final List<StorageLocation> storageLocations = loaderConfig.toStorageLocations();
    SegmentLocalCacheManager manager = new SegmentLocalCacheManager(
        storageLocations,
        loaderConfig,
        new LeastBytesUsedStorageLocationSelectorStrategy(storageLocations),
        TestHelper.getTestIndexIO(jsonMapper, ColumnConfig.DEFAULT),
        jsonMapper
    );

    final DataSegment dataSegment = TestSegmentUtils.makeSegment("foo", "v1", Intervals.of("2020/2021"));

    manager.bootstrap(dataSegment, SegmentLazyLoadFailCallback.NOOP);
    Segment actualBootstrapSegment = manager.acquireCachedSegment(dataSegment).orElse(null);
    Assert.assertNotNull(actualBootstrapSegment);
    Assert.assertEquals(dataSegment.getId(), actualBootstrapSegment.getId());
    Assert.assertEquals(dataSegment.getInterval(), actualBootstrapSegment.getDataInterval());
  }


  @Test
  public void testGetBootstrapSegmentLazy() throws SegmentLoadingException
  {
    final StorageLocationConfig locationConfig = new StorageLocationConfig(localSegmentCacheDir, 10000L, null);
    final SegmentLoaderConfig loaderConfig = new SegmentLoaderConfig()
    {
      @Override
      public boolean isLazyLoadOnStart()
      {
        return true;
      }

      @Override
      public List<StorageLocationConfig> getLocations()
      {
        return List.of(locationConfig);
      }
    };
    final List<StorageLocation> storageLocations = loaderConfig.toStorageLocations();
    SegmentLocalCacheManager manager = new SegmentLocalCacheManager(
        storageLocations,
        loaderConfig,
        new LeastBytesUsedStorageLocationSelectorStrategy(storageLocations),
        TestHelper.getTestIndexIO(jsonMapper, ColumnConfig.DEFAULT),
        jsonMapper
    );

    final DataSegment dataSegment = TestSegmentUtils.makeSegment("foo", "v1", Intervals.of("2020/2021"));

    manager.bootstrap(dataSegment, () -> {});
    Segment actualBootstrapSegment = manager.acquireCachedSegment(dataSegment).orElse(null);
    Assert.assertNotNull(actualBootstrapSegment);
    Assert.assertEquals(dataSegment.getId(), actualBootstrapSegment.getId());
    Assert.assertEquals(dataSegment.getInterval(), actualBootstrapSegment.getDataInterval());
  }

  @Test
  public void testGetSegmentVirtualStorage() throws Exception
  {
    final StorageLocationConfig locationConfig = new StorageLocationConfig(localSegmentCacheDir, 10000L, null);
    final SegmentLoaderConfig loaderConfig = new SegmentLoaderConfig()
    {
      @Override
      public List<StorageLocationConfig> getLocations()
      {
        return ImmutableList.of(locationConfig);
      }

      @Override
      public boolean isVirtualStorage()
      {
        return true;
      }

      @Override
      public File getInfoDir()
      {
        try {
          return tmpFolder.newFolder();
        }
        catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    };
    final List<StorageLocation> storageLocations = loaderConfig.toStorageLocations();
    SegmentLocalCacheManager manager = new SegmentLocalCacheManager(
        storageLocations,
        loaderConfig,
        new LeastBytesUsedStorageLocationSelectorStrategy(storageLocations),
        TestHelper.getTestIndexIO(jsonMapper, ColumnConfig.DEFAULT),
        jsonMapper
    );

    final DataSegment segmentToLoad = makeTestDataSegment(segmentDeepStorageDir);
    createSegmentZipInLocation(segmentDeepStorageDir, TEST_DATA_RELATIVE_PATH);

    manager.load(segmentToLoad);
    Assert.assertNull(manager.getSegmentFiles(segmentToLoad));
    Assert.assertFalse(manager.acquireCachedSegment(segmentToLoad).isPresent());
    AcquireSegmentAction segmentAction = manager.acquireSegment(segmentToLoad);
    AcquireSegmentResult result = segmentAction.getSegmentFuture().get();
    Optional<Segment> theSegment = result.getReferenceProvider().acquireReference();
    Assert.assertTrue(theSegment.isPresent());
    Assert.assertNotNull(manager.getSegmentFiles(segmentToLoad));
    Assert.assertEquals(segmentToLoad.getId(), theSegment.get().getId());
    Assert.assertEquals(segmentToLoad.getInterval(), theSegment.get().getDataInterval());
    theSegment.get().close();
    segmentAction.close();

    manager.drop(segmentToLoad);
    // drop doesn't really drop, segments hang out until evicted
    Assert.assertNotNull(manager.getSegmentFiles(segmentToLoad));

    // can actually load them again because load doesn't really do anything
    AcquireSegmentAction segmentActionAfterDrop = manager.acquireSegment(segmentToLoad);
    AcquireSegmentResult resultAfterDrop = segmentActionAfterDrop.getSegmentFuture().get();
    Optional<Segment> theSegmentAfterDrop = resultAfterDrop.getReferenceProvider().acquireReference();
    Assert.assertTrue(theSegmentAfterDrop.isPresent());
    Assert.assertNotNull(manager.getSegmentFiles(segmentToLoad));
    Assert.assertEquals(segmentToLoad.getId(), theSegmentAfterDrop.get().getId());
    Assert.assertEquals(segmentToLoad.getInterval(), theSegmentAfterDrop.get().getDataInterval());

    theSegmentAfterDrop.get().close();
    segmentActionAfterDrop.close();
  }

  @Test
  public void testGetBootstrapSegmentVirtualStorage() throws Exception
  {
    final StorageLocationConfig locationConfig = new StorageLocationConfig(localSegmentCacheDir, 10000L, null);
    final SegmentLoaderConfig loaderConfig = new SegmentLoaderConfig()
    {
      @Override
      public List<StorageLocationConfig> getLocations()
      {
        return ImmutableList.of(locationConfig);
      }

      @Override
      public boolean isVirtualStorage()
      {
        return true;
      }

      @Override
      public File getInfoDir()
      {
        try {
          return tmpFolder.newFolder();
        }
        catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    };
    final List<StorageLocation> storageLocations = loaderConfig.toStorageLocations();
    SegmentLocalCacheManager manager = new SegmentLocalCacheManager(
        storageLocations,
        loaderConfig,
        new LeastBytesUsedStorageLocationSelectorStrategy(storageLocations),
        TestHelper.getTestIndexIO(jsonMapper, ColumnConfig.DEFAULT),
        jsonMapper
    );
    final DataSegment segmentToBootstrap = makeTestDataSegment(segmentDeepStorageDir);
    createSegmentZipInLocation(segmentDeepStorageDir, TEST_DATA_RELATIVE_PATH);

    manager.bootstrap(segmentToBootstrap, SegmentLazyLoadFailCallback.NOOP);
    Assert.assertNull(manager.getSegmentFiles(segmentToBootstrap));
    Assert.assertFalse(manager.acquireCachedSegment(segmentToBootstrap).isPresent());
    AcquireSegmentAction segmentAction = manager.acquireSegment(segmentToBootstrap);
    AcquireSegmentResult result = segmentAction.getSegmentFuture().get();
    Optional<Segment> theSegment = result.getReferenceProvider().acquireReference();
    Assert.assertTrue(theSegment.isPresent());
    Assert.assertNotNull(manager.getSegmentFiles(segmentToBootstrap));
    Assert.assertEquals(segmentToBootstrap.getId(), theSegment.get().getId());
    Assert.assertEquals(segmentToBootstrap.getInterval(), theSegment.get().getDataInterval());

    theSegment.get().close();
    segmentAction.close();

    manager.drop(segmentToBootstrap);
    // drop doesn't really drop, segments hang out until evicted
    Assert.assertNotNull(manager.getSegmentFiles(segmentToBootstrap));

    // can actually load them again because bootstrap doesn't really do anything unless the segment is already
    // present in the cache
    AcquireSegmentAction segmentActionAfterDrop = manager.acquireSegment(segmentToBootstrap);
    AcquireSegmentResult resultAfterDrop = segmentActionAfterDrop.getSegmentFuture().get();
    Optional<Segment> theSegmentAfterDrop = resultAfterDrop.getReferenceProvider().acquireReference();
    Assert.assertTrue(theSegmentAfterDrop.isPresent());
    Assert.assertNotNull(manager.getSegmentFiles(segmentToBootstrap));
    Assert.assertEquals(segmentToBootstrap.getId(), theSegmentAfterDrop.get().getId());
    Assert.assertEquals(segmentToBootstrap.getInterval(), theSegmentAfterDrop.get().getDataInterval());

    theSegmentAfterDrop.get().close();
    segmentActionAfterDrop.close();
  }

  @Test
  public void testGetBootstrapSegmentVirtualStorageSegmentAlreadyCached() throws Exception
  {
    final StorageLocationConfig locationConfig = new StorageLocationConfig(localSegmentCacheDir, 10000L, null);
    final SegmentLoaderConfig loaderConfig = new SegmentLoaderConfig()
    {
      @Override
      public List<StorageLocationConfig> getLocations()
      {
        return ImmutableList.of(locationConfig);
      }

      @Override
      public boolean isVirtualStorage()
      {
        return true;
      }
    };
    final List<StorageLocation> storageLocations = loaderConfig.toStorageLocations();
    SegmentLocalCacheManager manager = new SegmentLocalCacheManager(
        storageLocations,
        loaderConfig,
        new LeastBytesUsedStorageLocationSelectorStrategy(storageLocations),
        TestHelper.getTestIndexIO(jsonMapper, ColumnConfig.DEFAULT),
        jsonMapper
    );

    final DataSegment segmentToBootstrap = makeTestDataSegment(segmentDeepStorageDir);
    FileUtils.mkdirp(new File(localSegmentCacheDir, "info_dir"));
    manager.storeInfoFile(segmentToBootstrap);
    File segmentZip = createSegmentZipInLocation(
        segmentDeepStorageDir,
        TEST_DATA_RELATIVE_PATH
    );
    File unzippedSegmentPathInLocation = new File(
        localSegmentCacheDir,
        segmentToBootstrap.getId().toString()
    );
    FileUtils.mkdirp(unzippedSegmentPathInLocation);
    CompressionUtils.unzip(
        segmentZip,
        unzippedSegmentPathInLocation
    );

    for (DataSegment segment : manager.getCachedSegments()) {
      manager.bootstrap(segment, SegmentLazyLoadFailCallback.NOOP);
    }

    // if bootstrapping a file that already exists it will be mounted by bootsrap
    Assert.assertNotNull(manager.getSegmentFiles(segmentToBootstrap));
  }

  @Test
  public void testGetSegmentAfterDroppedWithNoVirtualStorageEnabled() throws Exception
  {
    SegmentLocalCacheManager manager = makeDefaultManager(jsonMapper);

    final DataSegment segmentToLoad = makeTestDataSegment(segmentDeepStorageDir);
    createSegmentZipInLocation(segmentDeepStorageDir, TEST_DATA_RELATIVE_PATH);

    manager.load(segmentToLoad);
    Assert.assertNotNull(manager.getSegmentFiles(segmentToLoad));
    manager.drop(segmentToLoad);
    Assert.assertNull(manager.getSegmentFiles(segmentToLoad));

    // ensure that if virtual storage is not enabled, we do not download the segment (callers might have a DataSegment
    // reference which was originally cached and then dropped before attempting to acquire a segment. if virtual storage
    // is not enabled, this should return a missing segment instead of downloading
    AcquireSegmentAction segmentAction = manager.acquireSegment(segmentToLoad);
    AcquireSegmentResult result = segmentAction.getSegmentFuture().get();
    Optional<Segment> theSegment = result.getReferenceProvider().acquireReference();
    Assert.assertFalse(theSegment.isPresent());
    segmentAction.close();

    Assert.assertNull(manager.getSegmentFiles(segmentToLoad));
  }

  @Test
  public void testGetSegmentVirtualStorageMountAfterDrop() throws Exception
  {
    final StorageLocationConfig locationConfig = new StorageLocationConfig(localSegmentCacheDir, 10L, null);
    final SegmentLoaderConfig loaderConfig = new SegmentLoaderConfig()
    {
      @Override
      public List<StorageLocationConfig> getLocations()
      {
        return ImmutableList.of(locationConfig);
      }

      @Override
      public boolean isVirtualStorage()
      {
        return true;
      }

      @Override
      public File getInfoDir()
      {
        try {
          return tmpFolder.newFolder();
        }
        catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    };
    final List<StorageLocation> storageLocations = loaderConfig.toStorageLocations();
    SegmentLocalCacheManager manager = new SegmentLocalCacheManager(
        storageLocations,
        loaderConfig,
        new LeastBytesUsedStorageLocationSelectorStrategy(storageLocations),
        TestHelper.getTestIndexIO(jsonMapper, ColumnConfig.DEFAULT),
        jsonMapper
    );

    final DataSegment segmentToLoad = makeTestDataSegment(segmentDeepStorageDir);
    createSegmentZipInLocation(segmentDeepStorageDir, TEST_DATA_RELATIVE_PATH);

    manager.load(segmentToLoad);
    Assert.assertNull(manager.getSegmentFiles(segmentToLoad));
    Assert.assertFalse(manager.acquireCachedSegment(segmentToLoad).isPresent());
    AcquireSegmentAction segmentAction = manager.acquireSegment(segmentToLoad);

    // now drop it before we actually load it, but dropping a weakly held reference does not remove the entry from the
    // cache, deferring it until eviction
    manager.drop(segmentToLoad);

    // however, we also have a hold, so it will not be evicted
    final DataSegment cannotLoad = makeTestDataSegment(segmentDeepStorageDir, 1, TEST_DATA_RELATIVE_PATH_2);
    Assert.assertThrows(DruidException.class, () -> manager.acquireSegment(cannotLoad));

    // and we can still mount and use the segment we are holding
    AcquireSegmentResult result = segmentAction.getSegmentFuture().get();
    Assert.assertNotNull(result);
    Optional<Segment> theSegment = result.getReferenceProvider().acquireReference();
    Assert.assertTrue(theSegment.isPresent());
    Assert.assertNotNull(manager.getSegmentFiles(segmentToLoad));
    Assert.assertEquals(segmentToLoad.getId(), theSegment.get().getId());
    Assert.assertEquals(segmentToLoad.getInterval(), theSegment.get().getDataInterval());
    theSegment.get().close();
    segmentAction.close();
    Assert.assertNotNull(manager.getSegmentFiles(segmentToLoad));

    // now that the hold has been released, we can load the other segment and evict the one that was held
    createSegmentZipInLocation(segmentDeepStorageDir, TEST_DATA_RELATIVE_PATH_2);
    manager.load(cannotLoad);
    AcquireSegmentAction segmentActionAfterDrop = manager.acquireSegment(cannotLoad);
    AcquireSegmentResult resultDrop = segmentActionAfterDrop.getSegmentFuture().get();
    Optional<Segment> theSegmentAfterDrop = resultDrop.getReferenceProvider().acquireReference();
    Assert.assertTrue(theSegmentAfterDrop.isPresent());
    Assert.assertNotNull(manager.getSegmentFiles(cannotLoad));
    Assert.assertEquals(cannotLoad.getId(), theSegmentAfterDrop.get().getId());
    Assert.assertEquals(cannotLoad.getInterval(), theSegmentAfterDrop.get().getDataInterval());
    Assert.assertNull(manager.getSegmentFiles(segmentToLoad));

    theSegmentAfterDrop.get().close();
    segmentActionAfterDrop.close();
  }

  @Test
  public void testIfTombstoneIsLoaded() throws IOException, SegmentLoadingException
  {
    final DataSegment tombstone = DataSegment.builder()
                                             .dataSource("foo")
                                             .interval(Intervals.of("2014-10-20T00:00:00Z/P1D"))
                                             .version("version")
                                             .loadSpec(Collections.singletonMap(
                                                 "type",
                                                 DataSegment.TOMBSTONE_LOADSPEC_TYPE
                                             ))
                                             .shardSpec(TombstoneShardSpec.INSTANCE)
                                             .size(1)
                                             .build();


    final File cachedSegmentFile = new File(
        localSegmentCacheDir,
        "test_segment_loader/2014-10-20T00:00:00.000Z_2014-10-21T00:00:00.000Z/2015-05-27T03:38:35.683Z/0"
    );
    FileUtils.mkdirp(cachedSegmentFile);

    manager.load(tombstone);
    manager.getSegmentFiles(tombstone);
    Assert.assertTrue("Expect cache hit after downloading segment", manager.isSegmentCached(tombstone));
  }

  @Test
  public void testGetTombstoneSegment() throws SegmentLoadingException
  {
    final Interval interval = Intervals.of("2014-01-01/2014-01-02");
    final DataSegment tombstone = DataSegment.builder()
                                             .dataSource("foo")
                                             .interval(interval)
                                             .version("v1")
                                             .loadSpec(ImmutableMap.of("type", "tombstone"))
                                             .shardSpec(TombstoneShardSpec.INSTANCE)
                                             .size(100)
                                             .build();

    manager.load(tombstone);
    Segment segment = manager.acquireCachedSegment(tombstone).orElse(null);

    Assert.assertEquals(tombstone.getId(), segment.getId());
    Assert.assertEquals(interval, segment.getDataInterval());

    final CursorFactory cursorFactory = segment.as(CursorFactory.class);
    Assert.assertNotNull(cursorFactory);
    Assert.assertTrue(segment.isTombstone());

    final QueryableIndex queryableIndex = segment.as(QueryableIndex.class);
    Assert.assertNotNull(queryableIndex);
    Assert.assertEquals(interval, queryableIndex.getDataInterval());
    Assert.assertThrows(UnsupportedOperationException.class, queryableIndex::getMetadata);
    Assert.assertThrows(UnsupportedOperationException.class, queryableIndex::getNumRows);
    Assert.assertThrows(UnsupportedOperationException.class, queryableIndex::getAvailableDimensions);
    Assert.assertThrows(UnsupportedOperationException.class, queryableIndex::getBitmapFactoryForDimensions);
    Assert.assertThrows(UnsupportedOperationException.class, queryableIndex::getDimensionHandlers);
    Assert.assertThrows(UnsupportedOperationException.class, () -> queryableIndex.getColumnHolder("foo"));
  }

  private SegmentLocalCacheManager makeDefaultManager(ObjectMapper jsonMapper)
  {
    final List<StorageLocationConfig> locationConfigs = new ArrayList<>();
    final StorageLocationConfig locationConfig = new StorageLocationConfig(localSegmentCacheDir, 10000000000L, null);
    locationConfigs.add(locationConfig);
    final SegmentLoaderConfig loaderConfig = new SegmentLoaderConfig().withLocations(locationConfigs);
    final List<StorageLocation> locations = loaderConfig.toStorageLocations();
    return new SegmentLocalCacheManager(
        locations,
        loaderConfig,
        new LeastBytesUsedStorageLocationSelectorStrategy(locations),
        TestHelper.getTestIndexIO(jsonMapper, ColumnConfig.DEFAULT),
        jsonMapper
    );
  }

  private File createSegmentZipInLocation(File src, String relativePath) throws Exception
  {
    final File tmpSegmentFile = tmpFolder.newFile();
    FileUtils.mkdirp(new File(src, relativePath));
    return makeSegmentZip(tmpSegmentFile, new File(src, relativePath + "/index.zip"));
  }

  private StorageLocationConfig createStorageLocationConfig(String localPath, long maxSize, boolean writable) throws Exception
  {

    final File localStorageFolder = tmpFolder.newFolder(localPath);
    localStorageFolder.setWritable(writable);
    final StorageLocationConfig locationConfig = new StorageLocationConfig(localStorageFolder, maxSize, 1.0);
    return locationConfig;
  }

  private DataSegment dataSegmentWithInterval(String intervalStr)
  {
    return dataSegmentWithInterval(intervalStr, 10L);
  }

  private DataSegment dataSegmentWithInterval(String intervalStr, long size)
  {
    return dataSegmentWithInterval(intervalStr, size, 0);
  }

  private DataSegment dataSegmentWithInterval(String intervalStr, long size, int partition)
  {
    return DataSegment.builder()
                      .dataSource("test_segment_loader")
                      .interval(Intervals.of(intervalStr))
                      .loadSpec(
                          ImmutableMap.of(
                              "type",
                              "local",
                              "path",
                              "somewhere"
                          )
                      )
                      .version("2015-05-27T03:38:35.683Z")
                      .dimensions(ImmutableList.of())
                      .metrics(ImmutableList.of())
                      .shardSpec(new LinearShardSpec(partition))
                      .binaryVersion(9)
                      .size(size)
                      .build();
  }



  private DataSegment makeTestDataSegment(File segmentSrcFolder) throws IOException
  {
    return makeTestDataSegment(segmentSrcFolder, 10L, 0, TEST_DATA_RELATIVE_PATH);
  }

  private DataSegment makeTestDataSegment(File segmentSrcFolder, int partition, String relativePath) throws IOException
  {
    return makeTestDataSegment(segmentSrcFolder, 10L, partition, relativePath);
  }

  private DataSegment makeTestDataSegment(File segmentSrcFolder, long size, int partition, String relativePath) throws IOException
  {
    return dataSegmentWithInterval("2011-01-12T00:00:00.000Z/2011-04-15T00:00:00.001Z", size, partition)
        .withLoadSpec(
            ImmutableMap.of(
                "type",
                "local",
                "path",
                segmentSrcFolder.getCanonicalPath() + "/" + relativePath + "/index.zip"
            )
        );
  }

  private void writeSegmentFile(final DataSegment segment) throws IOException
  {
    final File segmentFile = new File(
        localSegmentCacheDir,
        segment.getId().toString()
    );
    FileUtils.mkdirp(segmentFile);
  }

  static File makeSegmentZip(File segmentFiles, File zipOutFile) throws IOException
  {
    TestIndex.persist(TestIndex.getIncrementalTestIndex(), IndexSpec.getDefault(), segmentFiles);
    FileUtils.mkdirp(zipOutFile.getParentFile());
    CompressionUtils.zip(segmentFiles, zipOutFile);
    return zipOutFile;
  }
}
