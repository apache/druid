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
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.NoneShardSpec;
import org.apache.druid.timeline.partition.TombstoneShardSpec;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;

public class SegmentLocalCacheManagerTest
{
  @Rule
  public final TemporaryFolder tmpFolder = new TemporaryFolder();

  private final ObjectMapper jsonMapper;

  private File localSegmentCacheFolder;
  private SegmentLocalCacheManager manager;

  public SegmentLocalCacheManagerTest()
  {
    jsonMapper = new DefaultObjectMapper();
    jsonMapper.registerSubtypes(new NamedType(LocalLoadSpec.class, "local"),
                                new NamedType(TombstoneLoadSpec.class, "tombstone"));
    jsonMapper.setInjectableValues(
        new InjectableValues.Std().addValue(
            LocalDataSegmentPuller.class,
            new LocalDataSegmentPuller()
        )
    );
  }

  @Before
  public void setUp() throws Exception
  {
    EmittingLogger.registerEmitter(new NoopServiceEmitter());
    localSegmentCacheFolder = tmpFolder.newFolder("segment_cache_folder");

    final List<StorageLocationConfig> locations = new ArrayList<>();
    final StorageLocationConfig locationConfig = new StorageLocationConfig(localSegmentCacheFolder, 10000000000L, null);
    locations.add(locationConfig);

    manager = new SegmentLocalCacheManager(
        new SegmentLoaderConfig().withLocations(locations),
        jsonMapper
    );
  }

  @Test
  public void testIfSegmentIsLoaded() throws IOException
  {
    final DataSegment cachedSegment = dataSegmentWithInterval("2014-10-20T00:00:00Z/P1D");
    final File cachedSegmentFile = new File(
        localSegmentCacheFolder,
        "test_segment_loader/2014-10-20T00:00:00.000Z_2014-10-21T00:00:00.000Z/2015-05-27T03:38:35.683Z/0"
    );
    FileUtils.mkdirp(cachedSegmentFile);

    Assert.assertTrue("Expect cache hit", manager.isSegmentCached(cachedSegment));

    final DataSegment uncachedSegment = dataSegmentWithInterval("2014-10-21T00:00:00Z/P1D");
    Assert.assertFalse("Expect cache miss", manager.isSegmentCached(uncachedSegment));
  }

  @Test
  public void testNoLoadingOfSegmentInPageCache() throws IOException
  {
    final DataSegment segment = dataSegmentWithInterval("2014-10-20T00:00:00Z/P1D");
    final File segmentFile = new File(
        localSegmentCacheFolder,
        "test_segment_loader/2014-10-20T00:00:00.000Z_2014-10-21T00:00:00.000Z/2015-05-27T03:38:35.683Z/0"
    );
    FileUtils.mkdirp(segmentFile);
    // should not throw any exception
    manager.loadSegmentIntoPageCache(segment, null);
  }

  @Test
  public void testLoadSegmentInPageCache() throws IOException
  {
    final DataSegment segment = dataSegmentWithInterval("2014-10-20T00:00:00Z/P1D");
    final File segmentFile = new File(
        localSegmentCacheFolder,
        "test_segment_loader/2014-10-20T00:00:00.000Z_2014-10-21T00:00:00.000Z/2015-05-27T03:38:35.683Z/0"
    );
    FileUtils.mkdirp(segmentFile);
    // should not throw any exception
    manager.loadSegmentIntoPageCache(segment, Executors.newSingleThreadExecutor());
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
        localSegmentCacheFolder,
        "test_segment_loader/2014-10-20T00:00:00.000Z_2014-10-21T00:00:00.000Z/2015-05-27T03:38:35.683Z/0"
    );
    FileUtils.mkdirp(cachedSegmentFile);

    manager.getSegmentFiles(tombstone);
    Assert.assertTrue("Expect cache hit after downloading segment", manager.isSegmentCached(tombstone));
  }

  @Test
  public void testGetAndCleanSegmentFiles() throws Exception
  {
    final File localStorageFolder = tmpFolder.newFolder("local_storage_folder");

    final DataSegment segmentToDownload = dataSegmentWithInterval("2014-10-20T00:00:00Z/P1D").withLoadSpec(
        ImmutableMap.of(
            "type",
            "local",
            "path",
            localStorageFolder.getCanonicalPath()
            + "/test_segment_loader"
            + "/2014-10-20T00:00:00.000Z_2014-10-21T00:00:00.000Z/2015-05-27T03:38:35.683Z"
            + "/0/index.zip"
        )
    );

    // manually create a local segment under localStorageFolder
    final File localSegmentFile = new File(
        localStorageFolder,
        "test_segment_loader/2014-10-20T00:00:00.000Z_2014-10-21T00:00:00.000Z/2015-05-27T03:38:35.683Z/0"
    );
    FileUtils.mkdirp(localSegmentFile);
    final File indexZip = new File(localSegmentFile, "index.zip");
    indexZip.createNewFile();

    Assert.assertFalse("Expect cache miss before downloading segment", manager.isSegmentCached(segmentToDownload));

    manager.getSegmentFiles(segmentToDownload);
    Assert.assertTrue("Expect cache hit after downloading segment", manager.isSegmentCached(segmentToDownload));

    manager.cleanup(segmentToDownload);
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

    manager = new SegmentLocalCacheManager(
        new SegmentLoaderConfig().withLocations(locations),
        jsonMapper
    );
    final File segmentSrcFolder = tmpFolder.newFolder("segmentSrcFolder");
    final DataSegment segmentToDownload = dataSegmentWithInterval("2014-10-20T00:00:00Z/P1D").withLoadSpec(
        ImmutableMap.of(
            "type",
            "local",
            "path",
            segmentSrcFolder.getCanonicalPath()
            + "/test_segment_loader"
            + "/2014-10-20T00:00:00.000Z_2014-10-21T00:00:00.000Z/2015-05-27T03:38:35.683Z"
            + "/0/index.zip"
        )
    );
    // manually create a local segment under segmentSrcFolder
    final File localSegmentFile = new File(
        segmentSrcFolder,
        "test_segment_loader/2014-10-20T00:00:00.000Z_2014-10-21T00:00:00.000Z/2015-05-27T03:38:35.683Z/0"
    );
    FileUtils.mkdirp(localSegmentFile);
    final File indexZip = new File(localSegmentFile, "index.zip");
    indexZip.createNewFile();

    Assert.assertFalse("Expect cache miss before downloading segment", manager.isSegmentCached(segmentToDownload));

    File segmentFile = manager.getSegmentFiles(segmentToDownload);
    Assert.assertTrue(segmentFile.getAbsolutePath().contains("/local_storage_folder/"));
    Assert.assertTrue("Expect cache hit after downloading segment", manager.isSegmentCached(segmentToDownload));

    manager.cleanup(segmentToDownload);
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

    manager = new SegmentLocalCacheManager(
        new SegmentLoaderConfig().withLocations(locations),
        jsonMapper
    );
    final File segmentSrcFolder = tmpFolder.newFolder("segmentSrcFolder");
    final DataSegment segmentToDownload = dataSegmentWithInterval("2014-10-20T00:00:00Z/P1D").withLoadSpec(
        ImmutableMap.of(
            "type",
            "local",
            "path",
            segmentSrcFolder.getCanonicalPath()
            + "/test_segment_loader"
            + "/2014-10-20T00:00:00.000Z_2014-10-21T00:00:00.000Z/2015-05-27T03:38:35.683Z"
            + "/0/index.zip"
        )
    );
    // manually create a local segment under segmentSrcFolder
    final File localSegmentFile = new File(
        segmentSrcFolder,
        "test_segment_loader/2014-10-20T00:00:00.000Z_2014-10-21T00:00:00.000Z/2015-05-27T03:38:35.683Z/0"
    );
    FileUtils.mkdirp(localSegmentFile);
    final File indexZip = new File(localSegmentFile, "index.zip");
    indexZip.createNewFile();

    Assert.assertFalse("Expect cache miss before downloading segment", manager.isSegmentCached(segmentToDownload));

    File segmentFile = manager.getSegmentFiles(segmentToDownload);
    Assert.assertTrue(segmentFile.getAbsolutePath().contains("/local_storage_folder2/"));
    Assert.assertTrue("Expect cache hit after downloading segment", manager.isSegmentCached(segmentToDownload));

    manager.cleanup(segmentToDownload);
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

    manager = new SegmentLocalCacheManager(
        new SegmentLoaderConfig().withLocations(locations),
        jsonMapper
    );
    final File segmentSrcFolder = tmpFolder.newFolder("segmentSrcFolder");
    final DataSegment segmentToDownload = dataSegmentWithInterval("2014-10-20T00:00:00Z/P1D").withLoadSpec(
        ImmutableMap.of(
            "type",
            "local",
            "path",
            segmentSrcFolder.getCanonicalPath()
            + "/test_segment_loader"
            + "/2014-10-20T00:00:00.000Z_2014-10-21T00:00:00.000Z/2015-05-27T03:38:35.683Z"
            + "/0/index.zip"
        )
    );
    // manually create a local segment under segmentSrcFolder
    final File localSegmentFile = new File(
        segmentSrcFolder,
        "test_segment_loader/2014-10-20T00:00:00.000Z_2014-10-21T00:00:00.000Z/2015-05-27T03:38:35.683Z/0"
    );
    FileUtils.mkdirp(localSegmentFile);
    final File indexZip = new File(localSegmentFile, "index.zip");
    indexZip.createNewFile();

    try {
      // expect failure
      manager.getSegmentFiles(segmentToDownload);
      Assert.fail();
    }
    catch (SegmentLoadingException e) {
    }
    Assert.assertFalse("Expect cache miss after dropping segment", manager.isSegmentCached(segmentToDownload));
    manager.cleanup(segmentToDownload);
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

    manager = new SegmentLocalCacheManager(
        new SegmentLoaderConfig().withLocations(locations),
        jsonMapper
    );
    final File segmentSrcFolder = tmpFolder.newFolder("segmentSrcFolder");
    final DataSegment segmentToDownload = dataSegmentWithInterval("2014-10-20T00:00:00Z/P1D").withLoadSpec(
        ImmutableMap.of(
            "type",
            "local",
            "path",
            segmentSrcFolder.getCanonicalPath()
                + "/test_segment_loader"
                + "/2014-10-20T00:00:00.000Z_2014-10-21T00:00:00.000Z/2015-05-27T03:38:35.683Z"
                + "/0/index.zip"
        )
    );
    // manually create a local segment under segmentSrcFolder
    final File localSegmentFile = new File(
        segmentSrcFolder,
        "test_segment_loader/2014-10-20T00:00:00.000Z_2014-10-21T00:00:00.000Z/2015-05-27T03:38:35.683Z/0"
    );
    FileUtils.mkdirp(localSegmentFile);
    final File indexZip = new File(localSegmentFile, "index.zip");
    indexZip.createNewFile();

    Assert.assertFalse("Expect cache miss before downloading segment", manager.isSegmentCached(segmentToDownload));

    File segmentFile = manager.getSegmentFiles(segmentToDownload);
    Assert.assertTrue(segmentFile.getAbsolutePath().contains("/local_storage_folder/"));
    Assert.assertTrue("Expect cache hit after downloading segment", manager.isSegmentCached(segmentToDownload));

    final DataSegment segmentToDownload2 = dataSegmentWithInterval("2014-11-20T00:00:00Z/P1D").withLoadSpec(
        ImmutableMap.of(
            "type",
            "local",
            "path",
            segmentSrcFolder.getCanonicalPath()
                + "/test_segment_loader"
                + "/2014-11-20T00:00:00.000Z_2014-11-21T00:00:00.000Z/2015-05-27T03:38:35.683Z"
                + "/0/index.zip"
        )
    );
    // manually create a local segment under segmentSrcFolder
    final File localSegmentFile2 = new File(
        segmentSrcFolder,
        "test_segment_loader/2014-11-20T00:00:00.000Z_2014-11-21T00:00:00.000Z/2015-05-27T03:38:35.683Z/0"
    );
    FileUtils.mkdirp(localSegmentFile2);
    final File indexZip2 = new File(localSegmentFile2, "index.zip");
    indexZip2.createNewFile();

    File segmentFile2 = manager.getSegmentFiles(segmentToDownload2);
    Assert.assertTrue(segmentFile2.getAbsolutePath().contains("/local_storage_folder2/"));
    Assert.assertTrue("Expect cache hit after downloading segment", manager.isSegmentCached(segmentToDownload2));

    manager.cleanup(segmentToDownload2);
    Assert.assertFalse("Expect cache miss after dropping segment", manager.isSegmentCached(segmentToDownload2));
  }

  private DataSegment dataSegmentWithInterval(String intervalStr)
  {
    return dataSegmentWithInterval(intervalStr, 10L);
  }

  private DataSegment dataSegmentWithInterval(String intervalStr, long size)
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
                      .shardSpec(NoneShardSpec.instance())
                      .binaryVersion(9)
                      .size(size)
                      .build();
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

    manager = new SegmentLocalCacheManager(
      new SegmentLoaderConfig().withLocations(locationConfigs),
      new RoundRobinStorageLocationSelectorStrategy(locations),
      jsonMapper
    );
    final File segmentSrcFolder = tmpFolder.newFolder("segmentSrcFolder");

    // Segment 1 should be downloaded in local_storage_folder
    final DataSegment segmentToDownload1 = dataSegmentWithInterval("2014-10-20T00:00:00Z/P1D").withLoadSpec(
        ImmutableMap.of(
        "type",
        "local",
        "path",
        segmentSrcFolder.getCanonicalPath()
          + "/test_segment_loader"
          + "/2014-10-20T00:00:00.000Z_2014-10-21T00:00:00.000Z/2015-05-27T03:38:35.683Z"
          + "/0/index.zip"
      )
    );
    // manually create a local segment under segmentSrcFolder
    createLocalSegmentFile(segmentSrcFolder, "test_segment_loader/2014-10-20T00:00:00.000Z_2014-10-21T00:00:00.000Z/2015-05-27T03:38:35.683Z/0");

    Assert.assertFalse("Expect cache miss before downloading segment", manager.isSegmentCached(segmentToDownload1));

    File segmentFile = manager.getSegmentFiles(segmentToDownload1);
    Assert.assertTrue(segmentFile.getAbsolutePath().contains("/local_storage_folder/"));
    Assert.assertTrue("Expect cache hit after downloading segment", manager.isSegmentCached(segmentToDownload1));

    manager.cleanup(segmentToDownload1);
    Assert.assertFalse("Expect cache miss after dropping segment", manager.isSegmentCached(segmentToDownload1));

    // Segment 2 should be downloaded in local_storage_folder2
    final DataSegment segmentToDownload2 = dataSegmentWithInterval("2014-11-20T00:00:00Z/P1D").withLoadSpec(
        ImmutableMap.of(
        "type",
        "local",
        "path",
        segmentSrcFolder.getCanonicalPath()
          + "/test_segment_loader"
          + "/2014-11-20T00:00:00.000Z_2014-11-21T00:00:00.000Z/2015-05-27T03:38:35.683Z"
          + "/0/index.zip"
      )
    );
    // manually create a local segment under segmentSrcFolder
    createLocalSegmentFile(segmentSrcFolder, "test_segment_loader/2014-11-20T00:00:00.000Z_2014-11-21T00:00:00.000Z/2015-05-27T03:38:35.683Z/0");

    Assert.assertFalse("Expect cache miss before downloading segment", manager.isSegmentCached(segmentToDownload2));

    File segmentFile2 = manager.getSegmentFiles(segmentToDownload2);
    Assert.assertTrue(segmentFile2.getAbsolutePath().contains("/local_storage_folder2/"));
    Assert.assertTrue("Expect cache hit after downloading segment", manager.isSegmentCached(segmentToDownload2));

    manager.cleanup(segmentToDownload2);
    Assert.assertFalse("Expect cache miss after dropping segment", manager.isSegmentCached(segmentToDownload2));

    // Segment 3 should be downloaded in local_storage_folder3
    final DataSegment segmentToDownload3 = dataSegmentWithInterval("2014-12-20T00:00:00Z/P1D").withLoadSpec(
        ImmutableMap.of(
        "type",
        "local",
        "path",
        segmentSrcFolder.getCanonicalPath()
          + "/test_segment_loader"
          + "/2014-12-20T00:00:00.000Z_2014-12-21T00:00:00.000Z/2015-05-27T03:38:35.683Z"
          + "/0/index.zip"
      )
    );
    // manually create a local segment under segmentSrcFolder
    createLocalSegmentFile(segmentSrcFolder,
        "test_segment_loader/2014-12-20T00:00:00.000Z_2014-12-21T00:00:00.000Z/2015-05-27T03:38:35.683Z/0");

    File segmentFile3 = manager.getSegmentFiles(segmentToDownload3);
    Assert.assertTrue(segmentFile3.getAbsolutePath().contains("/local_storage_folder3/"));
    Assert.assertTrue("Expect cache hit after downloading segment", manager.isSegmentCached(segmentToDownload3));

    manager.cleanup(segmentToDownload3);
    Assert.assertFalse("Expect cache miss after dropping segment", manager.isSegmentCached(segmentToDownload3));

    // Segment 4 should be downloaded in local_storage_folder again, asserting round robin distribution of segments
    final DataSegment segmentToDownload4 = dataSegmentWithInterval("2014-08-20T00:00:00Z/P1D").withLoadSpec(
        ImmutableMap.of(
        "type",
        "local",
        "path",
        segmentSrcFolder.getCanonicalPath()
          + "/test_segment_loader"
          + "/2014-08-20T00:00:00.000Z_2014-08-21T00:00:00.000Z/2015-05-27T03:38:35.683Z"
          + "/0/index.zip"
      )
    );
    // manually create a local segment under segmentSrcFolder
    createLocalSegmentFile(segmentSrcFolder, "test_segment_loader/2014-08-20T00:00:00.000Z_2014-08-21T00:00:00" +
        ".000Z/2015-05-27T03:38:35.683Z/0");

    Assert.assertFalse("Expect cache miss before downloading segment", manager.isSegmentCached(segmentToDownload4));

    File segmentFile1 = manager.getSegmentFiles(segmentToDownload4);
    Assert.assertTrue(segmentFile1.getAbsolutePath().contains("/local_storage_folder/"));
    Assert.assertTrue("Expect cache hit after downloading segment", manager.isSegmentCached(segmentToDownload4));
    manager.cleanup(segmentToDownload4);
    Assert.assertFalse("Expect cache miss after dropping segment", manager.isSegmentCached(segmentToDownload4));
  }

  private void createLocalSegmentFile(File segmentSrcFolder, String localSegmentPath) throws Exception
  {
    // manually create a local segment under segmentSrcFolder
    final File localSegmentFile = new File(segmentSrcFolder, localSegmentPath);
    FileUtils.mkdirp(localSegmentFile);
    final File indexZip = new File(localSegmentFile, "index.zip");
    indexZip.createNewFile();
  }

  private StorageLocationConfig createStorageLocationConfig(String localPath, long maxSize, boolean writable) throws Exception
  {

    final File localStorageFolder = tmpFolder.newFolder(localPath);
    localStorageFolder.setWritable(writable);
    final StorageLocationConfig locationConfig = new StorageLocationConfig(localStorageFolder, maxSize, 1.0);
    return locationConfig;
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

    manager = new SegmentLocalCacheManager(
      new SegmentLoaderConfig().withLocations(locations),
      jsonMapper
    );
    final File segmentSrcFolder = tmpFolder.newFolder("segmentSrcFolder");

    // Segment 1 should be downloaded in local_storage_folder, segment1 size 10L
    final DataSegment segmentToDownload = dataSegmentWithInterval("2014-10-20T00:00:00Z/P1D", 10L).withLoadSpec(
        ImmutableMap.of(
        "type",
        "local",
        "path",
        segmentSrcFolder.getCanonicalPath()
          + "/test_segment_loader"
          + "/2014-10-20T00:00:00.000Z_2014-10-21T00:00:00.000Z/2015-05-27T03:38:35.683Z"
          + "/0/index.zip"
      )
    );
    // manually create a local segment under segmentSrcFolder
    createLocalSegmentFile(segmentSrcFolder,
        "test_segment_loader/2014-10-20T00:00:00.000Z_2014-10-21T00:00:00.000Z/2015-05-27T03:38:35.683Z/0");

    Assert.assertFalse("Expect cache miss before downloading segment", manager.isSegmentCached(segmentToDownload));

    File segmentFile = manager.getSegmentFiles(segmentToDownload);
    Assert.assertTrue(segmentFile.getAbsolutePath().contains("/local_storage_folder/"));
    Assert.assertTrue("Expect cache hit after downloading segment", manager.isSegmentCached(segmentToDownload));

    // Segment 2 should be downloaded in local_storage_folder2, segment2 size 5L
    final DataSegment segmentToDownload2 = dataSegmentWithInterval("2014-11-20T00:00:00Z/P1D", 5L).withLoadSpec(
        ImmutableMap.of(
        "type",
        "local",
        "path",
        segmentSrcFolder.getCanonicalPath()
          + "/test_segment_loader"
          + "/2014-11-20T00:00:00.000Z_2014-11-21T00:00:00.000Z/2015-05-27T03:38:35.683Z"
          + "/0/index.zip"
      )
    );
    // manually create a local segment under segmentSrcFolder
    createLocalSegmentFile(segmentSrcFolder,
        "test_segment_loader/2014-11-20T00:00:00.000Z_2014-11-21T00:00:00.000Z/2015-05-27T03:38:35.683Z/0");

    Assert.assertFalse("Expect cache miss before downloading segment", manager.isSegmentCached(segmentToDownload2));

    File segmentFile2 = manager.getSegmentFiles(segmentToDownload2);
    Assert.assertTrue(segmentFile2.getAbsolutePath().contains("/local_storage_folder2/"));
    Assert.assertTrue("Expect cache hit after downloading segment", manager.isSegmentCached(segmentToDownload2));


    // Segment 3 should be downloaded in local_storage_folder3, segment3 size 20L
    final DataSegment segmentToDownload3 = dataSegmentWithInterval("2014-12-20T00:00:00Z/P1D", 20L).withLoadSpec(
        ImmutableMap.of(
        "type",
        "local",
        "path",
        segmentSrcFolder.getCanonicalPath()
          + "/test_segment_loader"
          + "/2014-12-20T00:00:00.000Z_2014-12-21T00:00:00.000Z/2015-05-27T03:38:35.683Z"
          + "/0/index.zip"
      )
    );
    // manually create a local segment under segmentSrcFolder
    createLocalSegmentFile(segmentSrcFolder,
        "test_segment_loader/2014-12-20T00:00:00.000Z_2014-12-21T00:00:00.000Z/2015-05-27T03:38:35.683Z/0");

    File segmentFile3 = manager.getSegmentFiles(segmentToDownload3);
    Assert.assertTrue(segmentFile3.getAbsolutePath().contains("/local_storage_folder3/"));
    Assert.assertTrue("Expect cache hit after downloading segment", manager.isSegmentCached(segmentToDownload3));

    // Now the storage locations local_storage_folder1, local_storage_folder2 and local_storage_folder3 have 10, 5 and
    // 20 bytes occupied respectively. The default strategy should pick location2 (as it has least bytes used) for the
    // next segment to be downloaded asserting the least bytes used distribution of segments.
    final DataSegment segmentToDownload4 = dataSegmentWithInterval("2014-08-20T00:00:00Z/P1D").withLoadSpec(
        ImmutableMap.of(
        "type",
        "local",
        "path",
        segmentSrcFolder.getCanonicalPath()
          + "/test_segment_loader"
          + "/2014-08-20T00:00:00.000Z_2014-08-21T00:00:00.000Z/2015-05-27T03:38:35.683Z"
          + "/0/index.zip"
      )
    );
    // manually create a local segment under segmentSrcFolder
    createLocalSegmentFile(segmentSrcFolder, "test_segment_loader/2014-08-20T00:00:00.000Z_2014-08-21T00:00:00" +
        ".000Z/2015-05-27T03:38:35.683Z/0");

    Assert.assertFalse("Expect cache miss before downloading segment", manager.isSegmentCached(segmentToDownload4));

    File segmentFile1 = manager.getSegmentFiles(segmentToDownload4);
    Assert.assertTrue(segmentFile1.getAbsolutePath().contains("/local_storage_folder2/"));
    Assert.assertTrue("Expect cache hit after downloading segment", manager.isSegmentCached(segmentToDownload4));

  }

  @Test
  public void testSegmentDistributionUsingRandomStrategy() throws Exception
  {
    final List<StorageLocationConfig> locationConfigs = new ArrayList<>();
    final StorageLocationConfig locationConfig = createStorageLocationConfig("local_storage_folder", 10L,
            true);
    final StorageLocationConfig locationConfig2 = createStorageLocationConfig("local_storage_folder2", 100L,
            false);
    final StorageLocationConfig locationConfig3 = createStorageLocationConfig("local_storage_folder3", 9L,
            true);
    locationConfigs.add(locationConfig);
    locationConfigs.add(locationConfig2);
    locationConfigs.add(locationConfig3);

    SegmentLoaderConfig segmentLoaderConfig = new SegmentLoaderConfig().withLocations(locationConfigs);

    manager = new SegmentLocalCacheManager(
            new SegmentLoaderConfig().withLocations(locationConfigs),
            new RandomStorageLocationSelectorStrategy(segmentLoaderConfig.toStorageLocations()),
            jsonMapper
    );

    final File segmentSrcFolder = tmpFolder.newFolder("segmentSrcFolder");

    // Segment 1 should be downloaded in local_storage_folder, segment1 size 10L
    final DataSegment segmentToDownload = dataSegmentWithInterval("2014-10-20T00:00:00Z/P1D", 10L).withLoadSpec(
            ImmutableMap.of(
                    "type",
                    "local",
                    "path",
                    segmentSrcFolder.getCanonicalPath()
                            + "/test_segment_loader"
                            + "/2014-10-20T00:00:00.000Z_2014-10-21T00:00:00.000Z/2015-05-27T03:38:35.683Z"
                            + "/0/index.zip"
            )
    );
    // manually create a local segment under segmentSrcFolder
    createLocalSegmentFile(segmentSrcFolder,
            "test_segment_loader/2014-10-20T00:00:00.000Z_2014-10-21T00:00:00.000Z/2015-05-27T03:38:35.683Z/0");

    Assert.assertFalse("Expect cache miss before downloading segment", manager.isSegmentCached(segmentToDownload));

    File segmentFile = manager.getSegmentFiles(segmentToDownload);
    Assert.assertTrue(segmentFile.getAbsolutePath().contains("/local_storage_folder/"));
    Assert.assertTrue("Expect cache hit after downloading segment", manager.isSegmentCached(segmentToDownload));

    // Segment 2 should be downloaded in local_storage_folder3, segment2 size 9L
    final DataSegment segmentToDownload2 = dataSegmentWithInterval("2014-11-20T00:00:00Z/P1D", 9L).withLoadSpec(
            ImmutableMap.of(
                    "type",
                    "local",
                    "path",
                    segmentSrcFolder.getCanonicalPath()
                            + "/test_segment_loader"
                            + "/2014-11-20T00:00:00.000Z_2014-11-21T00:00:00.000Z/2015-05-27T03:38:35.683Z"
                            + "/0/index.zip"
            )
    );
    // manually create a local segment under segmentSrcFolder
    createLocalSegmentFile(segmentSrcFolder,
            "test_segment_loader/2014-11-20T00:00:00.000Z_2014-11-21T00:00:00.000Z/2015-05-27T03:38:35.683Z/0");

    Assert.assertFalse("Expect cache miss before downloading segment", manager.isSegmentCached(segmentToDownload2));

    File segmentFile2 = manager.getSegmentFiles(segmentToDownload2);
    Assert.assertTrue(segmentFile2.getAbsolutePath().contains("/local_storage_folder3/"));
    Assert.assertTrue("Expect cache hit after downloading segment", manager.isSegmentCached(segmentToDownload2));


    // Segment 3 should not be downloaded, segment3 size 20L
    final DataSegment segmentToDownload3 = dataSegmentWithInterval("2014-12-20T00:00:00Z/P1D", 20L).withLoadSpec(
            ImmutableMap.of(
                    "type",
                    "local",
                    "path",
                    segmentSrcFolder.getCanonicalPath()
                            + "/test_segment_loader"
                            + "/2014-12-20T00:00:00.000Z_2014-12-21T00:00:00.000Z/2015-05-27T03:38:35.683Z"
                            + "/0/index.zip"
            )
    );
    // manually create a local segment under segmentSrcFolder
    createLocalSegmentFile(segmentSrcFolder,
            "test_segment_loader/2014-12-20T00:00:00.000Z_2014-12-21T00:00:00.000Z/2015-05-27T03:38:35.683Z/0");

    try {
      // expect failure
      manager.getSegmentFiles(segmentToDownload3);
      Assert.fail();
    }
    catch (SegmentLoadingException e) {
    }
    Assert.assertFalse("Expect cache miss after dropping segment", manager.isSegmentCached(segmentToDownload3));
  }

  @Test
  public void testGetSegmentFilesWhenDownloadStartMarkerExists() throws Exception
  {
    final File localStorageFolder = tmpFolder.newFolder("local_storage_folder");

    final DataSegment segmentToDownload = dataSegmentWithInterval("2014-10-20T00:00:00Z/P1D").withLoadSpec(
        ImmutableMap.of(
            "type",
            "local",
            "path",
            localStorageFolder.getCanonicalPath()
            + "/test_segment_loader"
            + "/2014-10-20T00:00:00.000Z_2014-10-21T00:00:00.000Z/2015-05-27T03:38:35.683Z"
            + "/0/index.zip"
        )
    );

    // manually create a local segment under localStorageFolder
    final File localSegmentFile = new File(
        localStorageFolder,
        "test_segment_loader/2014-10-20T00:00:00.000Z_2014-10-21T00:00:00.000Z/2015-05-27T03:38:35.683Z/0"
    );
    FileUtils.mkdirp(localSegmentFile);
    final File indexZip = new File(localSegmentFile, "index.zip");
    Assert.assertTrue(indexZip.createNewFile());

    final File cachedSegmentDir = manager.getSegmentFiles(segmentToDownload);
    Assert.assertTrue("Expect cache hit after downloading segment", manager.isSegmentCached(segmentToDownload));

    // Emulate a corrupted segment file
    final File downloadMarker = new File(
        cachedSegmentDir,
        SegmentLocalCacheManager.DOWNLOAD_START_MARKER_FILE_NAME
    );
    Assert.assertTrue(downloadMarker.createNewFile());

    Assert.assertFalse("Expect cache miss for corrupted segment file", manager.isSegmentCached(segmentToDownload));
    Assert.assertFalse(cachedSegmentDir.exists());
  }

  @Test
  public void testReserveSegment()
  {
    final DataSegment dataSegment = dataSegmentWithInterval("2014-10-20T00:00:00Z/P1D").withSize(100L);
    final StorageLocation firstLocation = new StorageLocation(localSegmentCacheFolder, 200L, 0.0d);
    final StorageLocation secondLocation = new StorageLocation(localSegmentCacheFolder, 150L, 0.0d);

    manager = new SegmentLocalCacheManager(
        Arrays.asList(secondLocation, firstLocation),
        new SegmentLoaderConfig(),
        new RoundRobinStorageLocationSelectorStrategy(Arrays.asList(firstLocation, secondLocation)),
        jsonMapper
    );
    Assert.assertTrue(manager.reserve(dataSegment));
    Assert.assertTrue(firstLocation.isReserved(DataSegmentPusher.getDefaultStorageDir(dataSegment, false)));
    Assert.assertEquals(100L, firstLocation.availableSizeBytes());
    Assert.assertEquals(150L, secondLocation.availableSizeBytes());

    // Reserving again should be no-op
    Assert.assertTrue(manager.reserve(dataSegment));
    Assert.assertTrue(firstLocation.isReserved(DataSegmentPusher.getDefaultStorageDir(dataSegment, false)));
    Assert.assertEquals(100L, firstLocation.availableSizeBytes());
    Assert.assertEquals(150L, secondLocation.availableSizeBytes());

    // Reserving a second segment should now go to a different location
    final DataSegment otherSegment = dataSegmentWithInterval("2014-10-21T00:00:00Z/P1D").withSize(100L);
    Assert.assertTrue(manager.reserve(otherSegment));
    Assert.assertTrue(firstLocation.isReserved(DataSegmentPusher.getDefaultStorageDir(dataSegment, false)));
    Assert.assertFalse(firstLocation.isReserved(DataSegmentPusher.getDefaultStorageDir(otherSegment, false)));
    Assert.assertTrue(secondLocation.isReserved(DataSegmentPusher.getDefaultStorageDir(otherSegment, false)));
    Assert.assertEquals(100L, firstLocation.availableSizeBytes());
    Assert.assertEquals(50L, secondLocation.availableSizeBytes());
  }

  @Test
  public void testReserveNotEnoughSpace()
  {
    final DataSegment dataSegment = dataSegmentWithInterval("2014-10-20T00:00:00Z/P1D").withSize(100L);
    final StorageLocation firstLocation = new StorageLocation(localSegmentCacheFolder, 50L, 0.0d);
    final StorageLocation secondLocation = new StorageLocation(localSegmentCacheFolder, 150L, 0.0d);

    manager = new SegmentLocalCacheManager(
        Arrays.asList(secondLocation, firstLocation),
        new SegmentLoaderConfig(),
        new RoundRobinStorageLocationSelectorStrategy(Arrays.asList(firstLocation, secondLocation)),
        jsonMapper
    );

    // should go to second location if first one doesn't have enough space
    Assert.assertTrue(manager.reserve(dataSegment));
    Assert.assertTrue(secondLocation.isReserved(DataSegmentPusher.getDefaultStorageDir(dataSegment, false)));
    Assert.assertEquals(50L, firstLocation.availableSizeBytes());
    Assert.assertEquals(50L, secondLocation.availableSizeBytes());

    final DataSegment otherSegment = dataSegmentWithInterval("2014-10-21T00:00:00Z/P1D").withSize(100L);
    Assert.assertFalse(manager.reserve(otherSegment));
    Assert.assertEquals(50L, firstLocation.availableSizeBytes());
    Assert.assertEquals(50L, secondLocation.availableSizeBytes());
  }

  @Test
  public void testSegmentDownloadWhenLocationReserved() throws Exception
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

    manager = new SegmentLocalCacheManager(
        new SegmentLoaderConfig().withLocations(locationConfigs),
        new RoundRobinStorageLocationSelectorStrategy(locations),
        jsonMapper
    );

    StorageLocation location3 = manager.getLocations().get(2);
    Assert.assertEquals(locationConfig3.getPath(), location3.getPath());
    final File segmentSrcFolder = tmpFolder.newFolder("segmentSrcFolder");

    // Segment should be downloaded in local_storage_folder3 even if that is the third location
    final DataSegment segmentToDownload = dataSegmentWithInterval("2014-10-20T00:00:00Z/P1D").withLoadSpec(
        ImmutableMap.of(
            "type",
            "local",
            "path",
            segmentSrcFolder.getCanonicalPath()
                + "/test_segment_loader"
                + "/2014-10-20T00:00:00.000Z_2014-10-21T00:00:00.000Z/2015-05-27T03:38:35.683Z"
                + "/0/index.zip"
        )
    );
    String segmentDir = DataSegmentPusher.getDefaultStorageDir(segmentToDownload, false);
    location3.reserve(segmentDir, segmentToDownload);
    // manually create a local segment under segmentSrcFolder
    createLocalSegmentFile(segmentSrcFolder, "test_segment_loader/2014-10-20T00:00:00.000Z_2014-10-21T00:00:00.000Z/2015-05-27T03:38:35.683Z/0");

    Assert.assertFalse("Expect cache miss before downloading segment", manager.isSegmentCached(segmentToDownload));

    File segmentFile = manager.getSegmentFiles(segmentToDownload);
    Assert.assertTrue(segmentFile.getAbsolutePath().contains("/local_storage_folder3/"));
    Assert.assertTrue("Expect cache hit after downloading segment", manager.isSegmentCached(segmentToDownload));

    manager.cleanup(segmentToDownload);
    Assert.assertFalse("Expect cache miss after dropping segment", manager.isSegmentCached(segmentToDownload));
    Assert.assertFalse(location3.isReserved(segmentDir));
  }

  @Test
  public void testRelease()
  {
    final DataSegment dataSegment = dataSegmentWithInterval("2014-10-20T00:00:00Z/P1D").withSize(100L);
    final StorageLocation firstLocation = new StorageLocation(localSegmentCacheFolder, 50L, 0.0d);
    final StorageLocation secondLocation = new StorageLocation(localSegmentCacheFolder, 150L, 0.0d);

    manager = new SegmentLocalCacheManager(
        Arrays.asList(secondLocation, firstLocation),
        new SegmentLoaderConfig(),
        new RoundRobinStorageLocationSelectorStrategy(Arrays.asList(firstLocation, secondLocation)),
        jsonMapper
    );

    manager.reserve(dataSegment);
    manager.release(dataSegment);
    Assert.assertEquals(50L, firstLocation.availableSizeBytes());
    Assert.assertEquals(150L, secondLocation.availableSizeBytes());
    Assert.assertFalse(firstLocation.isReserved(DataSegmentPusher.getDefaultStorageDir(dataSegment, false)));
    Assert.assertFalse(secondLocation.isReserved(DataSegmentPusher.getDefaultStorageDir(dataSegment, false)));

    // calling release again should have no effect
    manager.release(dataSegment);
    Assert.assertEquals(50L, firstLocation.availableSizeBytes());
    Assert.assertEquals(150L, secondLocation.availableSizeBytes());
  }
}
