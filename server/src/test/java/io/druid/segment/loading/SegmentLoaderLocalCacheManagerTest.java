/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.segment.loading;

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.metamx.emitter.EmittingLogger;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.java.util.common.Intervals;
import io.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import io.druid.segment.writeout.SegmentWriteOutMediumFactory;
import io.druid.segment.writeout.TmpFileSegmentWriteOutMediumFactory;
import io.druid.segment.TestHelper;
import io.druid.server.metrics.NoopServiceEmitter;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.NoneShardSpec;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;

@RunWith(Parameterized.class)
public class SegmentLoaderLocalCacheManagerTest
{
  @Parameterized.Parameters
  public static Collection<?> constructorFeeder() throws IOException
  {
    return ImmutableList.of(
        new Object[] {TmpFileSegmentWriteOutMediumFactory.instance()},
        new Object[] {OffHeapMemorySegmentWriteOutMediumFactory.instance()}
    );
  }

  @Rule
  public final TemporaryFolder tmpFolder = new TemporaryFolder();

  private final ObjectMapper jsonMapper;
  private final SegmentWriteOutMediumFactory segmentWriteOutMediumFactory;

  private File localSegmentCacheFolder;
  private SegmentLoaderLocalCacheManager manager;

  public SegmentLoaderLocalCacheManagerTest(SegmentWriteOutMediumFactory segmentWriteOutMediumFactory)
  {
    jsonMapper = new DefaultObjectMapper();
    jsonMapper.registerSubtypes(new NamedType(LocalLoadSpec.class, "local"));
    jsonMapper.setInjectableValues(
        new InjectableValues.Std().addValue(
            LocalDataSegmentPuller.class,
            new LocalDataSegmentPuller()
        )
    );
    this.segmentWriteOutMediumFactory = segmentWriteOutMediumFactory;
  }

  @Before
  public void setUp() throws Exception
  {
    EmittingLogger.registerEmitter(new NoopServiceEmitter());
    localSegmentCacheFolder = tmpFolder.newFolder("segment_cache_folder");

    final List<StorageLocationConfig> locations = Lists.newArrayList();
    final StorageLocationConfig locationConfig = new StorageLocationConfig();
    locationConfig.setPath(localSegmentCacheFolder);
    locationConfig.setMaxSize(10000000000L);
    locations.add(locationConfig);

    manager = new SegmentLoaderLocalCacheManager(
        TestHelper.getTestIndexIO(segmentWriteOutMediumFactory),
        new SegmentLoaderConfig().withLocations(locations),
        jsonMapper
    );
  }

  @Test
  public void testIfSegmentIsLoaded() throws Exception
  {
    final DataSegment cachedSegment = dataSegmentWithInterval("2014-10-20T00:00:00Z/P1D");
    final File cachedSegmentFile = new File(
        localSegmentCacheFolder,
        "test_segment_loader/2014-10-20T00:00:00.000Z_2014-10-21T00:00:00.000Z/2015-05-27T03:38:35.683Z/0"
    );
    cachedSegmentFile.mkdirs();

    Assert.assertTrue("Expect cache hit", manager.isSegmentLoaded(cachedSegment));

    final DataSegment uncachedSegment = dataSegmentWithInterval("2014-10-21T00:00:00Z/P1D");
    Assert.assertFalse("Expect cache miss", manager.isSegmentLoaded(uncachedSegment));
  }

  @Test
  public void testGetAndCleanSegmentFiles() throws Exception
  {
    final File localStorageFolder = tmpFolder.newFolder("local_storage_folder");

    final DataSegment segmentToDownload = dataSegmentWithInterval("2014-10-20T00:00:00Z/P1D").withLoadSpec(
        ImmutableMap.<String, Object>of(
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
    localSegmentFile.mkdirs();
    final File indexZip = new File(localSegmentFile, "index.zip");
    indexZip.createNewFile();

    Assert.assertFalse("Expect cache miss before downloading segment", manager.isSegmentLoaded(segmentToDownload));

    manager.getSegmentFiles(segmentToDownload);
    Assert.assertTrue("Expect cache hit after downloading segment", manager.isSegmentLoaded(segmentToDownload));

    manager.cleanup(segmentToDownload);
    Assert.assertFalse("Expect cache miss after dropping segment", manager.isSegmentLoaded(segmentToDownload));
  }

  @Test
  public void testRetrySuccessAtFirstLocation() throws Exception
  {
    final File localStorageFolder = tmpFolder.newFolder("local_storage_folder");

    final List<StorageLocationConfig> locations = Lists.newArrayList();
    final StorageLocationConfig locationConfig = new StorageLocationConfig();
    locationConfig.setPath(localStorageFolder);
    locationConfig.setMaxSize(10000000000L);
    locations.add(locationConfig);
    final StorageLocationConfig locationConfig2 = new StorageLocationConfig();
    final File localStorageFolder2 = tmpFolder.newFolder("local_storage_folder2");
    locationConfig2.setPath(localStorageFolder2);
    locationConfig2.setMaxSize(1000000000L);
    locations.add(locationConfig2);

    manager = new SegmentLoaderLocalCacheManager(
        TestHelper.getTestIndexIO(segmentWriteOutMediumFactory),
        new SegmentLoaderConfig().withLocations(locations),
        jsonMapper
    );
    final File segmentSrcFolder = tmpFolder.newFolder("segmentSrcFolder");
    final DataSegment segmentToDownload = dataSegmentWithInterval("2014-10-20T00:00:00Z/P1D").withLoadSpec(
        ImmutableMap.<String, Object>of(
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
    localSegmentFile.mkdirs();
    final File indexZip = new File(localSegmentFile, "index.zip");
    indexZip.createNewFile();

    Assert.assertFalse("Expect cache miss before downloading segment", manager.isSegmentLoaded(segmentToDownload));

    File segmentFile = manager.getSegmentFiles(segmentToDownload);
    Assert.assertTrue(segmentFile.getAbsolutePath().contains("/local_storage_folder/"));
    Assert.assertTrue("Expect cache hit after downloading segment", manager.isSegmentLoaded(segmentToDownload));

    manager.cleanup(segmentToDownload);
    Assert.assertFalse("Expect cache miss after dropping segment", manager.isSegmentLoaded(segmentToDownload));
  }

  @Test
  public void testRetrySuccessAtSecondLocation() throws Exception
  {
    final List<StorageLocationConfig> locations = Lists.newArrayList();
    final StorageLocationConfig locationConfig = new StorageLocationConfig();
    final File localStorageFolder = tmpFolder.newFolder("local_storage_folder");
    // mock can't write in first location
    localStorageFolder.setWritable(false);
    locationConfig.setPath(localStorageFolder);
    locationConfig.setMaxSize(1000000000L);
    locations.add(locationConfig);
    final StorageLocationConfig locationConfig2 = new StorageLocationConfig();
    final File localStorageFolder2 = tmpFolder.newFolder("local_storage_folder2");
    locationConfig2.setPath(localStorageFolder2);
    locationConfig2.setMaxSize(10000000L);
    locations.add(locationConfig2);

    manager = new SegmentLoaderLocalCacheManager(
        TestHelper.getTestIndexIO(segmentWriteOutMediumFactory),
        new SegmentLoaderConfig().withLocations(locations),
        jsonMapper
    );
    final File segmentSrcFolder = tmpFolder.newFolder("segmentSrcFolder");
    final DataSegment segmentToDownload = dataSegmentWithInterval("2014-10-20T00:00:00Z/P1D").withLoadSpec(
        ImmutableMap.<String, Object>of(
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
    localSegmentFile.mkdirs();
    final File indexZip = new File(localSegmentFile, "index.zip");
    indexZip.createNewFile();

    Assert.assertFalse("Expect cache miss before downloading segment", manager.isSegmentLoaded(segmentToDownload));

    File segmentFile = manager.getSegmentFiles(segmentToDownload);
    Assert.assertTrue(segmentFile.getAbsolutePath().contains("/local_storage_folder2/"));
    Assert.assertTrue("Expect cache hit after downloading segment", manager.isSegmentLoaded(segmentToDownload));

    manager.cleanup(segmentToDownload);
    Assert.assertFalse("Expect cache miss after dropping segment", manager.isSegmentLoaded(segmentToDownload));
  }

  @Test
  public void testRetryAllFail() throws Exception
  {
    final List<StorageLocationConfig> locations = Lists.newArrayList();
    final StorageLocationConfig locationConfig = new StorageLocationConfig();
    final File localStorageFolder = tmpFolder.newFolder("local_storage_folder");
    // mock can't write in first location
    localStorageFolder.setWritable(false);
    locationConfig.setPath(localStorageFolder);
    locationConfig.setMaxSize(1000000000L);
    locations.add(locationConfig);
    final StorageLocationConfig locationConfig2 = new StorageLocationConfig();
    final File localStorageFolder2 = tmpFolder.newFolder("local_storage_folder2");
    // mock can't write in second location
    localStorageFolder2.setWritable(false);
    locationConfig2.setPath(localStorageFolder2);
    locationConfig2.setMaxSize(10000000L);
    locations.add(locationConfig2);

    manager = new SegmentLoaderLocalCacheManager(
        TestHelper.getTestIndexIO(segmentWriteOutMediumFactory),
        new SegmentLoaderConfig().withLocations(locations),
        jsonMapper
    );
    final File segmentSrcFolder = tmpFolder.newFolder("segmentSrcFolder");
    final DataSegment segmentToDownload = dataSegmentWithInterval("2014-10-20T00:00:00Z/P1D").withLoadSpec(
        ImmutableMap.<String, Object>of(
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
    localSegmentFile.mkdirs();
    final File indexZip = new File(localSegmentFile, "index.zip");
    indexZip.createNewFile();

    try {
      // expect failure
      manager.getSegmentFiles(segmentToDownload);
      Assert.fail();
    }
    catch (SegmentLoadingException e) {
    }
    Assert.assertFalse("Expect cache miss after dropping segment", manager.isSegmentLoaded(segmentToDownload));
    manager.cleanup(segmentToDownload);
  }

  @Test
  public void testEmptyToFullOrder() throws Exception
  {
    final List<StorageLocationConfig> locations = Lists.newArrayList();
    final StorageLocationConfig locationConfig = new StorageLocationConfig();
    final File localStorageFolder = tmpFolder.newFolder("local_storage_folder");
    localStorageFolder.setWritable(true);
    locationConfig.setPath(localStorageFolder);
    locationConfig.setMaxSize(10L);
    locations.add(locationConfig);
    final StorageLocationConfig locationConfig2 = new StorageLocationConfig();
    final File localStorageFolder2 = tmpFolder.newFolder("local_storage_folder2");
    localStorageFolder2.setWritable(true);
    locationConfig2.setPath(localStorageFolder2);
    locationConfig2.setMaxSize(10L);
    locations.add(locationConfig2);

    manager = new SegmentLoaderLocalCacheManager(
        TestHelper.getTestIndexIO(segmentWriteOutMediumFactory),
        new SegmentLoaderConfig().withLocations(locations),
        jsonMapper
    );
    final File segmentSrcFolder = tmpFolder.newFolder("segmentSrcFolder");
    final DataSegment segmentToDownload = dataSegmentWithInterval("2014-10-20T00:00:00Z/P1D").withLoadSpec(
        ImmutableMap.<String, Object>of(
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
    localSegmentFile.mkdirs();
    final File indexZip = new File(localSegmentFile, "index.zip");
    indexZip.createNewFile();

    Assert.assertFalse("Expect cache miss before downloading segment", manager.isSegmentLoaded(segmentToDownload));

    File segmentFile = manager.getSegmentFiles(segmentToDownload);
    Assert.assertTrue(segmentFile.getAbsolutePath().contains("/local_storage_folder/"));
    Assert.assertTrue("Expect cache hit after downloading segment", manager.isSegmentLoaded(segmentToDownload));

    final DataSegment segmentToDownload2 = dataSegmentWithInterval("2014-11-20T00:00:00Z/P1D").withLoadSpec(
        ImmutableMap.<String, Object>of(
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
    localSegmentFile2.mkdirs();
    final File indexZip2 = new File(localSegmentFile2, "index.zip");
    indexZip2.createNewFile();

    File segmentFile2 = manager.getSegmentFiles(segmentToDownload2);
    Assert.assertTrue(segmentFile2.getAbsolutePath().contains("/local_storage_folder2/"));
    Assert.assertTrue("Expect cache hit after downloading segment", manager.isSegmentLoaded(segmentToDownload2));

    manager.cleanup(segmentToDownload2);
    Assert.assertFalse("Expect cache miss after dropping segment", manager.isSegmentLoaded(segmentToDownload2));
  }

  private DataSegment dataSegmentWithInterval(String intervalStr)
  {
    return DataSegment.builder()
                      .dataSource("test_segment_loader")
                      .interval(Intervals.of(intervalStr))
                      .loadSpec(
                          ImmutableMap.<String, Object>of(
                              "type",
                              "local",
                              "path",
                              "somewhere"
                          )
                      )
                      .version("2015-05-27T03:38:35.683Z")
                      .dimensions(ImmutableList.<String>of())
                      .metrics(ImmutableList.<String>of())
                      .shardSpec(NoneShardSpec.instance())
                      .binaryVersion(9)
                      .size(10L)
                      .build();
  }
}
