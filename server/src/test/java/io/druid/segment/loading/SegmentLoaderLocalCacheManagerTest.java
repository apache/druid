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
import io.druid.jackson.DefaultObjectMapper;
import io.druid.segment.TestHelper;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.NoneShardSpec;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.List;

public class SegmentLoaderLocalCacheManagerTest
{
  @Rule
  public final TemporaryFolder tmpFolder = new TemporaryFolder();

  private final ObjectMapper jsonMapper;

  private File localSegmentCacheFolder;
  private SegmentLoaderLocalCacheManager manager;

  public SegmentLoaderLocalCacheManagerTest()
  {
    jsonMapper = new DefaultObjectMapper();
    jsonMapper.registerSubtypes(new NamedType(LocalLoadSpec.class, "local"));
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
    localSegmentCacheFolder = tmpFolder.newFolder("segment_cache_folder");

    final List<StorageLocationConfig> locations = Lists.newArrayList();
    final StorageLocationConfig locationConfig = new StorageLocationConfig();
    locationConfig.setPath(localSegmentCacheFolder);
    locationConfig.setMaxSize(10000000000L);
    locations.add(locationConfig);

    manager = new SegmentLoaderLocalCacheManager(
        new MMappedQueryableIndexFactory(TestHelper.getTestIndexIO()),
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

  private DataSegment dataSegmentWithInterval(String intervalStr)
  {
    return DataSegment.builder()
                      .dataSource("test_segment_loader")
                      .interval(new Interval(intervalStr))
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
                      .shardSpec(new NoneShardSpec())
                      .binaryVersion(9)
                      .size(0)
                      .build();
  }
}
