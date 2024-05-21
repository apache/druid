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
import org.apache.druid.guice.ServerTypeConfig;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.TestIndex;
import org.apache.druid.segment.loading.DataSegmentPusher;
import org.apache.druid.segment.loading.SegmentCacheManager;
import org.apache.druid.segment.loading.SegmentLoaderConfig;
import org.apache.druid.segment.loading.SegmentLoadingException;
import org.apache.druid.segment.loading.SegmentLocalCacheManager;
import org.apache.druid.server.SegmentManager;
import org.apache.druid.server.TestSegmentUtils;
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.apache.druid.timeline.DataSegment;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;

/**
 * Similar to {@link SegmentLoadDropHandlerTest}. This class includes tests that cover the
 * storage location layer as well.
 */
public class SegmentLoadDropHandlerCacheTest
{
  private static final long MAX_SIZE = 1000L;
  private static final long SEGMENT_SIZE = 100L;
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();
  private SegmentLoadDropHandler loadDropHandler;
  private TestStorageLocation storageLoc;
  private DataSegmentAnnouncer segmentAnnouncer;

  @Before
  public void setup() throws IOException
  {
    storageLoc = new TestStorageLocation(temporaryFolder);
    SegmentLoaderConfig config = new SegmentLoaderConfig()
        .withLocations(Collections.singletonList(storageLoc.toStorageLocationConfig(MAX_SIZE, null)))
        .withInfoDir(storageLoc.getInfoDir());
    final ObjectMapper objectMapper = TestHelper.makeJsonMapper();
    objectMapper.registerSubtypes(TestSegmentUtils.TestLoadSpec.class);
    objectMapper.registerSubtypes(TestSegmentUtils.TestSegmentizerFactory.class);

    SegmentCacheManager cacheManager = new SegmentLocalCacheManager(config, TestIndex.INDEX_IO, objectMapper);
    SegmentManager segmentManager = new SegmentManager(cacheManager);
    segmentAnnouncer = Mockito.mock(DataSegmentAnnouncer.class);
    loadDropHandler = new SegmentLoadDropHandler(
        config,
        segmentAnnouncer,
        Mockito.mock(DataSegmentServerAnnouncer.class),
        segmentManager,
        new ServerTypeConfig(ServerType.HISTORICAL)
    );
    EmittingLogger.registerEmitter(new NoopServiceEmitter());
  }

  @Test
  public void testLoadLocalCache() throws IOException, SegmentLoadingException
  {
    File cacheDir = storageLoc.getCacheDir();

    // write some segments to file bypassing loadDropHandler
    int numSegments = (int) (MAX_SIZE / SEGMENT_SIZE);
    List<DataSegment> expectedSegments = new ArrayList<>();
    for (int i = 0; i < numSegments; i++) {
      String name = "segment-" + i;
      DataSegment segment = makeSegment("test", name);
      storageLoc.writeSegmentInfoToCache(segment);
      String storageDir = DataSegmentPusher.getDefaultStorageDir(segment, false);
      File segmentDir = new File(cacheDir, storageDir);
      new TestSegmentUtils.TestLoadSpec((int) SEGMENT_SIZE, name).loadSegment(segmentDir);
      expectedSegments.add(segment);
    }

    // Start the load drop handler
    loadDropHandler.start();

    // Verify the expected announcements
    ArgumentCaptor<Iterable<DataSegment>> argCaptor = ArgumentCaptor.forClass(Iterable.class);
    Mockito.verify(segmentAnnouncer).announceSegments(argCaptor.capture());
    List<DataSegment> announcedSegments = new ArrayList<>();
    argCaptor.getValue().forEach(announcedSegments::add);
    announcedSegments.sort(Comparator.comparing(DataSegment::getVersion));
    Assert.assertEquals(expectedSegments, announcedSegments);

    // make sure adding segments beyond allowed size fails
    Mockito.reset(segmentAnnouncer);
    DataSegment newSegment = makeSegment("test", "new-segment");
    loadDropHandler.addSegment(newSegment, null);
    Mockito.verify(segmentAnnouncer, Mockito.never()).announceSegment(any());
    Mockito.verify(segmentAnnouncer, Mockito.never()).announceSegments(any());

    // clearing some segment should allow for new segments
    loadDropHandler.removeSegment(expectedSegments.get(0), null, false);
    loadDropHandler.addSegment(newSegment, null);
    Mockito.verify(segmentAnnouncer).announceSegment(newSegment);
  }

  private DataSegment makeSegment(String dataSource, String name)
  {
    return TestSegmentUtils.makeSegment(dataSource, name, SEGMENT_SIZE);
  }
}
