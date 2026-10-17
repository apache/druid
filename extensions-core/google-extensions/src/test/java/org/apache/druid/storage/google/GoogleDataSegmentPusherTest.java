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

package org.apache.druid.storage.google;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.segment.IndexBuilder;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.loading.DeepStorageSegmentConfig;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.partition.NoneShardSpec;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.easymock.EasyMock;
import org.easymock.EasyMockSupport;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;

public class GoogleDataSegmentPusherTest extends EasyMockSupport
{
  @TempDir
  public File tempFolder;

  private static final String BUCKET = "bucket";
  private static final String PREFIX = "prefix";
  private static final GoogleInputDataConfig INPUT_DATA_CONFIG = new GoogleInputDataConfig();
  private static final DeepStorageSegmentConfig ZIP_CONFIG = new DeepStorageSegmentConfig(true);
  private static final DeepStorageSegmentConfig NO_ZIP_CONFIG = new DeepStorageSegmentConfig(false);

  private GoogleStorage storage;
  private GoogleAccountConfig googleAccountConfig;

  @BeforeEach
  public void before()
  {
    storage = createMock(GoogleStorage.class);
    googleAccountConfig = new GoogleAccountConfig();
    googleAccountConfig.setBucket(BUCKET);
    googleAccountConfig.setPrefix(PREFIX);
  }

  @Test
  public void testPush() throws Exception
  {
    // Create a mock segment on disk
    File tmp = new File(tempFolder, "version.bin");

    final byte[] data = new byte[]{0x0, 0x0, 0x0, 0x1};
    Files.write(data, tmp);
    final long size = data.length;

    DataSegment segmentToPush = new DataSegment(
        "foo",
        Intervals.of("2015/2016"),
        "0",
        new HashMap<>(),
        new ArrayList<>(),
        new ArrayList<>(),
        NoneShardSpec.instance(),
        0,
        size
    );

    GoogleDataSegmentPusher pusher = createMockBuilder(GoogleDataSegmentPusher.class)
        .withConstructor(storage, googleAccountConfig, INPUT_DATA_CONFIG, ZIP_CONFIG)
        .addMockedMethod("insert", File.class, String.class, String.class)
        .createMock();

    final String storageDir = pusher.getStorageDir(segmentToPush, false);
    final String indexPath = PREFIX + "/" + storageDir + "/" + "index.zip";

    pusher.insert(
        EasyMock.anyObject(File.class),
        EasyMock.eq("application/zip"),
        EasyMock.eq(indexPath)
    );
    EasyMock.expectLastCall();

    replayAll();

    DataSegment segment = pusher.push(tempFolder, segmentToPush, false);

    Assertions.assertEquals(segmentToPush.getSize(), segment.getSize());
    Assertions.assertEquals(segmentToPush, segment);
    Assertions.assertEquals(ImmutableMap.of(
        "type", GoogleStorageDruidModule.SCHEME,
        "bucket", BUCKET,
        "path", indexPath
    ), segment.getLoadSpec());

    verifyAll();
  }

  @Test
  public void testPushNoZip() throws Exception
  {
    final byte[] data = new byte[]{0x0, 0x0, 0x0, 0x1};
    Files.write(data, new File(tempFolder, "version.bin"));
    Files.write(data, new File(tempFolder, "meta.smoosh"));

    DataSegment segmentToPush = newSegmentToPush(2 * data.length);

    GoogleDataSegmentPusher pusher = createMockBuilder(GoogleDataSegmentPusher.class)
        .withConstructor(storage, googleAccountConfig, INPUT_DATA_CONFIG, NO_ZIP_CONFIG)
        .addMockedMethod("insert", File.class, String.class, String.class)
        .createMock();

    final String expectedDir = PREFIX + "/" + pusher.getStorageDir(segmentToPush, false);
    pusher.insert(EasyMock.anyObject(File.class), EasyMock.anyString(), EasyMock.eq(expectedDir + "/version.bin"));
    EasyMock.expectLastCall();
    pusher.insert(EasyMock.anyObject(File.class), EasyMock.anyString(), EasyMock.eq(expectedDir + "/meta.smoosh"));
    EasyMock.expectLastCall();

    // nothing else is under the path, so there is nothing to clean up
    EasyMock.expect(storage.list(EasyMock.eq(BUCKET), EasyMock.eq(expectedDir + "/"), EasyMock.anyObject(), EasyMock.anyObject()))
            .andReturn(new GoogleStorageObjectPage(
                ImmutableList.of(
                    new GoogleStorageObjectMetadata(BUCKET, expectedDir + "/version.bin", (long) data.length, 0L),
                    new GoogleStorageObjectMetadata(BUCKET, expectedDir + "/meta.smoosh", (long) data.length, 0L)
                ),
                null
            ));

    replayAll();

    DataSegment segment = pusher.push(tempFolder, segmentToPush, false);

    // the trailing slash is what marks the path as a directory of files rather than a single object, and V1 (the test
    // fixture) is not V10, so rangeable is stamped false
    Assertions.assertEquals(ImmutableMap.of(
        "type", GoogleStorageDruidModule.SCHEME,
        "bucket", BUCKET,
        "path", expectedDir + "/",
        "rangeable", false
    ), segment.getLoadSpec());
    Assertions.assertEquals(2 * data.length, segment.getSize());

    verifyAll();
  }

  @Test
  public void testPushNoZipV10StampsRangeableTrue() throws Exception
  {
    // A real V10 segment rather than a hand-made file, so the layout under test is whatever IndexMergerV10 actually
    // writes: one druid.segment whose leading byte is the version getVersionFromDir reads.
    final File segmentDir = IndexBuilder.create()
                                        .useV10()
                                        .tmpDir(new File(tempFolder, "v10"))
                                        .rows(ImmutableList.of(
                                            new MapBasedInputRow(
                                                DateTimes.of("2015-01-01"),
                                                ImmutableList.of("dim"),
                                                ImmutableMap.of("dim", "a")
                                            )
                                        ))
                                        .buildMMappedIndexFile();

    final File[] segmentFiles = segmentDir.listFiles();
    Assertions.assertNotNull(segmentFiles);
    long segmentSize = 0;
    for (final File file : segmentFiles) {
      segmentSize += file.length();
    }

    DataSegment segmentToPush = newSegmentToPush(segmentSize);

    GoogleDataSegmentPusher pusher = createMockBuilder(GoogleDataSegmentPusher.class)
        .withConstructor(storage, googleAccountConfig, INPUT_DATA_CONFIG, NO_ZIP_CONFIG)
        .addMockedMethod("insert", File.class, String.class, String.class)
        .createMock();

    final String expectedDir = PREFIX + "/" + pusher.getStorageDir(segmentToPush, false);
    final ImmutableList.Builder<GoogleStorageObjectMetadata> listing = ImmutableList.builder();
    for (final File file : segmentFiles) {
      pusher.insert(
          EasyMock.anyObject(File.class),
          EasyMock.anyString(),
          EasyMock.eq(expectedDir + "/" + file.getName())
      );
      EasyMock.expectLastCall();
      listing.add(new GoogleStorageObjectMetadata(BUCKET, expectedDir + "/" + file.getName(), file.length(), 0L));
    }
    // everything under the path was written by this push, so there is nothing to clean up
    EasyMock.expect(storage.list(EasyMock.eq(BUCKET), EasyMock.eq(expectedDir + "/"), EasyMock.anyObject(), EasyMock.anyObject()))
            .andReturn(new GoogleStorageObjectPage(listing.build(), null));

    replayAll();

    DataSegment segment = pusher.push(segmentDir, segmentToPush, false);

    // a V10 segment is a single file, and that is what makes it range-readable
    Assertions.assertEquals(1, segmentFiles.length);
    Assertions.assertEquals(IndexIO.V10_FILE_NAME, segmentFiles[0].getName());
    Assertions.assertEquals(IndexIO.V10_VERSION, (int) segment.getBinaryVersion());
    Assertions.assertEquals(Boolean.TRUE, segment.getLoadSpec().get("rangeable"));

    verifyAll();
  }

  @Test
  public void testPushZipDoesNotStampRangeable() throws Exception
  {
    // The zip path uses the no-flag makeLoadSpec overload; openRangeReader returns null on the zip short circuit
    // regardless, but the loadSpec stays compact for zipped segments by omitting the field entirely.
    final byte[] data = new byte[]{0x0, 0x0, 0x0, 0x1};
    Files.write(data, new File(tempFolder, "version.bin"));

    DataSegment segmentToPush = newSegmentToPush(data.length);

    GoogleDataSegmentPusher pusher = createMockBuilder(GoogleDataSegmentPusher.class)
        .withConstructor(storage, googleAccountConfig, INPUT_DATA_CONFIG, ZIP_CONFIG)
        .addMockedMethod("insert", File.class, String.class, String.class)
        .createMock();

    pusher.insert(EasyMock.anyObject(File.class), EasyMock.eq("application/zip"), EasyMock.anyString());
    EasyMock.expectLastCall();

    replayAll();

    DataSegment segment = pusher.push(tempFolder, segmentToPush, false);

    Assertions.assertFalse(segment.getLoadSpec().containsKey("rangeable"));

    verifyAll();
  }

  @Test
  public void testPushNoZipRemovesObjectsLeftByPreviousPush() throws Exception
  {
    final byte[] data = new byte[]{0x0, 0x0, 0x0, 0x1};
    Files.write(data, new File(tempFolder, "version.bin"));

    DataSegment segmentToPush = newSegmentToPush(data.length);

    GoogleDataSegmentPusher pusher = createMockBuilder(GoogleDataSegmentPusher.class)
        .withConstructor(storage, googleAccountConfig, INPUT_DATA_CONFIG, NO_ZIP_CONFIG)
        .addMockedMethod("insert", File.class, String.class, String.class)
        .createMock();

    final String expectedDir = PREFIX + "/" + pusher.getStorageDir(segmentToPush, false);
    pusher.insert(EasyMock.anyObject(File.class), EasyMock.anyString(), EasyMock.eq(expectedDir + "/version.bin"));
    EasyMock.expectLastCall();

    // an index.zip from a zipped push and a smoosh chunk from a larger prior segment: nothing in the new segment
    // references either, but the puller would list and download both
    final String staleZip = expectedDir + "/index.zip";
    final String staleChunk = expectedDir + "/00001.smoosh";
    EasyMock.expect(storage.list(EasyMock.eq(BUCKET), EasyMock.eq(expectedDir + "/"), EasyMock.anyObject(), EasyMock.anyObject()))
            .andReturn(new GoogleStorageObjectPage(
                ImmutableList.of(
                    new GoogleStorageObjectMetadata(BUCKET, expectedDir + "/version.bin", (long) data.length, 0L),
                    new GoogleStorageObjectMetadata(BUCKET, staleZip, 128L, 0L),
                    new GoogleStorageObjectMetadata(BUCKET, staleChunk, 64L, 0L)
                ),
                null
            ));

    // only the objects this push did not write are removed
    storage.delete(BUCKET, staleZip);
    EasyMock.expectLastCall();
    storage.delete(BUCKET, staleChunk);
    EasyMock.expectLastCall();

    replayAll();

    pusher.push(tempFolder, segmentToPush, false);

    verifyAll();
  }

  @Test
  public void testBuildPath()
  {
    GoogleAccountConfig config = new GoogleAccountConfig();
    StringBuilder sb = new StringBuilder();
    sb.setLength(0);
    config.setPrefix(sb.toString()); // avoid cached empty string
    GoogleDataSegmentPusher pusher = new GoogleDataSegmentPusher(storage, config, INPUT_DATA_CONFIG, ZIP_CONFIG);
    Assertions.assertEquals("/path", pusher.buildPath("/path"));
    config.setPrefix(null);
    Assertions.assertEquals("/path", pusher.buildPath("/path"));
  }

  private static DataSegment newSegmentToPush(long size)
  {
    return DataSegment.builder(SegmentId.of("foo", Intervals.of("2015/2016"), "0", 0))
                      .shardSpec(new NumberedShardSpec(0, 1))
                      .binaryVersion(0)
                      .size(size)
                      .build();
  }
}
