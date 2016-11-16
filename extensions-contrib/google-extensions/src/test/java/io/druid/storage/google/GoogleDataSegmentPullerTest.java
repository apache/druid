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

package io.druid.storage.google;

import com.google.common.collect.ImmutableMap;
import io.druid.java.util.common.FileUtils;
import io.druid.segment.loading.SegmentLoadingException;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.NoneShardSpec;
import org.easymock.EasyMockSupport;
import org.joda.time.Interval;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class GoogleDataSegmentPullerTest extends EasyMockSupport
{
  private static final String bucket = "bucket";
  private static final String path = "/path/to/storage/index.zip";
  private static final DataSegment dataSegment = new DataSegment(
      "test",
      new Interval("2015-04-12/2015-04-13"),
      "1",
      ImmutableMap.<String, Object>of("bucket", bucket, "path", path),
      null,
      null,
      NoneShardSpec.instance(),
      0,
      1
  );

  @Test(expected = SegmentLoadingException.class)
  public void testDeleteOutputDirectoryWhenErrorIsRaisedPullingSegmentFiles()
      throws IOException, SegmentLoadingException
  {
    final File outDir = Files.createTempDirectory("druid").toFile();
    outDir.deleteOnExit();
    GoogleStorage storage = createMock(GoogleStorage.class);

    expect(storage.get(bucket, path)).andThrow(new IOException(""));

    replayAll();

    GoogleDataSegmentPuller puller = new GoogleDataSegmentPuller(storage);
    puller.getSegmentFiles(bucket, path, outDir);

    assertFalse(outDir.exists());

    verifyAll();
  }

  @Test
  public void getSegmentFilesTest() throws SegmentLoadingException
  {
    final File outDir = new File("");
    final FileUtils.FileCopyResult result = createMock(FileUtils.FileCopyResult.class);
    GoogleStorage storage = createMock(GoogleStorage.class);
    GoogleDataSegmentPuller puller = createMockBuilder(GoogleDataSegmentPuller.class).withConstructor(
        storage
    ).addMockedMethod("getSegmentFiles", String.class, String.class, File.class).createMock();

    expect(puller.getSegmentFiles(bucket, path, outDir)).andReturn(result);

    replayAll();

    puller.getSegmentFiles(dataSegment, outDir);

    verifyAll();
  }

  @Test
  public void prepareOutDirTest() throws IOException
  {
    GoogleStorage storage = createMock(GoogleStorage.class);
    File outDir = Files.createTempDirectory("druid").toFile();

    try {
      GoogleDataSegmentPuller puller = new GoogleDataSegmentPuller(storage);
      puller.prepareOutDir(outDir);

      assertTrue(outDir.exists());
    }
    finally {
      outDir.delete();
    }
  }
}
