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

import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.googleapis.testing.json.GoogleJsonResponseExceptionFactoryTesting;
import com.google.api.client.json.jackson2.JacksonFactory;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.loading.SegmentLoadingException;
import org.easymock.EasyMock;
import org.easymock.EasyMockSupport;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

public class GoogleDataSegmentPullerTest extends EasyMockSupport
{
  private static final String BUCKET = "bucket";
  private static final String PATH = "/path/to/storage/index.zip";

  @Test(expected = SegmentLoadingException.class)
  public void testDeleteOutputDirectoryWhenErrorIsRaisedPullingSegmentFiles()
      throws IOException, SegmentLoadingException
  {
    final File outDir = FileUtils.createTempDir();
    try {
      GoogleStorage storage = createMock(GoogleStorage.class);
      final GoogleJsonResponseException exception = GoogleJsonResponseExceptionFactoryTesting.newMock(
          JacksonFactory.getDefaultInstance(),
          300,
          "test"
      );
      EasyMock.expect(storage.getInputStream(EasyMock.eq(BUCKET), EasyMock.eq(PATH))).andThrow(exception);

      replayAll();

      GoogleDataSegmentPuller puller = new GoogleDataSegmentPuller(storage);
      puller.getSegmentFiles(BUCKET, PATH, outDir);

      Assert.assertFalse(outDir.exists());

      verifyAll();
    }
    finally {
      FileUtils.deleteDirectory(outDir);
    }
  }

  @Test
  public void testGetVersionBucketNameWithUnderscores() throws IOException
  {
    String bucket = "bucket_test";
    String prefix = "prefix/";
    String version = "0";

    GoogleStorage storage = createMock(GoogleStorage.class);
    EasyMock.expect(storage.version(EasyMock.eq(bucket), EasyMock.eq(prefix))).andReturn("0");
    EasyMock.replay(storage);

    GoogleDataSegmentPuller puller = new GoogleDataSegmentPuller(storage);

    String actual = puller.getVersion(URI.create(StringUtils.format("gs://%s/%s", bucket, prefix)));
    Assert.assertEquals(version, actual);
    EasyMock.verify(storage);
  }

  @Test
  public void testGetInputStreamBucketNameWithUnderscores() throws IOException
  {
    String bucket = "bucket_test";
    String prefix = "prefix/";

    GoogleStorage storage = createMock(GoogleStorage.class);
    EasyMock.expect(storage.getInputStream(EasyMock.eq(bucket), EasyMock.eq(prefix))).andReturn(EasyMock.createMock(InputStream.class));
    EasyMock.replay(storage);

    GoogleDataSegmentPuller puller = new GoogleDataSegmentPuller(storage);

    puller.getInputStream(URI.create(StringUtils.format("gs://%s/%s", bucket, prefix)));
    EasyMock.verify(storage);
  }
}
