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
import org.apache.commons.io.FileUtils;
import org.apache.druid.segment.loading.SegmentLoadingException;
import org.easymock.EasyMock;
import org.easymock.EasyMockSupport;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertFalse;

public class GoogleDataSegmentPullerTest extends EasyMockSupport
{
  private static final String bucket = "bucket";
  private static final String path = "/path/to/storage/index.zip";

  @Test(expected = SegmentLoadingException.class)
  public void testDeleteOutputDirectoryWhenErrorIsRaisedPullingSegmentFiles()
      throws IOException, SegmentLoadingException
  {
    final File outDir = Files.createTempDirectory("druid").toFile();
    try {
      GoogleStorage storage = createMock(GoogleStorage.class);
      final GoogleJsonResponseException exception = GoogleJsonResponseExceptionFactoryTesting.newMock(
          JacksonFactory.getDefaultInstance(),
          300,
          "test"
      );
      expect(storage.get(EasyMock.eq(bucket), EasyMock.eq(path))).andThrow(exception);

      replayAll();

      GoogleDataSegmentPuller puller = new GoogleDataSegmentPuller(storage);
      puller.getSegmentFiles(bucket, path, outDir);

      assertFalse(outDir.exists());

      verifyAll();
    }
    finally {
      FileUtils.deleteDirectory(outDir);
    }
  }
}
