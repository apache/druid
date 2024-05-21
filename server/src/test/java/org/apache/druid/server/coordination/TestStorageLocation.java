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
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.loading.StorageLocationConfig;
import org.apache.druid.timeline.DataSegment;
import org.junit.Assert;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

public class TestStorageLocation
{
  private static final Logger log = new Logger(TestStorageLocation.class);
  private final File cacheDir;
  private final File infoDir;
  private final ObjectMapper jsonMapper;

  public TestStorageLocation(TemporaryFolder temporaryFolder) throws IOException
  {
    cacheDir = temporaryFolder.newFolder();
    infoDir = temporaryFolder.newFolder();
    log.info("Creating tmp test files in [%s]", infoDir);
    jsonMapper = TestHelper.makeJsonMapper();
  }

  public File getInfoDir()
  {
    return infoDir;
  }

  public File getCacheDir()
  {
    return cacheDir;
  }

  public void writeSegmentInfoToCache(final DataSegment segment)
  {
    if (!infoDir.exists()) {
      infoDir.mkdir();
    }

    File segmentInfoCacheFile = new File(infoDir, segment.getId().toString());
    try {
      jsonMapper.writeValue(segmentInfoCacheFile, segment);
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }

    Assert.assertTrue(segmentInfoCacheFile.exists());
  }

  public void deleteSegmentInfoFromCache(final DataSegment segment)
  {
    File segmentInfoCacheFile = new File(infoDir, segment.getId().toString());
    if (segmentInfoCacheFile.exists()) {
      segmentInfoCacheFile.delete();
    }

    Assert.assertFalse(segmentInfoCacheFile.exists());
  }

  public void checkInfoCache(Set<DataSegment> expectedSegments)
  {
    Assert.assertTrue(infoDir.exists());
    File[] files = infoDir.listFiles();

    Set<DataSegment> segmentsInFiles = Arrays
        .stream(files)
        .map(file -> {
          try {
            return jsonMapper.readValue(file, DataSegment.class);
          }
          catch (IOException e) {
            throw new RuntimeException(e);
          }
        })
        .collect(Collectors.toSet());
    Assert.assertEquals(expectedSegments, segmentsInFiles);
  }

  public StorageLocationConfig toStorageLocationConfig() throws IOException
  {
    FileUtils.mkdirp(cacheDir);
    return new StorageLocationConfig(cacheDir, 100L, 100d);
  }

  public StorageLocationConfig toStorageLocationConfig(long maxSize, Double freeSpacePercent) throws IOException
  {
    FileUtils.mkdirp(cacheDir);
    return new StorageLocationConfig(cacheDir, maxSize, freeSpacePercent);
  }
}
