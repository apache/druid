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

package org.apache.druid.storage.azure;

import com.google.common.collect.ImmutableMap;
import com.microsoft.azure.storage.StorageException;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.segment.loading.SegmentLoadingException;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.NoneShardSpec;
import org.easymock.EasyMock;
import org.easymock.EasyMockSupport;
import org.junit.Before;
import org.junit.Test;

import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class AzureDataSegmentKillerTest extends EasyMockSupport
{
  private static final String CONTAINER_NAME = "container";
  private static final String BLOB_PATH = "test/2015-04-12T00:00:00.000Z_2015-04-13T00:00:00.000Z/1/0/index.zip";

  private static final DataSegment DATA_SEGMENT = new DataSegment(
      "test",
      Intervals.of("2015-04-12/2015-04-13"),
      "1",
      ImmutableMap.of("containerName", CONTAINER_NAME, "blobPath", BLOB_PATH),
      null,
      null,
      NoneShardSpec.instance(),
      0,
      1
  );

  private AzureStorage azureStorage;

  @Before
  public void before()
  {
    azureStorage = createMock(AzureStorage.class);
  }

  @Test
  public void killTest() throws SegmentLoadingException, URISyntaxException, StorageException
  {

    List<String> deletedFiles = new ArrayList<>();
    final String dirPath = Paths.get(BLOB_PATH).getParent().toString();

    EasyMock.expect(azureStorage.emptyCloudBlobDirectory(CONTAINER_NAME, dirPath)).andReturn(deletedFiles);

    replayAll();

    AzureDataSegmentKiller killer = new AzureDataSegmentKiller(azureStorage);

    killer.kill(DATA_SEGMENT);

    verifyAll();
  }

  @Test(expected = SegmentLoadingException.class)
  public void killWithErrorTest() throws SegmentLoadingException, URISyntaxException, StorageException
  {

    String dirPath = Paths.get(BLOB_PATH).getParent().toString();

    EasyMock.expect(azureStorage.emptyCloudBlobDirectory(CONTAINER_NAME, dirPath)).andThrow(
        new StorageException(
            "",
            "",
            400,
            null,
            null
        )
    );

    replayAll();

    AzureDataSegmentKiller killer = new AzureDataSegmentKiller(azureStorage);

    killer.kill(DATA_SEGMENT);

    verifyAll();
  }
}
