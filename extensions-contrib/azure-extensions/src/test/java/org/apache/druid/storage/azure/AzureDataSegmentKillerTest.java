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
import org.easymock.EasyMockSupport;
import org.junit.Before;
import org.junit.Test;

import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static org.easymock.EasyMock.expect;

public class AzureDataSegmentKillerTest extends EasyMockSupport
{

  private static final String containerName = "container";
  private static final String blobPath = "test/2015-04-12T00:00:00.000Z_2015-04-13T00:00:00.000Z/1/0/index.zip";

  private static final DataSegment dataSegment = new DataSegment(
      "test",
      Intervals.of("2015-04-12/2015-04-13"),
      "1",
      ImmutableMap.of("containerName", containerName, "blobPath", blobPath),
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
    final String dirPath = Paths.get(blobPath).getParent().toString();

    expect(azureStorage.emptyCloudBlobDirectory(containerName, dirPath)).andReturn(deletedFiles);

    replayAll();

    AzureDataSegmentKiller killer = new AzureDataSegmentKiller(azureStorage);

    killer.kill(dataSegment);

    verifyAll();
  }

  @Test(expected = SegmentLoadingException.class)
  public void killWithErrorTest() throws SegmentLoadingException, URISyntaxException, StorageException
  {

    String dirPath = Paths.get(blobPath).getParent().toString();

    expect(azureStorage.emptyCloudBlobDirectory(containerName, dirPath)).andThrow(
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

    killer.kill(dataSegment);

    verifyAll();
  }
}
