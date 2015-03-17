/*
 * Druid - a distributed column store.
 *  Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package io.druid.storage.azure;

import com.google.common.collect.ImmutableMap;
import com.microsoft.azure.storage.StorageException;
import io.druid.segment.loading.SegmentLoadingException;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.NoneShardSpec;
import org.easymock.EasyMockSupport;
import org.joda.time.Interval;
import org.junit.Before;
import org.junit.Test;

import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import static org.easymock.EasyMock.*;

public class AzureDataSegmentKillerTest extends EasyMockSupport
{

  private static final DataSegment dataSegment = new DataSegment(
      "test",
      new Interval("2015-04-12/2015-04-13"),
      "1",
      ImmutableMap.<String, Object>of("storageDir", "/path/to/storage/"),
      null,
      null,
      new NoneShardSpec(),
      0,
      1
  );

  private AzureStorageContainer azureStorageContainer;

  @Before
  public void before()
  {
    azureStorageContainer = createMock(AzureStorageContainer.class);
  }

  @Test
  public void killTest() throws SegmentLoadingException, URISyntaxException, StorageException
  {

    List<String> deletedFiles = new ArrayList<>();

    expect(azureStorageContainer.emptyCloudBlobDirectory("/path/to/storage/")).andReturn(deletedFiles);

    replayAll();

    AzureDataSegmentKiller killer = new AzureDataSegmentKiller(azureStorageContainer);

    killer.kill(dataSegment);

    verifyAll();
  }

  @Test(expected = SegmentLoadingException.class)
  public void killWithErrorTest() throws SegmentLoadingException, URISyntaxException, StorageException
  {

    expect(azureStorageContainer.emptyCloudBlobDirectory("/path/to/storage/")).andThrow(
        new StorageException(
            "",
            "",
            400,
            null,
            null
        )
    );

    replayAll();

    AzureDataSegmentKiller killer = new AzureDataSegmentKiller(azureStorageContainer);

    killer.kill(dataSegment);

    verifyAll();
  }
}
