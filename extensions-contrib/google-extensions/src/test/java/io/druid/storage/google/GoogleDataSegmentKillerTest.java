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
import io.druid.segment.loading.SegmentLoadingException;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.NoneShardSpec;
import org.easymock.EasyMockSupport;
import org.joda.time.Interval;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.easymock.EasyMock.expectLastCall;

public class GoogleDataSegmentKillerTest extends EasyMockSupport
{
  private static final String bucket = "bucket";
  private static final String indexPath = "test/2015-04-12T00:00:00.000Z_2015-04-13T00:00:00.000Z/1/0/index.zip";

  private static final DataSegment dataSegment = new DataSegment(
      "test",
      new Interval("2015-04-12/2015-04-13"),
      "1",
      ImmutableMap.<String, Object>of("bucket", bucket, "path", indexPath),
      null,
      null,
      new NoneShardSpec(),
      0,
      1
  );

  private GoogleStorage storage;

  @Before
  public void before()
  {
    storage = createMock(GoogleStorage.class);
  }

  @Test
  public void killTest() throws SegmentLoadingException, IOException
  {
    final String descriptorPath = indexPath.substring(0, indexPath.lastIndexOf("/")) + "/descriptor.json";

    storage.delete(bucket, indexPath);
    expectLastCall();
    storage.delete(bucket, descriptorPath);
    expectLastCall();

    replayAll();

    GoogleDataSegmentKiller killer = new GoogleDataSegmentKiller(storage);

    killer.kill(dataSegment);

    verifyAll();
  }

  @Test(expected = SegmentLoadingException.class)
  public void killWithErrorTest() throws SegmentLoadingException, IOException
  {
    storage.delete(bucket, indexPath);
    expectLastCall().andThrow(new IOException(""));

    replayAll();

    GoogleDataSegmentKiller killer = new GoogleDataSegmentKiller(storage);

    killer.kill(dataSegment);

    verifyAll();
  }
}
