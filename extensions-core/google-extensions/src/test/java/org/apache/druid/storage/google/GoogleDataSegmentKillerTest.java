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
import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.segment.loading.DataSegmentKiller;
import org.apache.druid.segment.loading.SegmentLoadingException;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.NoneShardSpec;
import org.easymock.EasyMock;
import org.easymock.EasyMockSupport;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class GoogleDataSegmentKillerTest extends EasyMockSupport
{
  private static final String BUCKET = "bucket";
  private static final String INDEX_PATH = "test/2015-04-12T00:00:00.000Z_2015-04-13T00:00:00.000Z/1/0/index.zip";
  private static final String DESCRIPTOR_PATH = DataSegmentKiller.descriptorPath(INDEX_PATH);

  private static final DataSegment DATA_SEGMENT = new DataSegment(
      "test",
      Intervals.of("2015-04-12/2015-04-13"),
      "1",
      ImmutableMap.of("bucket", BUCKET, "path", INDEX_PATH),
      null,
      null,
      NoneShardSpec.instance(),
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
    storage.delete(EasyMock.eq(BUCKET), EasyMock.eq(INDEX_PATH));
    EasyMock.expectLastCall();
    storage.delete(EasyMock.eq(BUCKET), EasyMock.eq(DESCRIPTOR_PATH));
    EasyMock.expectLastCall();

    replayAll();

    GoogleDataSegmentKiller killer = new GoogleDataSegmentKiller(storage);

    killer.kill(DATA_SEGMENT);

    verifyAll();
  }

  @Test(expected = SegmentLoadingException.class)
  public void killWithErrorTest() throws SegmentLoadingException, IOException
  {
    final GoogleJsonResponseException exception = GoogleJsonResponseExceptionFactoryTesting.newMock(
        JacksonFactory.getDefaultInstance(),
        300,
        "test"
    );
    storage.delete(EasyMock.eq(BUCKET), EasyMock.eq(INDEX_PATH));
    EasyMock.expectLastCall().andThrow(exception);

    replayAll();

    GoogleDataSegmentKiller killer = new GoogleDataSegmentKiller(storage);

    killer.kill(DATA_SEGMENT);

    verifyAll();
  }

  @Test
  public void killRetryWithErrorTest() throws SegmentLoadingException, IOException
  {
    final GoogleJsonResponseException exception = GoogleJsonResponseExceptionFactoryTesting.newMock(
        JacksonFactory.getDefaultInstance(),
        500,
        "test"
    );
    storage.delete(EasyMock.eq(BUCKET), EasyMock.eq(INDEX_PATH));
    EasyMock.expectLastCall().andThrow(exception).once().andVoid().once();
    storage.delete(EasyMock.eq(BUCKET), EasyMock.eq(DESCRIPTOR_PATH));
    EasyMock.expectLastCall().andThrow(exception).once().andVoid().once();

    replayAll();

    GoogleDataSegmentKiller killer = new GoogleDataSegmentKiller(storage);

    killer.kill(DATA_SEGMENT);

    verifyAll();
  }
}
