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
import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpResponseException;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.loading.DataSegmentKiller;
import org.apache.druid.segment.loading.SegmentLoadingException;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.NoneShardSpec;
import org.easymock.EasyMock;
import org.easymock.EasyMockSupport;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;

public class GoogleDataSegmentKillerTest extends EasyMockSupport
{
  private static final String KEY_1 = "key1";
  private static final String KEY_2 = "key2";
  private static final String BUCKET = "bucket";
  private static final String PREFIX = "test/log";
  private static final URI PREFIX_URI = URI.create(StringUtils.format("gs://%s/%s", BUCKET, PREFIX));
  private static final String INDEX_PATH = "test/2015-04-12T00:00:00.000Z_2015-04-13T00:00:00.000Z/1/0/index.zip";
  private static final String DESCRIPTOR_PATH = DataSegmentKiller.descriptorPath(INDEX_PATH);
  private static final long TIME_0 = 0L;
  private static final long TIME_1 = 1L;
  private static final int MAX_KEYS = 1;
  private static final Exception RECOVERABLE_EXCEPTION = new HttpResponseException.Builder(429, "recoverable", new HttpHeaders()).build();
  private static final Exception NON_RECOVERABLE_EXCEPTION = new HttpResponseException.Builder(404, "non recoverable", new HttpHeaders()).build();


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
  private GoogleAccountConfig accountConfig;
  private GoogleInputDataConfig inputDataConfig;

  @Before
  public void before()
  {
    accountConfig = createMock(GoogleAccountConfig.class);
    inputDataConfig = createMock(GoogleInputDataConfig.class);
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

    GoogleDataSegmentKiller killer = new GoogleDataSegmentKiller(storage, accountConfig, inputDataConfig);

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

    GoogleDataSegmentKiller killer = new GoogleDataSegmentKiller(storage, accountConfig, inputDataConfig);

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

    GoogleDataSegmentKiller killer = new GoogleDataSegmentKiller(storage, accountConfig, inputDataConfig);

    killer.kill(DATA_SEGMENT);

    verifyAll();
  }

  @Test
  public void test_killAll_accountConfigWithNullBucketAndPrefix_throwsISEException() throws IOException
  {

    EasyMock.expect(accountConfig.getBucket()).andReturn(null).atLeastOnce();
    EasyMock.expect(accountConfig.getPrefix()).andReturn(null).anyTimes();

    boolean thrownISEException = false;

    try {
      GoogleDataSegmentKiller killer = new GoogleDataSegmentKiller(storage, accountConfig, inputDataConfig);
      EasyMock.replay(storage, inputDataConfig, accountConfig);

      killer.killAll();
    }
    catch (ISE e) {
      thrownISEException = true;
    }

    Assert.assertTrue(thrownISEException);
    EasyMock.verify(accountConfig, inputDataConfig, storage);
  }

  @Test
  public void test_killAll_noException_deletesAllTaskLogs() throws IOException
  {
    GoogleStorageObjectMetadata object1 = GoogleTestUtils.newStorageObject(BUCKET, KEY_1, TIME_0);
    GoogleStorageObjectMetadata object2 = GoogleTestUtils.newStorageObject(BUCKET, KEY_2, TIME_1);

    GoogleTestUtils.expectListObjectsPageRequest(storage, PREFIX_URI, MAX_KEYS, ImmutableList.of(object1, object2));

    GoogleTestUtils.expectDeleteObjects(
        storage,
        ImmutableList.of(object1, object2),
        ImmutableMap.of()
    );
    EasyMock.expect(accountConfig.getBucket()).andReturn(BUCKET).anyTimes();
    EasyMock.expect(accountConfig.getPrefix()).andReturn(PREFIX).anyTimes();
    EasyMock.expect(inputDataConfig.getMaxListingLength()).andReturn(MAX_KEYS);

    EasyMock.replay(accountConfig, inputDataConfig, storage);

    GoogleDataSegmentKiller killer = new GoogleDataSegmentKiller(storage, accountConfig, inputDataConfig);

    killer.killAll();

    EasyMock.verify(accountConfig, inputDataConfig, storage);
  }


  @Test
  public void test_killAll_recoverableExceptionWhenDeletingObjects_deletesAllTaskLogs() throws IOException
  {
    GoogleStorageObjectMetadata object1 = GoogleTestUtils.newStorageObject(BUCKET, KEY_1, TIME_0);

    GoogleTestUtils.expectListObjectsPageRequest(storage, PREFIX_URI, MAX_KEYS, ImmutableList.of(object1));

    GoogleTestUtils.expectDeleteObjects(
        storage,
        ImmutableList.of(object1),
        ImmutableMap.of(object1, RECOVERABLE_EXCEPTION)
    );

    EasyMock.expect(accountConfig.getBucket()).andReturn(BUCKET).anyTimes();
    EasyMock.expect(accountConfig.getPrefix()).andReturn(PREFIX).anyTimes();
    EasyMock.expect(inputDataConfig.getMaxListingLength()).andReturn(MAX_KEYS);

    EasyMock.replay(accountConfig, inputDataConfig, storage);

    GoogleDataSegmentKiller killer = new GoogleDataSegmentKiller(storage, accountConfig, inputDataConfig);
    killer.killAll();

    EasyMock.verify(accountConfig, inputDataConfig, storage);
  }

  @Test
  public void test_killAll_nonrecoverableExceptionWhenListingObjects_doesntDeleteAnyTaskLogs()
  {
    boolean ioExceptionThrown = false;
    try {
      GoogleStorageObjectMetadata object1 = GoogleTestUtils.newStorageObject(BUCKET, KEY_1, TIME_0);

      GoogleTestUtils.expectListObjectsPageRequest(storage, PREFIX_URI, MAX_KEYS, ImmutableList.of(object1));

      GoogleTestUtils.expectDeleteObjects(
          storage,
          ImmutableList.of(),
          ImmutableMap.of(object1, NON_RECOVERABLE_EXCEPTION)
      );

      EasyMock.expect(accountConfig.getBucket()).andReturn(BUCKET).anyTimes();
      EasyMock.expect(accountConfig.getPrefix()).andReturn(PREFIX).anyTimes();
      EasyMock.expect(inputDataConfig.getMaxListingLength()).andReturn(MAX_KEYS);

      EasyMock.replay(accountConfig, inputDataConfig, storage);

      GoogleDataSegmentKiller killer = new GoogleDataSegmentKiller(storage, accountConfig, inputDataConfig);
      killer.killAll();
    }
    catch (IOException e) {
      ioExceptionThrown = true;
    }

    Assert.assertTrue(ioExceptionThrown);

    EasyMock.verify(accountConfig, inputDataConfig, storage);
  }
}
