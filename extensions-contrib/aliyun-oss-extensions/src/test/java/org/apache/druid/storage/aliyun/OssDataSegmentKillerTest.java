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

package org.apache.druid.storage.aliyun;

import com.aliyun.oss.ClientException;
import com.aliyun.oss.OSS;
import com.aliyun.oss.model.DeleteObjectsRequest;
import com.aliyun.oss.model.OSSObjectSummary;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.easymock.EasyMock;
import org.easymock.EasyMockRunner;
import org.easymock.EasyMockSupport;
import org.easymock.Mock;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.net.URI;
import java.util.Collections;

@RunWith(EasyMockRunner.class)
public class OssDataSegmentKillerTest extends EasyMockSupport
{
  private static final String KEY_1 = "key1";
  private static final String KEY_2 = "key2";
  private static final String TEST_BUCKET = "test_bucket";
  private static final String TEST_PREFIX = "test_prefix";
  private static final URI PREFIX_URI = URI.create(StringUtils.format(OssStorageDruidModule.SCHEME + "://%s/%s", TEST_BUCKET, TEST_PREFIX));
  private static final long TIME_0 = 0L;
  private static final long TIME_1 = 1L;
  private static final int MAX_KEYS = 1;
  private static final Exception RECOVERABLE_EXCEPTION = new ClientException(new IOException("mocked by test case"));
  private static final Exception NON_RECOVERABLE_EXCEPTION = new ClientException(new NullPointerException("mocked by test case"));

  @Mock
  private OSS client;
  @Mock
  private OssStorageConfig segmentPusherConfig;
  @Mock
  private OssInputDataConfig inputDataConfig;

  private OssDataSegmentKiller segmentKiller;

  @Test
  public void test_killAll_accountConfigWithNullBucketAndBaseKey_throwsISEException() throws IOException
  {
    EasyMock.expect(segmentPusherConfig.getBucket()).andReturn(null);
    EasyMock.expectLastCall().atLeastOnce();
    EasyMock.expect(segmentPusherConfig.getPrefix()).andReturn(null);
    EasyMock.expectLastCall().anyTimes();

    boolean thrownISEException = false;

    try {

      EasyMock.replay(client, segmentPusherConfig, inputDataConfig);

      segmentKiller = new OssDataSegmentKiller(client, segmentPusherConfig, inputDataConfig);
      segmentKiller.killAll();
    }
    catch (ISE e) {
      thrownISEException = true;
    }
    Assert.assertTrue(thrownISEException);
    EasyMock.verify(client, segmentPusherConfig, inputDataConfig);
  }

  @Test
  public void test_killAll_noException_deletesAllSegments() throws IOException
  {
    OSSObjectSummary objectSummary1 = OssTestUtils.newOSSObjectSummary(TEST_BUCKET, KEY_1, TIME_0);
    OSSObjectSummary objectSummary2 = OssTestUtils.newOSSObjectSummary(TEST_BUCKET, KEY_2, TIME_1);

    OssTestUtils.expectListObjects(
        client,
        PREFIX_URI,
        ImmutableList.of(objectSummary1, objectSummary2)
    );

    DeleteObjectsRequest deleteRequest1 = new DeleteObjectsRequest(TEST_BUCKET);
    deleteRequest1.setKeys(Collections.singletonList(KEY_1));
    DeleteObjectsRequest deleteRequest2 = new DeleteObjectsRequest(TEST_BUCKET);
    deleteRequest2.setKeys(Collections.singletonList(KEY_2));

    OssTestUtils.mockClientDeleteObjects(
        client,
        ImmutableList.of(deleteRequest1, deleteRequest2),
        ImmutableMap.of()
    );

    EasyMock.expect(segmentPusherConfig.getBucket()).andReturn(TEST_BUCKET);
    EasyMock.expectLastCall().anyTimes();
    EasyMock.expect(segmentPusherConfig.getPrefix()).andReturn(TEST_PREFIX);
    EasyMock.expectLastCall().anyTimes();

    EasyMock.expect(inputDataConfig.getMaxListingLength()).andReturn(MAX_KEYS);
    EasyMock.expectLastCall().anyTimes();

    EasyMock.replay(client, segmentPusherConfig, inputDataConfig);

    segmentKiller = new OssDataSegmentKiller(client, segmentPusherConfig, inputDataConfig);
    segmentKiller.killAll();
    EasyMock.verify(client, segmentPusherConfig, inputDataConfig);
  }

  @Test
  public void test_killAll_recoverableExceptionWhenListingObjects_deletesAllSegments() throws IOException
  {
    OSSObjectSummary objectSummary1 = OssTestUtils.newOSSObjectSummary(TEST_BUCKET, KEY_1, TIME_0);

    OssTestUtils.expectListObjects(
        client,
        PREFIX_URI,
        ImmutableList.of(objectSummary1)
    );

    DeleteObjectsRequest deleteRequest1 = new DeleteObjectsRequest(TEST_BUCKET);
    deleteRequest1.setKeys(Collections.singletonList(KEY_1));

    OssTestUtils.mockClientDeleteObjects(
        client,
        ImmutableList.of(deleteRequest1),
        ImmutableMap.of(deleteRequest1, RECOVERABLE_EXCEPTION)
    );

    EasyMock.expect(segmentPusherConfig.getBucket()).andReturn(TEST_BUCKET);
    EasyMock.expectLastCall().anyTimes();
    EasyMock.expect(segmentPusherConfig.getPrefix()).andReturn(TEST_PREFIX);
    EasyMock.expectLastCall().anyTimes();

    EasyMock.expect(inputDataConfig.getMaxListingLength()).andReturn(MAX_KEYS);
    EasyMock.expectLastCall().anyTimes();

    EasyMock.replay(client, segmentPusherConfig, inputDataConfig);

    segmentKiller = new OssDataSegmentKiller(client, segmentPusherConfig, inputDataConfig);
    segmentKiller.killAll();
    EasyMock.verify(client, segmentPusherConfig, inputDataConfig);
  }

  @Test
  public void test_killAll_nonrecoverableExceptionWhenListingObjects_deletesAllSegments()
  {
    boolean ioExceptionThrown = false;
    try {
      OSSObjectSummary objectSummary1 = OssTestUtils.newOSSObjectSummary(TEST_BUCKET, KEY_1, TIME_0);

      OssTestUtils.expectListObjects(
          client,
          PREFIX_URI,
          ImmutableList.of(objectSummary1)
      );

      DeleteObjectsRequest deleteRequest1 = new DeleteObjectsRequest(TEST_BUCKET);
      deleteRequest1.withKeys(ImmutableList.of(KEY_1));

      OssTestUtils.mockClientDeleteObjects(
          client,
          ImmutableList.of(),
          ImmutableMap.of(deleteRequest1, NON_RECOVERABLE_EXCEPTION)
      );


      EasyMock.expect(segmentPusherConfig.getBucket()).andReturn(TEST_BUCKET);
      EasyMock.expectLastCall().anyTimes();
      EasyMock.expect(segmentPusherConfig.getPrefix()).andReturn(TEST_PREFIX);
      EasyMock.expectLastCall().anyTimes();

      EasyMock.expect(inputDataConfig.getMaxListingLength()).andReturn(MAX_KEYS);
      EasyMock.expectLastCall().anyTimes();

      EasyMock.replay(client, segmentPusherConfig, inputDataConfig);

      segmentKiller = new OssDataSegmentKiller(client, segmentPusherConfig, inputDataConfig);
      segmentKiller.killAll();
    }
    catch (IOException e) {
      ioExceptionThrown = true;
    }

    Assert.assertTrue(ioExceptionThrown);
    EasyMock.verify(client, segmentPusherConfig, inputDataConfig);
  }
}
