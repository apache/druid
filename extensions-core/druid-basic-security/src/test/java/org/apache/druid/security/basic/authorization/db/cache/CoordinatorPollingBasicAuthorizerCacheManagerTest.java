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

package org.apache.druid.security.basic.authorization.db.cache;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Injector;
import org.apache.druid.discovery.DruidLeaderClient;
import org.apache.druid.java.util.common.jackson.JacksonUtils;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.BytesFullResponseHandler;
import org.apache.druid.java.util.http.client.response.BytesFullResponseHolder;
import org.apache.druid.java.util.metrics.StubServiceEmitter;
import org.apache.druid.security.basic.BasicAuthCommonCacheConfig;
import org.apache.druid.security.basic.authorization.BasicRoleBasedAuthorizer;
import org.apache.druid.security.basic.authorization.entity.GroupMappingAndRoleMap;
import org.apache.druid.security.basic.authorization.entity.UserAndRoleMap;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.server.security.AuthorizerMapper;
import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class CoordinatorPollingBasicAuthorizerCacheManagerTest
{
  private static final ObjectMapper MAPPER = TestHelper.JSON_MAPPER;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  // Mocks
  private Request request;
  private Injector injector;
  private DruidLeaderClient leaderClient;

  private CoordinatorPollingBasicAuthorizerCacheManager manager;

  @Before
  public void setup() throws IOException
  {
    EmittingLogger.registerEmitter(new StubServiceEmitter());

    final BasicRoleBasedAuthorizer authorizer = EasyMock.createStrictMock(BasicRoleBasedAuthorizer.class);
    injector = EasyMock.createStrictMock(Injector.class);
    EasyMock.expect(injector.getInstance(AuthorizerMapper.class))
            .andReturn(new AuthorizerMapper(Map.of("test-basic-auth", authorizer))).once();

    request = EasyMock.createStrictMock(Request.class);
    leaderClient = EasyMock.createStrictMock(DruidLeaderClient.class);

    final int numRetries = 10;
    manager = new CoordinatorPollingBasicAuthorizerCacheManager(
        injector,
        new BasicAuthCommonCacheConfig(0L, 1L, temporaryFolder.newFolder().getAbsolutePath(), numRetries),
        MAPPER,
        leaderClient
    );
  }

  private void replayAll()
  {
    EasyMock.replay(injector, leaderClient);
  }

  private void verifyAll()
  {
    EasyMock.verify(injector, leaderClient);
  }

  @Test
  public void test_stop_interruptsPollingThread_whileFetchingUserRoleMap() throws InterruptedException
  {
    final HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
    final BytesFullResponseHolder userResponseHolder = new BytesFullResponseHolder(response);
    userResponseHolder.addChunk(JacksonUtils.toBytes(MAPPER, new UserAndRoleMap(Map.of(), Map.of())));

    final BytesFullResponseHolder groupResponseHolder = new BytesFullResponseHolder(response);
    groupResponseHolder.addChunk(JacksonUtils.toBytes(MAPPER, new GroupMappingAndRoleMap(Map.of(), Map.of())));

    // Return the first set of requests immediately
    expectHttpRequestAndAnswer(() -> userResponseHolder);
    expectHttpRequestAndAnswer(() -> groupResponseHolder);

    // Block the second user request so that it can be interrupted by stop()
    final AtomicBoolean isInterrupted = new AtomicBoolean(false);
    expectHttpRequestAndAnswer(() -> {
      try {
        Thread.sleep(10_000);
        return userResponseHolder;
      }
      catch (InterruptedException e) {
        isInterrupted.set(true);
        throw e;
      }
    });

    replayAll();

    // Start the manager and wait for a while to ensure that polling has started
    manager.start();
    Thread.sleep(10);

    // Stop the manager and verify that the polling thread has been interrupted
    manager.stop();
    Assert.assertTrue(isInterrupted.get());

    verifyAll();
  }

  @Test
  public void test_stop_interruptsPollingThread_whileFetchingGroupRoleMap() throws InterruptedException
  {
    final HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
    final BytesFullResponseHolder userResponseHolder = new BytesFullResponseHolder(response);
    userResponseHolder.addChunk(JacksonUtils.toBytes(MAPPER, new UserAndRoleMap(Map.of(), Map.of())));

    final BytesFullResponseHolder groupResponseHolder = new BytesFullResponseHolder(response);
    groupResponseHolder.addChunk(JacksonUtils.toBytes(MAPPER, new GroupMappingAndRoleMap(Map.of(), Map.of())));

    // Return the first set of requests immediately
    expectHttpRequestAndAnswer(() -> userResponseHolder);
    expectHttpRequestAndAnswer(() -> groupResponseHolder);

    // Return the second user request immediately
    expectHttpRequestAndAnswer(() -> userResponseHolder);

    // Block the second group request so that it can be interrupted by stop()
    final AtomicBoolean isInterrupted = new AtomicBoolean(false);
    expectHttpRequestAndAnswer(() -> {
      try {
        Thread.sleep(10_000);
        return groupResponseHolder;
      }
      catch (InterruptedException e) {
        isInterrupted.set(true);
        throw e;
      }
    });

    replayAll();

    // Start the manager and wait for a while to ensure that polling has started
    manager.start();
    Thread.sleep(10);

    // Stop the manager and verify that the polling thread has been interrupted
    manager.stop();
    Assert.assertTrue(isInterrupted.get());

    verifyAll();
  }

  private void expectHttpRequestAndAnswer(IAnswer<BytesFullResponseHolder> responseHolder)
  {
    try {
      EasyMock.expect(
          leaderClient.makeRequest(EasyMock.anyObject(), EasyMock.anyString())
      ).andReturn(request).once();
      EasyMock.expect(
          leaderClient.go(
              EasyMock.anyObject(),
              EasyMock.anyObject(BytesFullResponseHandler.class)
          )
      ).andAnswer(responseHolder).once();
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
