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
import org.apache.druid.client.coordinator.CoordinatorClient;
import org.apache.druid.java.util.common.jackson.JacksonUtils;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.http.client.response.BytesFullResponseHolder;
import org.apache.druid.java.util.metrics.StubServiceEmitter;
import org.apache.druid.security.basic.BasicAuthCommonCacheConfig;
import org.apache.druid.security.basic.authorization.BasicRoleBasedAuthorizer;
import org.apache.druid.security.basic.authorization.entity.GroupMappingAndRoleMap;
import org.apache.druid.security.basic.authorization.entity.UserAndRoleMap;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.server.security.AuthorizerMapper;
import org.easymock.EasyMock;
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
  private Injector injector;
  private CoordinatorClient coordinatorClient;

  private CoordinatorPollingBasicAuthorizerCacheManager manager;

  @Before
  public void setup() throws IOException
  {
    EmittingLogger.registerEmitter(new StubServiceEmitter());

    final BasicRoleBasedAuthorizer authorizer = EasyMock.createStrictMock(BasicRoleBasedAuthorizer.class);
    injector = EasyMock.createStrictMock(Injector.class);
    EasyMock.expect(injector.getInstance(AuthorizerMapper.class))
            .andReturn(new AuthorizerMapper(Map.of("test-basic-auth", authorizer))).once();

    coordinatorClient = EasyMock.createStrictMock(CoordinatorClient.class);

    final int numRetries = 10;
    manager = new CoordinatorPollingBasicAuthorizerCacheManager(
        injector,
        new BasicAuthCommonCacheConfig(0L, 1L, temporaryFolder.newFolder().getAbsolutePath(), numRetries),
        MAPPER,
        coordinatorClient
    );
  }

  private void replayAll()
  {
    EasyMock.replay(injector, coordinatorClient);
  }

  private void verifyAll()
  {
    EasyMock.verify(injector, coordinatorClient);
  }

  @Test
  public void test_stop_interruptsPollingThread_whileFetchingUserRoleMap() throws InterruptedException
  {
    // Return the first set of requests immediately
    EasyMock.expect(coordinatorClient.getCachedSerializedUserMapSync("test-basic-auth"))
            .andReturn(JacksonUtils.toBytes(MAPPER, new UserAndRoleMap(Map.of(), Map.of())))
            .once();
    EasyMock.expect(coordinatorClient.getCachedSerializedGroupMappingMapSync("test-basic-auth"))
            .andReturn(JacksonUtils.toBytes(MAPPER, new GroupMappingAndRoleMap(Map.of(), Map.of())))
            .once();

    // Block the second user request so that it can be interrupted by stop()
    final AtomicBoolean isInterrupted = new AtomicBoolean(false);
    EasyMock.expect(coordinatorClient.getCachedSerializedUserMapSync("test-basic-auth"))
            .andAnswer(() -> {
              try {
                Thread.sleep(10_000);
                return JacksonUtils.toBytes(MAPPER, new UserAndRoleMap(Map.of(), Map.of()));
              }
              catch (InterruptedException e) {
                isInterrupted.set(true);
                throw e;
              }
            }).once();
    replayAll();

    // Start the manager and wait for a while to ensure that polling has started
    manager.start();
    Thread.sleep(10);

    // Stop the manager and verify that the polling thread has been interrupted
    manager.stop();
    Thread.sleep(10);

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
    EasyMock.expect(coordinatorClient.getCachedSerializedUserMapSync("test-basic-auth"))
            .andReturn(JacksonUtils.toBytes(MAPPER, new UserAndRoleMap(Map.of(), Map.of())))
            .once();
    EasyMock.expect(coordinatorClient.getCachedSerializedGroupMappingMapSync("test-basic-auth"))
            .andReturn(JacksonUtils.toBytes(MAPPER, new GroupMappingAndRoleMap(Map.of(), Map.of())))
            .once();

    // Return the second user request immediately
    EasyMock.expect(coordinatorClient.getCachedSerializedUserMapSync("test-basic-auth"))
            .andReturn(JacksonUtils.toBytes(MAPPER, new UserAndRoleMap(Map.of(), Map.of())))
            .once();

    // Block the second group request so that it can be interrupted by stop()
    final AtomicBoolean isInterrupted = new AtomicBoolean(false);
    EasyMock.expect(coordinatorClient.getCachedSerializedGroupMappingMapSync("test-basic-auth"))
            .andAnswer(() -> {
              try {
                Thread.sleep(10_000);
                return JacksonUtils.toBytes(MAPPER, new GroupMappingAndRoleMap(Map.of(), Map.of()));
              }
              catch (InterruptedException e) {
                isInterrupted.set(true);
                throw e;
              }
            }).once();

    replayAll();

    // Start the manager and wait for a while to ensure that polling has started
    manager.start();
    Thread.sleep(10);

    // Stop the manager and verify that the polling thread has been interrupted
    manager.stop();
    Thread.sleep(10);

    Assert.assertTrue(isInterrupted.get());

    verifyAll();
  }
}
