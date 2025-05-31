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

package org.apache.druid.security.basic.authentication.db.cache;

import com.google.inject.Injector;
import org.apache.druid.discovery.DruidLeaderClient;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.BytesFullResponseHandler;
import org.apache.druid.java.util.http.client.response.BytesFullResponseHolder;
import org.apache.druid.java.util.metrics.StubServiceEmitter;
import org.apache.druid.security.basic.BasicAuthCommonCacheConfig;
import org.apache.druid.security.basic.authentication.BasicHTTPAuthenticator;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.server.security.AuthenticatorMapper;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class CoordinatorPollingBasicAuthenticatorCacheManagerTest
{
  @Test
  public void test_stop_interruptsPollingThread() throws InterruptedException, IOException
  {
    EmittingLogger.registerEmitter(new StubServiceEmitter());

    final BasicHTTPAuthenticator authenticator = EasyMock.createStrictMock(BasicHTTPAuthenticator.class);
    final Injector injector = EasyMock.createStrictMock(Injector.class);
    EasyMock.expect(injector.getInstance(AuthenticatorMapper.class))
            .andReturn(new AuthenticatorMapper(Map.of("test-basic-auth", authenticator))).once();

    // Create a mock leader client and request
    final DruidLeaderClient leaderClient = EasyMock.createStrictMock(DruidLeaderClient.class);
    final Request request = EasyMock.createStrictMock(Request.class);

    // Return the first request immediately
    EasyMock.expect(leaderClient.makeRequest(EasyMock.anyObject(), EasyMock.anyString()))
            .andReturn(request).once();
    EasyMock.expect(
        leaderClient.go(EasyMock.anyObject(), EasyMock.anyObject(BytesFullResponseHandler.class))
    ).andReturn(
        new BytesFullResponseHolder(null)
    ).once();

    // Block the second request so that it can be interrupted by stop()
    final AtomicBoolean isInterrupted = new AtomicBoolean(false);

    EasyMock.expect(leaderClient.makeRequest(EasyMock.anyObject(), EasyMock.anyString()))
            .andReturn(request).once();
    EasyMock.expect(
        leaderClient.go(EasyMock.anyObject(), EasyMock.anyObject(BytesFullResponseHandler.class))
    ).andAnswer(() -> {
      try {
        Thread.sleep(10_000);
        return null;
      }
      catch (InterruptedException e) {
        isInterrupted.set(true);
        throw e;
      }
    }).once();

    EasyMock.replay(injector, leaderClient);

    final int numRetries = 10;
    final CoordinatorPollingBasicAuthenticatorCacheManager manager = new CoordinatorPollingBasicAuthenticatorCacheManager(
        injector,
        new BasicAuthCommonCacheConfig(0L, 1L, "test-dir", numRetries),
        TestHelper.JSON_MAPPER,
        leaderClient
    );

    // Start the manager and wait for a while to ensure that polling has started
    manager.start();
    Thread.sleep(10);

    // Stop the manager and verify that the polling thread has been interrupted
    manager.stop();
    Assert.assertTrue(isInterrupted.get());

    EasyMock.verify(injector, leaderClient);
  }

}