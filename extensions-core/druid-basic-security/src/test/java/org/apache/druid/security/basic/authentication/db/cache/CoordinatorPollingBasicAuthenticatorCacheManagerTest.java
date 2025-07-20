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
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.metrics.StubServiceEmitter;
import org.apache.druid.rpc.MockServiceClient;
import org.apache.druid.rpc.RequestBuilder;
import org.apache.druid.security.basic.BasicAuthCommonCacheConfig;
import org.apache.druid.security.basic.CoordinatorServiceClient;
import org.apache.druid.security.basic.authentication.BasicHTTPAuthenticator;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.server.security.AuthenticatorMapper;
import org.easymock.EasyMock;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class CoordinatorPollingBasicAuthenticatorCacheManagerTest
{
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void test_stop_interruptsPollingThread() throws InterruptedException, IOException
  {
    EmittingLogger.registerEmitter(new StubServiceEmitter());

    final BasicHTTPAuthenticator authenticator = EasyMock.createStrictMock(BasicHTTPAuthenticator.class);
    final Injector injector = EasyMock.createStrictMock(Injector.class);
    EasyMock.expect(injector.getInstance(AuthenticatorMapper.class))
            .andReturn(new AuthenticatorMapper(Map.of("test-basic-auth", authenticator))).once();

    // Create a mock leader client and request
    final CoordinatorServiceClient leaderClient = EasyMock.createStrictMock(CoordinatorServiceClient.class);
    final MockServiceClient serviceClient = new MockServiceClient();
    EasyMock.expect(leaderClient.getServiceClient()).andReturn(serviceClient).anyTimes();

    // Return the first request immediately
    final String path = StringUtils.format(
        "/druid-ext/basic-security/authentication/db/%s/cachedSerializedUserMap",
        "test-basic-auth"
    );
    serviceClient.expectAndRespond(
        new RequestBuilder(HttpMethod.GET, path),
        HttpResponseStatus.OK,
        Map.of(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON),
        new byte[0]
    );

    // Block the second request so that it can be interrupted by stop()
    final AtomicBoolean isInterrupted = new AtomicBoolean(false);

    serviceClient.expectAndRespond(
        new RequestBuilder(HttpMethod.GET, path),
        new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK) {
          @Override
          public ChannelBuffer getContent()
          {
            try {
              Thread.sleep(10_000);
              return null;
            }
            catch (InterruptedException e) {
              isInterrupted.set(true);
              throw new RuntimeException(e);
            }
          }
        }
    );

    EasyMock.replay(injector, leaderClient);

    final int numRetries = 10;
    final CoordinatorPollingBasicAuthenticatorCacheManager manager = new CoordinatorPollingBasicAuthenticatorCacheManager(
        injector,
        new BasicAuthCommonCacheConfig(0L, 1L, temporaryFolder.newFolder().getAbsolutePath(), numRetries),
        TestHelper.JSON_MAPPER,
        leaderClient
    );

    // Start the manager and wait for a while to ensure that polling has started
    manager.start();
    Thread.sleep(10);

    // Stop the manager and verify that the polling thread has been interrupted
    manager.stop();
    Thread.sleep(10);

    Assert.assertTrue(isInterrupted.get());

    EasyMock.verify(injector, leaderClient);
  }

}
