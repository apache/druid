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

package io.druid.segment.realtime.firehose;

import io.druid.curator.discovery.ServiceAnnouncer;
import io.druid.server.DruidNode;
import io.druid.server.initialization.ServerConfig;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.easymock.EasyMockRunner;
import org.easymock.EasyMockSupport;
import org.easymock.Mock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;

@RunWith(EasyMockRunner.class)
public class ServiceAnnouncingChatHandlerProviderTest extends EasyMockSupport
{
  private static class TestChatHandler implements ChatHandler {}

  private static final String TEST_SERVICE_NAME = "test-service-name";
  private static final String TEST_HOST = "test-host";
  private static final int TEST_PORT = 1234;

  private ServiceAnnouncingChatHandlerProvider chatHandlerProvider;

  @Mock
  private DruidNode node;

  @Mock
  private ServiceAnnouncer serviceAnnouncer;

  @Before
  public void setUp() throws Exception
  {
    chatHandlerProvider = new ServiceAnnouncingChatHandlerProvider(node, serviceAnnouncer, new ServerConfig());
  }

  @Test
  public void testRegistrationDefault() throws IOException
  {
    testRegistrationWithAnnounce(false);
  }

  @Test
  public void testRegistrationWithAnnounce() throws IOException
  {
    testRegistrationWithAnnounce(true);
  }

  @Test
  public void testRegistrationWithoutAnnounce() throws IOException
  {
    ChatHandler testChatHandler = new TestChatHandler();

    Assert.assertFalse("bad initial state", chatHandlerProvider.get(TEST_SERVICE_NAME).isPresent());

    chatHandlerProvider.register(TEST_SERVICE_NAME, testChatHandler, false);
    Assert.assertTrue("chatHandler did not register", chatHandlerProvider.get(TEST_SERVICE_NAME).isPresent());
    Assert.assertEquals(testChatHandler, chatHandlerProvider.get(TEST_SERVICE_NAME).get());

    chatHandlerProvider.unregister(TEST_SERVICE_NAME);
    Assert.assertFalse("chatHandler did not deregister", chatHandlerProvider.get(TEST_SERVICE_NAME).isPresent());
  }

  private void testRegistrationWithAnnounce(boolean useThreeArgConstructor) throws IOException
  {
    ChatHandler testChatHandler = new TestChatHandler();
    Capture<DruidNode> captured = Capture.newInstance();

    EasyMock.expect(node.getHost()).andReturn(TEST_HOST);
    EasyMock.expect(node.getPlaintextPort()).andReturn(TEST_PORT);
    EasyMock.expect(node.getTlsPort()).andReturn(-1);
    serviceAnnouncer.announce(EasyMock.capture(captured));
    replayAll();

    Assert.assertFalse("bad initial state", chatHandlerProvider.get(TEST_SERVICE_NAME).isPresent());

    if (useThreeArgConstructor) {
      chatHandlerProvider.register(TEST_SERVICE_NAME, testChatHandler, true);
    } else {
      chatHandlerProvider.register(TEST_SERVICE_NAME, testChatHandler);
    }
    verifyAll();

    DruidNode param = captured.getValues().get(0);
    Assert.assertEquals(TEST_SERVICE_NAME, param.getServiceName());
    Assert.assertEquals(TEST_HOST, param.getHost());
    Assert.assertEquals(TEST_PORT, param.getPlaintextPort());
    Assert.assertEquals(-1, param.getTlsPort());
    Assert.assertEquals(null, param.getHostAndTlsPort());
    Assert.assertTrue("chatHandler did not register", chatHandlerProvider.get(TEST_SERVICE_NAME).isPresent());
    Assert.assertEquals(testChatHandler, chatHandlerProvider.get(TEST_SERVICE_NAME).get());

    captured.reset();
    resetAll();
    EasyMock.expect(node.getHost()).andReturn(TEST_HOST);
    EasyMock.expect(node.getPlaintextPort()).andReturn(TEST_PORT);
    EasyMock.expect(node.getTlsPort()).andReturn(-1);
    serviceAnnouncer.unannounce(EasyMock.capture(captured));
    replayAll();

    chatHandlerProvider.unregister(TEST_SERVICE_NAME);
    verifyAll();

    param = captured.getValues().get(0);
    Assert.assertEquals(TEST_SERVICE_NAME, param.getServiceName());
    Assert.assertEquals(TEST_HOST, param.getHost());
    Assert.assertEquals(TEST_PORT, param.getPlaintextPort());
    Assert.assertEquals(-1, param.getTlsPort());
    Assert.assertEquals(null, param.getHostAndTlsPort());
    Assert.assertFalse("chatHandler did not deregister", chatHandlerProvider.get(TEST_SERVICE_NAME).isPresent());
  }
}
