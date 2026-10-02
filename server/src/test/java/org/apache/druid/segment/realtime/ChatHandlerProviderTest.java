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

package org.apache.druid.segment.realtime;

import org.apache.druid.java.util.common.ISE;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ChatHandlerProviderTest
{
  private static class TestChatHandler implements ChatHandler
  {
  }

  private static final String TEST_SERVICE_NAME = "test-service-name";

  private ChatHandlerProvider chatHandlerProvider;

  @BeforeEach
  public void setUp()
  {
    chatHandlerProvider = new ChatHandlerProvider();
  }

  @Test
  public void testRegisterAndGet()
  {
    ChatHandler testChatHandler = new TestChatHandler();

    Assertions.assertFalse(chatHandlerProvider.get(TEST_SERVICE_NAME).isPresent(), "bad initial state");

    chatHandlerProvider.register(TEST_SERVICE_NAME, testChatHandler);
    Assertions.assertTrue(chatHandlerProvider.get(TEST_SERVICE_NAME).isPresent(), "chatHandler did not register");
    Assertions.assertEquals(testChatHandler, chatHandlerProvider.get(TEST_SERVICE_NAME).get());

    chatHandlerProvider.unregister(TEST_SERVICE_NAME);
    Assertions.assertFalse(chatHandlerProvider.get(TEST_SERVICE_NAME).isPresent(), "chatHandler did not deregister");
  }

  @Test
  public void testDuplicateRegistrationThrows()
  {
    chatHandlerProvider.register(TEST_SERVICE_NAME, new TestChatHandler());
    Assertions.assertThrows(ISE.class, () -> chatHandlerProvider.register(TEST_SERVICE_NAME, new TestChatHandler()));
  }
}
