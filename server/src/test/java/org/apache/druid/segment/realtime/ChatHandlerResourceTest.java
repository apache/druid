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

import com.google.common.base.Optional;
import org.apache.druid.java.util.metrics.TaskHolder;
import org.apache.druid.server.initialization.jetty.ServiceUnavailableException;
import org.easymock.EasyMock;
import org.easymock.EasyMockRunner;
import org.easymock.EasyMockSupport;
import org.easymock.Mock;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(EasyMockRunner.class)
public class ChatHandlerResourceTest extends EasyMockSupport
{

  @Mock
  ChatHandlerProvider handlers;
  @Mock
  TaskHolder taskHolder;
  ChatHandlerResource chatHandlerResource;

  @Test
  public void test_noHandlerFound()
  {
    String handlerId = "handlerId";
    EasyMock.expect(taskHolder.getTaskId()).andReturn(null);
    EasyMock.expect(handlers.get(handlerId)).andReturn(Optional.absent());

    replayAll();
    chatHandlerResource = new ChatHandlerResource(handlers, taskHolder);
    Assert.assertThrows(ServiceUnavailableException.class, () -> chatHandlerResource.doTaskChat(handlerId, null));
    verifyAll();
  }
}
