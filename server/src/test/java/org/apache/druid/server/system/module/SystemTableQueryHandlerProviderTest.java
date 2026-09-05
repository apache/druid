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

package org.apache.druid.server.system.module;

import com.google.common.collect.ImmutableSet;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.server.system.handler.SystemTableBrokerQueryHandler;
import org.apache.druid.server.system.handler.SystemTableQueryHandler;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class SystemTableQueryHandlerProviderTest
{
  @Test
  public void testReturnsBrokerHandlerForBroker()
  {
    final SystemTableBrokerQueryHandler brokerHandler = Mockito.mock(SystemTableBrokerQueryHandler.class);
    final SystemTableQueryHandler localHandler = Mockito.mock(SystemTableQueryHandler.class);
    final SystemTableQueryHandlerProvider provider = new SystemTableQueryHandlerProvider(
        ImmutableSet.of(NodeRole.BROKER),
        () -> brokerHandler,
        () -> localHandler
    );

    Assertions.assertSame(brokerHandler, provider.get());
  }

  @Test
  public void testReturnsLocalHandlerForNonBroker()
  {
    final SystemTableBrokerQueryHandler brokerHandler = Mockito.mock(SystemTableBrokerQueryHandler.class);
    final SystemTableQueryHandler localHandler = Mockito.mock(SystemTableQueryHandler.class);
    final SystemTableQueryHandlerProvider provider = new SystemTableQueryHandlerProvider(
        ImmutableSet.of(NodeRole.COORDINATOR),
        () -> brokerHandler,
        () -> localHandler
    );

    Assertions.assertSame(localHandler, provider.get());
  }
}
