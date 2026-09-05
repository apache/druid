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

package org.apache.druid.server.system.handler;

import org.apache.druid.query.Druids;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.SystemTableDataSource;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.scan.ScanResultValue;
import org.apache.druid.server.security.AuthenticationResult;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class SystemTableBrokerQueryHandlerTest
{
  private static final AuthenticationResult AUTHENTICATION_RESULT =
      new AuthenticationResult("user", "authorizer", "authenticator", null);

  /** {@code X-Druid-Native-Query-Route: local} selects the Broker's local system-table handler. */
  @Test
  public void testLocalRouteUsesLocalHandler()
  {
    final SystemTableQueryClient queryClient = Mockito.mock(SystemTableQueryClient.class);
    final SystemTableQueryHandler localHandler = Mockito.mock(SystemTableQueryHandler.class);
    final SystemTableBrokerQueryHandler brokerHandler = new SystemTableBrokerQueryHandler(queryClient, localHandler);
    final ScanQuery query = query();
    final QueryRunner<ScanResultValue> expectedRunner = Mockito.mock(QueryRunner.class);
    Mockito.when(localHandler.createRunner(query, AUTHENTICATION_RESULT, true)).thenReturn(expectedRunner);

    Assertions.assertSame(expectedRunner, brokerHandler.createRunner(query, AUTHENTICATION_RESULT, true));
    Mockito.verifyNoInteractions(queryClient);
  }

  /** A request without the local route uses distributed Broker fanout. */
  @Test
  public void testDefaultRouteUsesQueryClient()
  {
    final SystemTableQueryClient queryClient = Mockito.mock(SystemTableQueryClient.class);
    final SystemTableQueryHandler localHandler = Mockito.mock(SystemTableQueryHandler.class);
    final SystemTableBrokerQueryHandler brokerHandler = new SystemTableBrokerQueryHandler(queryClient, localHandler);
    final ScanQuery query = query();
    final QueryRunner<ScanResultValue> expectedRunner = Mockito.mock(QueryRunner.class);
    Mockito.when(queryClient.createRunner(query, AUTHENTICATION_RESULT, false)).thenReturn(expectedRunner);

    Assertions.assertSame(expectedRunner, brokerHandler.createRunner(query, AUTHENTICATION_RESULT, false));
    Mockito.verifyNoInteractions(localHandler);
  }

  private static ScanQuery query()
  {
    return Druids.newScanQueryBuilder()
                 .dataSource(new SystemTableDataSource("server_properties"))
                 .eternityInterval()
                 .build();
  }
}
