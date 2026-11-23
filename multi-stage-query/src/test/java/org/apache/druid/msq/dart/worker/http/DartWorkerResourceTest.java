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

package org.apache.druid.msq.dart.worker.http;

import org.apache.druid.error.DruidException;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.messages.server.Outbox;
import org.apache.druid.msq.dart.DartResourcePermissionMapper;
import org.apache.druid.msq.dart.controller.messages.ControllerMessage;
import org.apache.druid.msq.dart.worker.DartWorkerRunner;
import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.ForbiddenException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import javax.servlet.http.HttpServletRequest;
import java.util.Map;
import java.util.Set;

public class DartWorkerResourceTest
{
  private static final String AUTHORIZER_NAME = "authz";
  private static final String ADMIN = "admin";
  private static final String CONTROLLER_HOST = "broker:8082";

  private DartWorkerRunner workerRunner;
  private DartWorkerResource resource;

  @BeforeEach
  public void setUp()
  {
    workerRunner = Mockito.mock(DartWorkerRunner.class);

    @SuppressWarnings("unchecked")
    final Outbox<ControllerMessage> outbox = Mockito.mock(Outbox.class);

    resource = new DartWorkerResource(
        workerRunner,
        new DartResourcePermissionMapper(),
        new DefaultObjectMapper(),
        outbox,
        new AuthorizerMapper(
            Map.of(
                AUTHORIZER_NAME,
                (authenticationResult, resource, action) ->
                    ADMIN.equals(authenticationResult.getIdentity()) ? Access.OK : Access.DENIED
            )
        )
    );
  }

  @Test
  public void test_httpPostSync()
  {
    Mockito.when(workerRunner.syncController(CONTROLLER_HOST, new SyncWorkersRequest(Set.of("q1"))))
           .thenReturn(new SyncWorkersResponse(Set.of("q1", "q2")));

    final SyncWorkersResponse response = resource.httpPostSync(
        new SyncWorkersRequest(Set.of("q1")),
        makeRequest(ADMIN, CONTROLLER_HOST)
    );

    Assertions.assertEquals(new SyncWorkersResponse(Set.of("q1", "q2")), response);
  }

  @Test
  public void test_httpPostSync_nullBody()
  {
    final DruidException e = Assertions.assertThrows(
        DruidException.class,
        () -> resource.httpPostSync(null, makeRequest(ADMIN, CONTROLLER_HOST))
    );
    Assertions.assertEquals(DruidException.Category.INVALID_INPUT, e.getCategory());
    Mockito.verifyNoInteractions(workerRunner);
  }

  @Test
  public void test_httpPostSync_missingControllerHost()
  {
    Assertions.assertThrows(
        DruidException.class,
        () -> resource.httpPostSync(new SyncWorkersRequest(Set.of()), makeRequest(ADMIN, null))
    );
    Mockito.verifyNoInteractions(workerRunner);
  }

  @Test
  public void test_httpPostSync_forbidden()
  {
    Assertions.assertThrows(
        ForbiddenException.class,
        () -> resource.httpPostSync(new SyncWorkersRequest(Set.of()), makeRequest("someone", CONTROLLER_HOST))
    );
    Mockito.verifyNoInteractions(workerRunner);
  }

  private static HttpServletRequest makeRequest(final String identity, final String controllerHost)
  {
    final HttpServletRequest req = Mockito.mock(HttpServletRequest.class);
    Mockito.when(req.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT))
           .thenReturn(new AuthenticationResult(identity, AUTHORIZER_NAME, null, null));
    Mockito.when(req.getHeader(DartWorkerResource.HEADER_CONTROLLER_HOST)).thenReturn(controllerHost);
    return req;
  }
}
