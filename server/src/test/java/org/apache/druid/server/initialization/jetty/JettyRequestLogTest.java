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

package org.apache.druid.server.initialization.jetty;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.easymock.EasyMock;
import org.eclipse.jetty.http.HttpURI;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Response;
import org.junit.Test;

import javax.ws.rs.HttpMethod;

public class JettyRequestLogTest
{
  private static final String COORDINATOR = "/coordinator";
  private static final String OVERLORD = "/indexer";
  private static final String WORKER = "/worker";
  private static final String HTTP_PROTOCOL = "HTTP";
  private static final String REMOTE_ADDRESS = "druid.apache.org";
  private static final String INFO = "INFO";
  private static final String ERROR = "ERROR";
  private static final String DEBUG = "DEBUG";
  private static final String REQUEST_LOG = "org.apache.druid.jetty.RequestLog";

  @Test
  public void testRequestLogWithGetDebugDisabled()
  {
    Request req = EasyMock.createMock(Request.class);
    Response resp = EasyMock.createMock(Response.class);
    HttpURI foo = new HttpURI(COORDINATOR);
    EasyMock.expect(req.getHttpURI()).andReturn(foo).anyTimes();
    EasyMock.expect(req.getRemoteAddr()).andReturn(REMOTE_ADDRESS).anyTimes();
    EasyMock.expect(req.getMethod()).andReturn(HttpMethod.GET).anyTimes();
    EasyMock.replay(req, resp);
    Configurator.setLevel(REQUEST_LOG,
        Level.getLevel(ERROR));
    JettyRequestLog logger = new JettyRequestLog();
    logger.log(req, resp);
    EasyMock.verify(req);
  }

  @Test
  public void testRequestLogWithGetDebugEnabled()
  {
    Request req = EasyMock.createMock(Request.class);
    Response resp = EasyMock.createMock(Response.class);
    HttpURI foo = new HttpURI(COORDINATOR);
    EasyMock.expect(req.getHttpURI()).andReturn(foo).anyTimes();
    EasyMock.expect(req.getRemoteAddr()).andReturn(REMOTE_ADDRESS).anyTimes();
    EasyMock.expect(req.getProtocol()).andReturn(HTTP_PROTOCOL).anyTimes();
    EasyMock.expect(req.getMethod()).andReturn(HttpMethod.GET).anyTimes();
    EasyMock.replay(req, resp);
    Configurator.setLevel(REQUEST_LOG,
        Level.getLevel(DEBUG));
    JettyRequestLog logger = new JettyRequestLog();
    logger.log(req, resp);
    EasyMock.verify(req);
  }

  @Test
  public void testRequestLogWithPostDebugEnabledCoordinator()
  {
    Request req = EasyMock.createMock(Request.class);
    Response resp = EasyMock.createMock(Response.class);
    HttpURI foo = new HttpURI(COORDINATOR);
    EasyMock.expect(req.getHttpURI()).andReturn(foo).anyTimes();
    EasyMock.expect(req.getRemoteAddr()).andReturn(REMOTE_ADDRESS).anyTimes();
    EasyMock.expect(req.getProtocol()).andReturn(HTTP_PROTOCOL).anyTimes();
    EasyMock.expect(req.getMethod()).andReturn(HttpMethod.POST).anyTimes();
    EasyMock.replay(req, resp);
    Configurator.setLevel(REQUEST_LOG,
        Level.getLevel(DEBUG));
    JettyRequestLog logger = new JettyRequestLog();
    logger.log(req, resp);
    EasyMock.verify(req);
  }

  @Test
  public void testRequestLogWithPostDebugEnabledOverlord()
  {
    Request req = EasyMock.createMock(Request.class);
    Response resp = EasyMock.createMock(Response.class);
    HttpURI foo = new HttpURI(OVERLORD);
    EasyMock.expect(req.getHttpURI()).andReturn(foo).anyTimes();
    EasyMock.expect(req.getRemoteAddr()).andReturn(REMOTE_ADDRESS).anyTimes();
    EasyMock.expect(req.getProtocol()).andReturn(HTTP_PROTOCOL).anyTimes();
    EasyMock.expect(req.getMethod()).andReturn(HttpMethod.POST).anyTimes();
    EasyMock.replay(req, resp);
    Configurator.setLevel(REQUEST_LOG,
        Level.getLevel(DEBUG));
    JettyRequestLog logger = new JettyRequestLog();
    logger.log(req, resp);
    EasyMock.verify(req);
  }


  @Test
  public void testRequestLogWithPostDebugEnabledNonCoordinatorOverlord()
  {
    Request req = EasyMock.createMock(Request.class);
    Response resp = EasyMock.createMock(Response.class);
    HttpURI foo = new HttpURI(WORKER);
    EasyMock.expect(req.getHttpURI()).andReturn(foo).anyTimes();
    EasyMock.expect(req.getRemoteAddr()).andReturn(REMOTE_ADDRESS).anyTimes();
    EasyMock.expect(req.getProtocol()).andReturn(HTTP_PROTOCOL).anyTimes();
    EasyMock.expect(req.getMethod()).andReturn(HttpMethod.POST).anyTimes();
    EasyMock.replay(req, resp);
    Configurator.setLevel(REQUEST_LOG,
        Level.getLevel(DEBUG));
    JettyRequestLog logger = new JettyRequestLog();
    logger.log(req, resp);
    EasyMock.verify(req);
  }

  @Test
  public void testRequestLogWithPostDebugDisabledNonCoordinatorOverlord()
  {
    Request req = EasyMock.createMock(Request.class);
    Response resp = EasyMock.createMock(Response.class);
    HttpURI foo = new HttpURI(WORKER);
    EasyMock.expect(req.getHttpURI()).andReturn(foo).anyTimes();
    EasyMock.expect(req.getRemoteAddr()).andReturn(REMOTE_ADDRESS).anyTimes();
    EasyMock.expect(req.getProtocol()).andReturn(HTTP_PROTOCOL).anyTimes();
    EasyMock.expect(req.getMethod()).andReturn(HttpMethod.POST).anyTimes();
    EasyMock.replay(req, resp);
    Configurator.setLevel(REQUEST_LOG,
        Level.getLevel(INFO));
    JettyRequestLog logger = new JettyRequestLog();
    logger.log(req, resp);
    EasyMock.verify(req);
  }

  @Test
  public void testRequestLogWithPostDebugDisabledCoordinator()
  {
    Request req = EasyMock.createMock(Request.class);
    Response resp = EasyMock.createMock(Response.class);
    HttpURI foo = new HttpURI(COORDINATOR);
    EasyMock.expect(req.getHttpURI()).andReturn(foo).anyTimes();
    EasyMock.expect(req.getRemoteAddr()).andReturn(REMOTE_ADDRESS).anyTimes();
    EasyMock.expect(req.getProtocol()).andReturn(HTTP_PROTOCOL).anyTimes();
    EasyMock.expect(req.getMethod()).andReturn(HttpMethod.POST).anyTimes();
    EasyMock.replay(req, resp);
    Configurator.setLevel(REQUEST_LOG,
        Level.getLevel(INFO));
    JettyRequestLog logger = new JettyRequestLog();
    logger.log(req, resp);
    EasyMock.verify(req);
  }

  @Test
  public void testRequestLogWithPostDebugDisabledOverlord()
  {
    Request req = EasyMock.createMock(Request.class);
    Response resp = EasyMock.createMock(Response.class);
    HttpURI foo = new HttpURI(OVERLORD);
    EasyMock.expect(req.getHttpURI()).andReturn(foo).anyTimes();
    EasyMock.expect(req.getRemoteAddr()).andReturn(REMOTE_ADDRESS).anyTimes();
    EasyMock.expect(req.getProtocol()).andReturn(HTTP_PROTOCOL).anyTimes();
    EasyMock.expect(req.getMethod()).andReturn(HttpMethod.POST).anyTimes();
    EasyMock.replay(req, resp);
    Configurator.setLevel(REQUEST_LOG,
        Level.getLevel(INFO));
    JettyRequestLog logger = new JettyRequestLog();
    logger.log(req, resp);
    EasyMock.verify(req);
  }

  @Test
  public void testRequestLogWithDeleteDebugEnabledCoordinator()
  {
    Request req = EasyMock.createMock(Request.class);
    Response resp = EasyMock.createMock(Response.class);
    HttpURI foo = new HttpURI(COORDINATOR);
    EasyMock.expect(req.getHttpURI()).andReturn(foo).anyTimes();
    EasyMock.expect(req.getRemoteAddr()).andReturn(REMOTE_ADDRESS).anyTimes();
    EasyMock.expect(req.getProtocol()).andReturn(HTTP_PROTOCOL).anyTimes();
    EasyMock.expect(req.getMethod()).andReturn(HttpMethod.DELETE).anyTimes();
    EasyMock.replay(req, resp);
    Configurator.setLevel(REQUEST_LOG,
        Level.getLevel(DEBUG));
    JettyRequestLog logger = new JettyRequestLog();
    logger.log(req, resp);
    EasyMock.verify(req);
  }

  @Test
  public void testRequestLogWithDeleteDebugEnabledOverlord()
  {
    Request req = EasyMock.createMock(Request.class);
    Response resp = EasyMock.createMock(Response.class);
    HttpURI foo = new HttpURI(OVERLORD);
    EasyMock.expect(req.getHttpURI()).andReturn(foo).anyTimes();
    EasyMock.expect(req.getRemoteAddr()).andReturn(REMOTE_ADDRESS).anyTimes();
    EasyMock.expect(req.getProtocol()).andReturn(HTTP_PROTOCOL).anyTimes();
    EasyMock.expect(req.getMethod()).andReturn(HttpMethod.DELETE).anyTimes();
    EasyMock.replay(req, resp);
    Configurator.setLevel(REQUEST_LOG,
        Level.getLevel(DEBUG));
    JettyRequestLog logger = new JettyRequestLog();
    logger.log(req, resp);
    EasyMock.verify(req);
  }

  @Test
  public void testRequestLogWithDeleteDebugEnabledNonCoordinatorOverlord()
  {
    Request req = EasyMock.createMock(Request.class);
    Response resp = EasyMock.createMock(Response.class);
    HttpURI foo = new HttpURI(WORKER);
    EasyMock.expect(req.getHttpURI()).andReturn(foo).anyTimes();
    EasyMock.expect(req.getRemoteAddr()).andReturn(REMOTE_ADDRESS).anyTimes();
    EasyMock.expect(req.getProtocol()).andReturn(HTTP_PROTOCOL).anyTimes();
    EasyMock.expect(req.getMethod()).andReturn(HttpMethod.DELETE).anyTimes();
    EasyMock.replay(req, resp);
    Configurator.setLevel(REQUEST_LOG,
        Level.getLevel(DEBUG));
    JettyRequestLog logger = new JettyRequestLog();
    logger.log(req, resp);
    EasyMock.verify(req);
  }

  @Test
  public void testRequestLogWithDeleteDebugDisabledNonCoordinatorOverlord()
  {
    Request req = EasyMock.createMock(Request.class);
    Response resp = EasyMock.createMock(Response.class);
    HttpURI foo = new HttpURI(WORKER);
    EasyMock.expect(req.getHttpURI()).andReturn(foo).anyTimes();
    EasyMock.expect(req.getRemoteAddr()).andReturn(REMOTE_ADDRESS).anyTimes();
    EasyMock.expect(req.getProtocol()).andReturn(HTTP_PROTOCOL).anyTimes();
    EasyMock.expect(req.getMethod()).andReturn(HttpMethod.DELETE).anyTimes();
    EasyMock.replay(req, resp);
    Configurator.setLevel(REQUEST_LOG,
        Level.getLevel(INFO));
    JettyRequestLog logger = new JettyRequestLog();
    logger.log(req, resp);
    EasyMock.verify(req);
  }

  @Test
  public void testRequestLogWithDeleteDebugDisabledCoordinator()
  {
    Request req = EasyMock.createMock(Request.class);
    Response resp = EasyMock.createMock(Response.class);
    HttpURI foo = new HttpURI(COORDINATOR);
    EasyMock.expect(req.getHttpURI()).andReturn(foo).anyTimes();
    EasyMock.expect(req.getRemoteAddr()).andReturn(REMOTE_ADDRESS).anyTimes();
    EasyMock.expect(req.getProtocol()).andReturn(HTTP_PROTOCOL).anyTimes();
    EasyMock.expect(req.getMethod()).andReturn(HttpMethod.DELETE).anyTimes();
    EasyMock.replay(req, resp);
    Configurator.setLevel(REQUEST_LOG,
        Level.getLevel(INFO));
    JettyRequestLog logger = new JettyRequestLog();
    logger.log(req, resp);
    EasyMock.verify(req);
  }

  @Test
  public void testRequestLogWithDeleteDebugDisabledOverlord()
  {
    Request req = EasyMock.createMock(Request.class);
    Response resp = EasyMock.createMock(Response.class);
    HttpURI foo = new HttpURI(OVERLORD);
    EasyMock.expect(req.getHttpURI()).andReturn(foo).anyTimes();
    EasyMock.expect(req.getRemoteAddr()).andReturn(REMOTE_ADDRESS).anyTimes();
    EasyMock.expect(req.getProtocol()).andReturn(HTTP_PROTOCOL).anyTimes();
    EasyMock.expect(req.getMethod()).andReturn(HttpMethod.DELETE).anyTimes();
    EasyMock.replay(req, resp);
    Configurator.setLevel(REQUEST_LOG,
        Level.getLevel(INFO));
    JettyRequestLog logger = new JettyRequestLog();
    logger.log(req, resp);
    EasyMock.verify(req);
  }

  @Test
  public void testRequestLogWithPutDebugEnabledCoordinator()
  {
    Request req = EasyMock.createMock(Request.class);
    Response resp = EasyMock.createMock(Response.class);
    HttpURI foo = new HttpURI(COORDINATOR);
    EasyMock.expect(req.getHttpURI()).andReturn(foo).anyTimes();
    EasyMock.expect(req.getRemoteAddr()).andReturn(REMOTE_ADDRESS).anyTimes();
    EasyMock.expect(req.getProtocol()).andReturn(HTTP_PROTOCOL).anyTimes();
    EasyMock.expect(req.getMethod()).andReturn(HttpMethod.PUT).anyTimes();
    EasyMock.replay(req, resp);
    Configurator.setLevel(REQUEST_LOG,
        Level.getLevel(DEBUG));
    JettyRequestLog logger = new JettyRequestLog();
    logger.log(req, resp);
    EasyMock.verify(req);
  }

  @Test
  public void testRequestLogWithPutDebugEnabledOverlord()
  {
    Request req = EasyMock.createMock(Request.class);
    Response resp = EasyMock.createMock(Response.class);
    HttpURI foo = new HttpURI(OVERLORD);
    EasyMock.expect(req.getHttpURI()).andReturn(foo).anyTimes();
    EasyMock.expect(req.getRemoteAddr()).andReturn(REMOTE_ADDRESS).anyTimes();
    EasyMock.expect(req.getProtocol()).andReturn(HTTP_PROTOCOL).anyTimes();
    EasyMock.expect(req.getMethod()).andReturn(HttpMethod.PUT).anyTimes();
    EasyMock.replay(req, resp);
    Configurator.setLevel(REQUEST_LOG,
        Level.getLevel(DEBUG));
    JettyRequestLog logger = new JettyRequestLog();
    logger.log(req, resp);
    EasyMock.verify(req);
  }

  @Test
  public void testRequestLogWithPutDebugEnabledNonCoordinatorOverlord()
  {
    Request req = EasyMock.createMock(Request.class);
    Response resp = EasyMock.createMock(Response.class);
    HttpURI foo = new HttpURI(WORKER);
    EasyMock.expect(req.getHttpURI()).andReturn(foo).anyTimes();
    EasyMock.expect(req.getRemoteAddr()).andReturn(REMOTE_ADDRESS).anyTimes();
    EasyMock.expect(req.getProtocol()).andReturn(HTTP_PROTOCOL).anyTimes();
    EasyMock.expect(req.getMethod()).andReturn(HttpMethod.PUT).anyTimes();
    EasyMock.replay(req, resp);
    Configurator.setLevel(REQUEST_LOG,
        Level.getLevel(DEBUG));
    JettyRequestLog logger = new JettyRequestLog();
    logger.log(req, resp);
    EasyMock.verify(req);
  }

  @Test
  public void testRequestLogWithPutDebugDisabledNonCoordinatorOverlord()
  {
    Request req = EasyMock.createMock(Request.class);
    Response resp = EasyMock.createMock(Response.class);
    HttpURI foo = new HttpURI(WORKER);
    EasyMock.expect(req.getHttpURI()).andReturn(foo).anyTimes();
    EasyMock.expect(req.getRemoteAddr()).andReturn(REMOTE_ADDRESS).anyTimes();
    EasyMock.expect(req.getProtocol()).andReturn(HTTP_PROTOCOL).anyTimes();
    EasyMock.expect(req.getMethod()).andReturn(HttpMethod.PUT).anyTimes();
    EasyMock.replay(req, resp);
    Configurator.setLevel(REQUEST_LOG,
        Level.getLevel(INFO));
    JettyRequestLog logger = new JettyRequestLog();
    logger.log(req, resp);
    EasyMock.verify(req);
  }

  @Test
  public void testRequestLogWithPutDebugDisabledCoordinator()
  {
    Request req = EasyMock.createMock(Request.class);
    Response resp = EasyMock.createMock(Response.class);
    HttpURI foo = new HttpURI(COORDINATOR);
    EasyMock.expect(req.getHttpURI()).andReturn(foo).anyTimes();
    EasyMock.expect(req.getRemoteAddr()).andReturn(REMOTE_ADDRESS).anyTimes();
    EasyMock.expect(req.getProtocol()).andReturn(HTTP_PROTOCOL).anyTimes();
    EasyMock.expect(req.getMethod()).andReturn(HttpMethod.PUT).anyTimes();
    EasyMock.replay(req, resp);
    Configurator.setLevel(REQUEST_LOG,
        Level.getLevel(INFO));
    JettyRequestLog logger = new JettyRequestLog();
    logger.log(req, resp);
    EasyMock.verify(req);
  }

  @Test
  public void testRequestLogWithPutDebugDisabledOverlord()
  {
    Request req = EasyMock.createMock(Request.class);
    Response resp = EasyMock.createMock(Response.class);
    HttpURI foo = new HttpURI(OVERLORD);
    EasyMock.expect(req.getHttpURI()).andReturn(foo).anyTimes();
    EasyMock.expect(req.getRemoteAddr()).andReturn(REMOTE_ADDRESS).anyTimes();
    EasyMock.expect(req.getProtocol()).andReturn(HTTP_PROTOCOL).anyTimes();
    EasyMock.expect(req.getMethod()).andReturn(HttpMethod.PUT).anyTimes();
    EasyMock.replay(req, resp);
    Configurator.setLevel(REQUEST_LOG,
        Level.getLevel(INFO));
    JettyRequestLog logger = new JettyRequestLog();
    logger.log(req, resp);
    EasyMock.verify(req);
  }
}
