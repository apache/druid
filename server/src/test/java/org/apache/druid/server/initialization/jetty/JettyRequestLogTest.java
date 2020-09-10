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

import java.security.Principal;

public class JettyRequestLogTest
{
  private static final String COORDINATOR = "/coordinator";
  private static final String OVERLORD = "/indexer";
  private static final String WORKER = "/worker";
  private static final String HTTP_PROTOCOL = "HTTP";
  private static final String REMOTE_USER = "remote_user";
  private static final String CERT_USER = "cert_user";
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
    EasyMock.expect(req.getMethod()).andReturn(HttpMethod.GET).anyTimes();
    Principal p = EasyMock.createMock(Principal.class);
    EasyMock.expect(p.getName()).andReturn(CERT_USER).anyTimes();
    EasyMock.expect(req.getUserPrincipal()).andReturn(p).anyTimes();
    EasyMock.replay(p, req, resp);
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
    Principal p = EasyMock.createMock(Principal.class);
    EasyMock.expect(p.getName()).andReturn(CERT_USER).anyTimes();
    EasyMock.expect(req.getUserPrincipal()).andReturn(p).anyTimes();
    EasyMock.expect(req.getMethod()).andReturn(HttpMethod.GET).anyTimes();
    EasyMock.replay(p, req, resp);
    Configurator.setLevel(REQUEST_LOG,
        Level.getLevel(DEBUG));
    JettyRequestLog logger = new JettyRequestLog();
    logger.log(req, resp);
    EasyMock.verify(req);
  }

  @Test
  public void testRequestLogWithPostDebugEnabled()
  {
    Request req = EasyMock.createMock(Request.class);
    Response resp = EasyMock.createMock(Response.class);
    HttpURI foo = new HttpURI(COORDINATOR);
    EasyMock.expect(req.getHttpURI()).andReturn(foo).anyTimes();
    EasyMock.expect(req.getRemoteAddr()).andReturn(REMOTE_ADDRESS).anyTimes();
    EasyMock.expect(req.getProtocol()).andReturn(HTTP_PROTOCOL).anyTimes();
    Principal p = EasyMock.createMock(Principal.class);
    EasyMock.expect(p.getName()).andReturn(CERT_USER).anyTimes();
    EasyMock.expect(req.getUserPrincipal()).andReturn(p).anyTimes();
    EasyMock.expect(req.getMethod()).andReturn(HttpMethod.POST).anyTimes();
    EasyMock.replay(p, req, resp);
    Configurator.setLevel(REQUEST_LOG,
        Level.getLevel(DEBUG));
    JettyRequestLog logger = new JettyRequestLog();
    logger.log(req, resp);
    EasyMock.verify(req);
  }

  @Test
  public void testRequestLogWithPostDebugDisabledNonNullRemoteUserNonCoordinator()
  {
    Request req = EasyMock.createMock(Request.class);
    Response resp = EasyMock.createMock(Response.class);
    HttpURI foo = new HttpURI(WORKER);
    EasyMock.expect(req.getHttpURI()).andReturn(foo).anyTimes();
    Principal p = EasyMock.createMock(Principal.class);
    EasyMock.expect(p.getName()).andReturn(CERT_USER).anyTimes();
    EasyMock.expect(req.getUserPrincipal()).andReturn(p).anyTimes();
    EasyMock.expect(req.getRemoteAddr()).andReturn(REMOTE_ADDRESS).anyTimes();
    EasyMock.expect(req.getRemoteUser()).andReturn(REMOTE_USER).anyTimes();
    EasyMock.expect(req.getProtocol()).andReturn(HTTP_PROTOCOL).anyTimes();
    EasyMock.expect(req.getMethod()).andReturn(HttpMethod.POST).anyTimes();
    EasyMock.replay(p, req, resp);
    Configurator.setLevel(REQUEST_LOG,
        Level.getLevel(INFO));
    JettyRequestLog logger = new JettyRequestLog();
    logger.log(req, resp);
    EasyMock.verify(req);
  }

  @Test
  public void testRequestLogWithPostDebugDisabledNonNullRemoteUserCoordinator()
  {
    Request req = EasyMock.createMock(Request.class);
    Response resp = EasyMock.createMock(Response.class);
    HttpURI foo = new HttpURI(COORDINATOR);
    EasyMock.expect(req.getHttpURI()).andReturn(foo).anyTimes();
    Principal p = EasyMock.createMock(Principal.class);
    EasyMock.expect(p.getName()).andReturn(CERT_USER).anyTimes();
    EasyMock.expect(req.getUserPrincipal()).andReturn(p).anyTimes();
    EasyMock.expect(req.getRemoteAddr()).andReturn(REMOTE_ADDRESS).anyTimes();
    EasyMock.expect(req.getRemoteUser()).andReturn(REMOTE_USER).anyTimes();
    EasyMock.expect(req.getProtocol()).andReturn(HTTP_PROTOCOL).anyTimes();
    EasyMock.expect(req.getMethod()).andReturn(HttpMethod.POST).anyTimes();
    EasyMock.replay(p, req, resp);
    Configurator.setLevel(REQUEST_LOG,
        Level.getLevel(INFO));
    JettyRequestLog logger = new JettyRequestLog();
    logger.log(req, resp);
    EasyMock.verify(req);
  }

  @Test
  public void testRequestLogWithPostDebugDisabledNullRemoteUserNonNullCertCoordinator()
  {
    Request req = EasyMock.createMock(Request.class);
    Response resp = EasyMock.createMock(Response.class);
    HttpURI foo = new HttpURI(COORDINATOR);
    EasyMock.expect(req.getHttpURI()).andReturn(foo).anyTimes();
    Principal p = EasyMock.createMock(Principal.class);
    EasyMock.expect(p.getName()).andReturn(CERT_USER).anyTimes();
    EasyMock.expect(req.getUserPrincipal()).andReturn(p).anyTimes();
    EasyMock.expect(req.getRemoteAddr()).andReturn(REMOTE_ADDRESS).anyTimes();
    EasyMock.expect(req.getRemoteUser()).andReturn(null).anyTimes();
    EasyMock.expect(req.getProtocol()).andReturn(HTTP_PROTOCOL).anyTimes();
    EasyMock.expect(req.getMethod()).andReturn(HttpMethod.POST).anyTimes();
    EasyMock.replay(p, req, resp);
    Configurator.setLevel(REQUEST_LOG,
        Level.getLevel(INFO));
    JettyRequestLog logger = new JettyRequestLog();
    logger.log(req, resp);
    EasyMock.verify(req);
  }

  @Test
  public void testRequestLogWithPostDebugDisabledNullRemoteUserNullCertCoordinator()
  {
    Request req = EasyMock.createMock(Request.class);
    Response resp = EasyMock.createMock(Response.class);
    HttpURI foo = new HttpURI(COORDINATOR);
    EasyMock.expect(req.getHttpURI()).andReturn(foo).anyTimes();
    Principal p = EasyMock.createMock(Principal.class);
    EasyMock.expect(req.getUserPrincipal()).andReturn(null).anyTimes();
    EasyMock.expect(p.getName()).andReturn(null).anyTimes();
    EasyMock.expect(req.getRemoteAddr()).andReturn(REMOTE_ADDRESS).anyTimes();
    EasyMock.expect(req.getRemoteUser()).andReturn(null).anyTimes();
    EasyMock.expect(req.getProtocol()).andReturn(HTTP_PROTOCOL).anyTimes();
    EasyMock.expect(req.getMethod()).andReturn(HttpMethod.POST).anyTimes();
    EasyMock.replay(p, req, resp);
    Configurator.setLevel(REQUEST_LOG,
        Level.getLevel(INFO));
    JettyRequestLog logger = new JettyRequestLog();
    logger.log(req, resp);
    EasyMock.verify(req);
  }

  @Test
  public void testRequestLogWithPostDebugDisabledNonNullRemoteUserNonOverlord()
  {
    Request req = EasyMock.createMock(Request.class);
    Response resp = EasyMock.createMock(Response.class);
    HttpURI foo = new HttpURI(WORKER);
    EasyMock.expect(req.getHttpURI()).andReturn(foo).anyTimes();
    Principal p = EasyMock.createMock(Principal.class);
    EasyMock.expect(p.getName()).andReturn(CERT_USER).anyTimes();
    EasyMock.expect(req.getUserPrincipal()).andReturn(p).anyTimes();
    EasyMock.expect(req.getRemoteAddr()).andReturn(REMOTE_ADDRESS).anyTimes();
    EasyMock.expect(req.getRemoteUser()).andReturn(REMOTE_USER).anyTimes();
    EasyMock.expect(req.getProtocol()).andReturn(HTTP_PROTOCOL).anyTimes();
    EasyMock.expect(req.getMethod()).andReturn(HttpMethod.POST).anyTimes();
    EasyMock.replay(p, req, resp);
    Configurator.setLevel(REQUEST_LOG,
        Level.getLevel(INFO));
    JettyRequestLog logger = new JettyRequestLog();
    logger.log(req, resp);
    EasyMock.verify(req);
  }

  @Test
  public void testRequestLogWithPostDebugDisabledNonNullRemoteUserOverlord()
  {
    Request req = EasyMock.createMock(Request.class);
    Response resp = EasyMock.createMock(Response.class);
    HttpURI foo = new HttpURI(OVERLORD);
    EasyMock.expect(req.getHttpURI()).andReturn(foo).anyTimes();
    Principal p = EasyMock.createMock(Principal.class);
    EasyMock.expect(p.getName()).andReturn(CERT_USER).anyTimes();
    EasyMock.expect(req.getUserPrincipal()).andReturn(p).anyTimes();
    EasyMock.expect(req.getRemoteAddr()).andReturn(REMOTE_ADDRESS).anyTimes();
    EasyMock.expect(req.getRemoteUser()).andReturn(REMOTE_USER).anyTimes();
    EasyMock.expect(req.getProtocol()).andReturn(HTTP_PROTOCOL).anyTimes();
    EasyMock.expect(req.getMethod()).andReturn(HttpMethod.POST).anyTimes();
    EasyMock.replay(p, req, resp);
    Configurator.setLevel(REQUEST_LOG,
        Level.getLevel(INFO));
    JettyRequestLog logger = new JettyRequestLog();
    logger.log(req, resp);
    EasyMock.verify(req);
  }

  @Test
  public void testRequestLogWithPostDebugDisabledNullRemoteUserNonNullCertOverlord()
  {
    Request req = EasyMock.createMock(Request.class);
    Response resp = EasyMock.createMock(Response.class);
    HttpURI foo = new HttpURI(OVERLORD);
    EasyMock.expect(req.getHttpURI()).andReturn(foo).anyTimes();
    Principal p = EasyMock.createMock(Principal.class);
    EasyMock.expect(p.getName()).andReturn(CERT_USER).anyTimes();
    EasyMock.expect(req.getUserPrincipal()).andReturn(p).anyTimes();
    EasyMock.expect(req.getRemoteAddr()).andReturn(REMOTE_ADDRESS).anyTimes();
    EasyMock.expect(req.getRemoteUser()).andReturn(null).anyTimes();
    EasyMock.expect(req.getProtocol()).andReturn(HTTP_PROTOCOL).anyTimes();
    EasyMock.expect(req.getMethod()).andReturn(HttpMethod.POST).anyTimes();
    EasyMock.replay(p, req, resp);
    Configurator.setLevel(REQUEST_LOG,
        Level.getLevel(INFO));
    JettyRequestLog logger = new JettyRequestLog();
    logger.log(req, resp);
    EasyMock.verify(req);
  }

  @Test
  public void testRequestLogWithPostDebugDisabledNullRemoteUserNullCertOverlord()
  {
    Request req = EasyMock.createMock(Request.class);
    Response resp = EasyMock.createMock(Response.class);
    HttpURI foo = new HttpURI(OVERLORD);
    EasyMock.expect(req.getHttpURI()).andReturn(foo).anyTimes();
    Principal p = EasyMock.createMock(Principal.class);
    EasyMock.expect(req.getUserPrincipal()).andReturn(null).anyTimes();
    EasyMock.expect(p.getName()).andReturn(null).anyTimes();
    EasyMock.expect(req.getRemoteAddr()).andReturn(REMOTE_ADDRESS).anyTimes();
    EasyMock.expect(req.getRemoteUser()).andReturn(null).anyTimes();
    EasyMock.expect(req.getProtocol()).andReturn(HTTP_PROTOCOL).anyTimes();
    EasyMock.expect(req.getMethod()).andReturn(HttpMethod.POST).anyTimes();
    EasyMock.replay(p, req, resp);
    Configurator.setLevel(REQUEST_LOG,
        Level.getLevel(INFO));
    JettyRequestLog logger = new JettyRequestLog();
    logger.log(req, resp);
    EasyMock.verify(req);
  }

  @Test
  public void testRequestLogWithDeleteDebugDisabledNonNullRemoteUserNonCoordinator()
  {
    Request req = EasyMock.createMock(Request.class);
    Response resp = EasyMock.createMock(Response.class);
    HttpURI foo = new HttpURI(WORKER);
    EasyMock.expect(req.getHttpURI()).andReturn(foo).anyTimes();
    Principal p = EasyMock.createMock(Principal.class);
    EasyMock.expect(p.getName()).andReturn(CERT_USER).anyTimes();
    EasyMock.expect(req.getUserPrincipal()).andReturn(p).anyTimes();
    EasyMock.expect(req.getRemoteAddr()).andReturn(REMOTE_ADDRESS).anyTimes();
    EasyMock.expect(req.getRemoteUser()).andReturn(REMOTE_USER).anyTimes();
    EasyMock.expect(req.getProtocol()).andReturn(HTTP_PROTOCOL).anyTimes();
    EasyMock.expect(req.getMethod()).andReturn(HttpMethod.DELETE).anyTimes();
    EasyMock.replay(p, req, resp);
    Configurator.setLevel(REQUEST_LOG,
        Level.getLevel(INFO));
    JettyRequestLog logger = new JettyRequestLog();
    logger.log(req, resp);
    EasyMock.verify(req);
  }

  @Test
  public void testRequestLogWithDeleteDebugDisabledNonNullRemoteUserCoordinator()
  {
    Request req = EasyMock.createMock(Request.class);
    Response resp = EasyMock.createMock(Response.class);
    HttpURI foo = new HttpURI(COORDINATOR);
    EasyMock.expect(req.getHttpURI()).andReturn(foo).anyTimes();
    Principal p = EasyMock.createMock(Principal.class);
    EasyMock.expect(p.getName()).andReturn(CERT_USER).anyTimes();
    EasyMock.expect(req.getUserPrincipal()).andReturn(p).anyTimes();
    EasyMock.expect(req.getRemoteAddr()).andReturn(REMOTE_ADDRESS).anyTimes();
    EasyMock.expect(req.getRemoteUser()).andReturn(REMOTE_USER).anyTimes();
    EasyMock.expect(req.getProtocol()).andReturn(HTTP_PROTOCOL).anyTimes();
    EasyMock.expect(req.getMethod()).andReturn(HttpMethod.DELETE).anyTimes();
    EasyMock.replay(p, req, resp);
    Configurator.setLevel(REQUEST_LOG,
        Level.getLevel(INFO));
    JettyRequestLog logger = new JettyRequestLog();
    logger.log(req, resp);
    EasyMock.verify(req);
  }

  @Test
  public void testRequestLogWithDeleteDebugDisabledNullRemoteUserNonNullCertCoordinator()
  {
    Request req = EasyMock.createMock(Request.class);
    Response resp = EasyMock.createMock(Response.class);
    HttpURI foo = new HttpURI(COORDINATOR);
    EasyMock.expect(req.getHttpURI()).andReturn(foo).anyTimes();
    Principal p = EasyMock.createMock(Principal.class);
    EasyMock.expect(p.getName()).andReturn(CERT_USER).anyTimes();
    EasyMock.expect(req.getUserPrincipal()).andReturn(p).anyTimes();
    EasyMock.expect(req.getRemoteAddr()).andReturn(REMOTE_ADDRESS).anyTimes();
    EasyMock.expect(req.getRemoteUser()).andReturn(null).anyTimes();
    EasyMock.expect(req.getProtocol()).andReturn(HTTP_PROTOCOL).anyTimes();
    EasyMock.expect(req.getMethod()).andReturn(HttpMethod.DELETE).anyTimes();
    EasyMock.replay(p, req, resp);
    Configurator.setLevel(REQUEST_LOG,
        Level.getLevel(INFO));
    JettyRequestLog logger = new JettyRequestLog();
    logger.log(req, resp);
    EasyMock.verify(req);
  }

  @Test
  public void testRequestLogWithDeleteDebugDisabledNullRemoteUserNullCertCoordinator()
  {
    Request req = EasyMock.createMock(Request.class);
    Response resp = EasyMock.createMock(Response.class);
    HttpURI foo = new HttpURI(COORDINATOR);
    EasyMock.expect(req.getHttpURI()).andReturn(foo).anyTimes();
    Principal p = EasyMock.createMock(Principal.class);
    EasyMock.expect(req.getUserPrincipal()).andReturn(null).anyTimes();
    EasyMock.expect(p.getName()).andReturn(null).anyTimes();
    EasyMock.expect(req.getRemoteAddr()).andReturn(REMOTE_ADDRESS).anyTimes();
    EasyMock.expect(req.getRemoteUser()).andReturn(null).anyTimes();
    EasyMock.expect(req.getProtocol()).andReturn(HTTP_PROTOCOL).anyTimes();
    EasyMock.expect(req.getMethod()).andReturn(HttpMethod.DELETE).anyTimes();
    EasyMock.replay(p, req, resp);
    Configurator.setLevel(REQUEST_LOG,
        Level.getLevel(INFO));
    JettyRequestLog logger = new JettyRequestLog();
    logger.log(req, resp);
    EasyMock.verify(req);
  }
  @Test
  public void testRequestLogWithDeleteDebugDisabledNonNullRemoteUserNonOverlord()
  {
    Request req = EasyMock.createMock(Request.class);
    Response resp = EasyMock.createMock(Response.class);
    HttpURI foo = new HttpURI(WORKER);
    EasyMock.expect(req.getHttpURI()).andReturn(foo).anyTimes();
    Principal p = EasyMock.createMock(Principal.class);
    EasyMock.expect(p.getName()).andReturn(CERT_USER).anyTimes();
    EasyMock.expect(req.getUserPrincipal()).andReturn(p).anyTimes();
    EasyMock.expect(req.getRemoteAddr()).andReturn(REMOTE_ADDRESS).anyTimes();
    EasyMock.expect(req.getRemoteUser()).andReturn(REMOTE_USER).anyTimes();
    EasyMock.expect(req.getProtocol()).andReturn(HTTP_PROTOCOL).anyTimes();
    EasyMock.expect(req.getMethod()).andReturn(HttpMethod.DELETE).anyTimes();
    EasyMock.replay(p, req, resp);
    Configurator.setLevel(REQUEST_LOG,
        Level.getLevel(INFO));
    JettyRequestLog logger = new JettyRequestLog();
    logger.log(req, resp);
    EasyMock.verify(req);
  }

  @Test
  public void testRequestLogWithDeleteDebugDisabledNonNullRemoteUserOverlord()
  {
    Request req = EasyMock.createMock(Request.class);
    Response resp = EasyMock.createMock(Response.class);
    HttpURI foo = new HttpURI(OVERLORD);
    EasyMock.expect(req.getHttpURI()).andReturn(foo).anyTimes();
    Principal p = EasyMock.createMock(Principal.class);
    EasyMock.expect(p.getName()).andReturn(CERT_USER).anyTimes();
    EasyMock.expect(req.getUserPrincipal()).andReturn(p).anyTimes();
    EasyMock.expect(req.getRemoteAddr()).andReturn(REMOTE_ADDRESS).anyTimes();
    EasyMock.expect(req.getRemoteUser()).andReturn(REMOTE_USER).anyTimes();
    EasyMock.expect(req.getProtocol()).andReturn(HTTP_PROTOCOL).anyTimes();
    EasyMock.expect(req.getMethod()).andReturn(HttpMethod.DELETE).anyTimes();
    EasyMock.replay(p, req, resp);
    Configurator.setLevel(REQUEST_LOG,
        Level.getLevel(INFO));
    JettyRequestLog logger = new JettyRequestLog();
    logger.log(req, resp);
    EasyMock.verify(req);
  }

  @Test
  public void testRequestLogWithDeleteDebugDisabledNullRemoteUserNonNullCertOverlord()
  {
    Request req = EasyMock.createMock(Request.class);
    Response resp = EasyMock.createMock(Response.class);
    HttpURI foo = new HttpURI(OVERLORD);
    EasyMock.expect(req.getHttpURI()).andReturn(foo).anyTimes();
    Principal p = EasyMock.createMock(Principal.class);
    EasyMock.expect(p.getName()).andReturn(CERT_USER).anyTimes();
    EasyMock.expect(req.getUserPrincipal()).andReturn(p).anyTimes();
    EasyMock.expect(req.getRemoteAddr()).andReturn(REMOTE_ADDRESS).anyTimes();
    EasyMock.expect(req.getRemoteUser()).andReturn(null).anyTimes();
    EasyMock.expect(req.getProtocol()).andReturn(HTTP_PROTOCOL).anyTimes();
    EasyMock.expect(req.getMethod()).andReturn(HttpMethod.DELETE).anyTimes();
    EasyMock.replay(p, req, resp);
    Configurator.setLevel(REQUEST_LOG,
        Level.getLevel(INFO));
    JettyRequestLog logger = new JettyRequestLog();
    logger.log(req, resp);
    EasyMock.verify(req);
  }

  @Test
  public void testRequestLogWithDeleteDebugDisabledNullRemoteUserNullCertOverlord()
  {
    Request req = EasyMock.createMock(Request.class);
    Response resp = EasyMock.createMock(Response.class);
    HttpURI foo = new HttpURI(OVERLORD);
    EasyMock.expect(req.getHttpURI()).andReturn(foo).anyTimes();
    Principal p = EasyMock.createMock(Principal.class);
    EasyMock.expect(req.getUserPrincipal()).andReturn(null).anyTimes();
    EasyMock.expect(p.getName()).andReturn(null).anyTimes();
    EasyMock.expect(req.getRemoteAddr()).andReturn(REMOTE_ADDRESS).anyTimes();
    EasyMock.expect(req.getRemoteUser()).andReturn(null).anyTimes();
    EasyMock.expect(req.getProtocol()).andReturn(HTTP_PROTOCOL).anyTimes();
    EasyMock.expect(req.getMethod()).andReturn(HttpMethod.DELETE).anyTimes();
    EasyMock.replay(p, req, resp);
    Configurator.setLevel(REQUEST_LOG,
        Level.getLevel(INFO));
    JettyRequestLog logger = new JettyRequestLog();
    logger.log(req, resp);
    EasyMock.verify(req);
  }

  @Test
  public void testRequestLogWithPutDebugDisabledNonNullRemoteUserNonCoordinator()
  {
    Request req = EasyMock.createMock(Request.class);
    Response resp = EasyMock.createMock(Response.class);
    HttpURI foo = new HttpURI(WORKER);
    EasyMock.expect(req.getHttpURI()).andReturn(foo).anyTimes();
    Principal p = EasyMock.createMock(Principal.class);
    EasyMock.expect(p.getName()).andReturn(CERT_USER).anyTimes();
    EasyMock.expect(req.getUserPrincipal()).andReturn(p).anyTimes();
    EasyMock.expect(req.getRemoteAddr()).andReturn(REMOTE_ADDRESS).anyTimes();
    EasyMock.expect(req.getRemoteUser()).andReturn(REMOTE_USER).anyTimes();
    EasyMock.expect(req.getProtocol()).andReturn(HTTP_PROTOCOL).anyTimes();
    EasyMock.expect(req.getMethod()).andReturn(HttpMethod.PUT).anyTimes();
    EasyMock.replay(p, req, resp);
    Configurator.setLevel(REQUEST_LOG,
        Level.getLevel(INFO));
    JettyRequestLog logger = new JettyRequestLog();
    logger.log(req, resp);
    EasyMock.verify(req);
  }

  @Test
  public void testRequestLogWithPutDebugDisabledNonNullRemoteUserCoordinator()
  {
    Request req = EasyMock.createMock(Request.class);
    Response resp = EasyMock.createMock(Response.class);
    HttpURI foo = new HttpURI(COORDINATOR);
    EasyMock.expect(req.getHttpURI()).andReturn(foo).anyTimes();
    Principal p = EasyMock.createMock(Principal.class);
    EasyMock.expect(p.getName()).andReturn(CERT_USER).anyTimes();
    EasyMock.expect(req.getUserPrincipal()).andReturn(p).anyTimes();
    EasyMock.expect(req.getRemoteAddr()).andReturn(REMOTE_ADDRESS).anyTimes();
    EasyMock.expect(req.getRemoteUser()).andReturn(REMOTE_USER).anyTimes();
    EasyMock.expect(req.getProtocol()).andReturn(HTTP_PROTOCOL).anyTimes();
    EasyMock.expect(req.getMethod()).andReturn(HttpMethod.PUT).anyTimes();
    EasyMock.replay(p, req, resp);
    Configurator.setLevel(REQUEST_LOG,
        Level.getLevel(INFO));
    JettyRequestLog logger = new JettyRequestLog();
    logger.log(req, resp);
    EasyMock.verify(req);
  }

  @Test
  public void testRequestLogWithPutDebugDisabledNullRemoteUserNonNullCertCoordinator()
  {
    Request req = EasyMock.createMock(Request.class);
    Response resp = EasyMock.createMock(Response.class);
    HttpURI foo = new HttpURI(COORDINATOR);
    EasyMock.expect(req.getHttpURI()).andReturn(foo).anyTimes();
    Principal p = EasyMock.createMock(Principal.class);
    EasyMock.expect(p.getName()).andReturn(CERT_USER).anyTimes();
    EasyMock.expect(req.getUserPrincipal()).andReturn(p).anyTimes();
    EasyMock.expect(req.getRemoteAddr()).andReturn(REMOTE_ADDRESS).anyTimes();
    EasyMock.expect(req.getRemoteUser()).andReturn(null).anyTimes();
    EasyMock.expect(req.getProtocol()).andReturn(HTTP_PROTOCOL).anyTimes();
    EasyMock.expect(req.getMethod()).andReturn(HttpMethod.PUT).anyTimes();
    EasyMock.replay(p, req, resp);
    Configurator.setLevel(REQUEST_LOG,
        Level.getLevel(INFO));
    JettyRequestLog logger = new JettyRequestLog();
    logger.log(req, resp);
    EasyMock.verify(req);
  }

  @Test
  public void testRequestLogWithPutDebugDisabledNullRemoteUserNullCertCoordinator()
  {
    Request req = EasyMock.createMock(Request.class);
    Response resp = EasyMock.createMock(Response.class);
    HttpURI foo = new HttpURI(COORDINATOR);
    EasyMock.expect(req.getHttpURI()).andReturn(foo).anyTimes();
    Principal p = EasyMock.createMock(Principal.class);
    EasyMock.expect(req.getUserPrincipal()).andReturn(null).anyTimes();
    EasyMock.expect(p.getName()).andReturn(null).anyTimes();
    EasyMock.expect(req.getRemoteAddr()).andReturn(REMOTE_ADDRESS).anyTimes();
    EasyMock.expect(req.getRemoteUser()).andReturn(null).anyTimes();
    EasyMock.expect(req.getProtocol()).andReturn(HTTP_PROTOCOL).anyTimes();
    EasyMock.expect(req.getMethod()).andReturn(HttpMethod.PUT).anyTimes();
    EasyMock.replay(p, req, resp);
    Configurator.setLevel(REQUEST_LOG,
        Level.getLevel(INFO));
    JettyRequestLog logger = new JettyRequestLog();
    logger.log(req, resp);
    EasyMock.verify(req);
  }
  @Test
  public void testRequestLogWithPutDebugDisabledNonNullRemoteUserNonOverlord()
  {
    Request req = EasyMock.createMock(Request.class);
    Response resp = EasyMock.createMock(Response.class);
    HttpURI foo = new HttpURI(WORKER);
    EasyMock.expect(req.getHttpURI()).andReturn(foo).anyTimes();
    Principal p = EasyMock.createMock(Principal.class);
    EasyMock.expect(p.getName()).andReturn(CERT_USER).anyTimes();
    EasyMock.expect(req.getUserPrincipal()).andReturn(p).anyTimes();
    EasyMock.expect(req.getRemoteAddr()).andReturn(REMOTE_ADDRESS).anyTimes();
    EasyMock.expect(req.getRemoteUser()).andReturn(REMOTE_USER).anyTimes();
    EasyMock.expect(req.getProtocol()).andReturn(HTTP_PROTOCOL).anyTimes();
    EasyMock.expect(req.getMethod()).andReturn(HttpMethod.PUT).anyTimes();
    EasyMock.replay(p, req, resp);
    Configurator.setLevel(REQUEST_LOG,
        Level.getLevel(INFO));
    JettyRequestLog logger = new JettyRequestLog();
    logger.log(req, resp);
    EasyMock.verify(req);
  }

  @Test
  public void testRequestLogWithPutDebugDisabledNonNullRemoteUserOverlord()
  {
    Request req = EasyMock.createMock(Request.class);
    Response resp = EasyMock.createMock(Response.class);
    HttpURI foo = new HttpURI(OVERLORD);
    EasyMock.expect(req.getHttpURI()).andReturn(foo).anyTimes();
    Principal p = EasyMock.createMock(Principal.class);
    EasyMock.expect(p.getName()).andReturn(CERT_USER).anyTimes();
    EasyMock.expect(req.getUserPrincipal()).andReturn(p).anyTimes();
    EasyMock.expect(req.getRemoteAddr()).andReturn(REMOTE_ADDRESS).anyTimes();
    EasyMock.expect(req.getRemoteUser()).andReturn(REMOTE_USER).anyTimes();
    EasyMock.expect(req.getProtocol()).andReturn(HTTP_PROTOCOL).anyTimes();
    EasyMock.expect(req.getMethod()).andReturn(HttpMethod.PUT).anyTimes();
    EasyMock.replay(p, req, resp);
    Configurator.setLevel(REQUEST_LOG,
        Level.getLevel(INFO));
    JettyRequestLog logger = new JettyRequestLog();
    logger.log(req, resp);
    EasyMock.verify(req);
  }

  @Test
  public void testRequestLogWithPutDebugDisabledNullRemoteUserNonNullCertOverlord()
  {
    Request req = EasyMock.createMock(Request.class);
    Response resp = EasyMock.createMock(Response.class);
    HttpURI foo = new HttpURI(OVERLORD);
    EasyMock.expect(req.getHttpURI()).andReturn(foo).anyTimes();
    Principal p = EasyMock.createMock(Principal.class);
    EasyMock.expect(p.getName()).andReturn(CERT_USER).anyTimes();
    EasyMock.expect(req.getUserPrincipal()).andReturn(p).anyTimes();
    EasyMock.expect(req.getRemoteAddr()).andReturn(REMOTE_ADDRESS).anyTimes();
    EasyMock.expect(req.getRemoteUser()).andReturn(null).anyTimes();
    EasyMock.expect(req.getProtocol()).andReturn(HTTP_PROTOCOL).anyTimes();
    EasyMock.expect(req.getMethod()).andReturn(HttpMethod.PUT).anyTimes();
    EasyMock.replay(p, req, resp);
    Configurator.setLevel(REQUEST_LOG,
        Level.getLevel(INFO));
    JettyRequestLog logger = new JettyRequestLog();
    logger.log(req, resp);
    EasyMock.verify(req);
  }

  @Test
  public void testRequestLogWithPutDebugDisabledNullRemoteUserNullCertOverlord()
  {
    Request req = EasyMock.createMock(Request.class);
    Response resp = EasyMock.createMock(Response.class);
    HttpURI foo = new HttpURI(OVERLORD);
    EasyMock.expect(req.getHttpURI()).andReturn(foo).anyTimes();
    Principal p = EasyMock.createMock(Principal.class);
    EasyMock.expect(req.getUserPrincipal()).andReturn(null).anyTimes();
    EasyMock.expect(p.getName()).andReturn(null).anyTimes();
    EasyMock.expect(req.getRemoteAddr()).andReturn(REMOTE_ADDRESS).anyTimes();
    EasyMock.expect(req.getRemoteUser()).andReturn(null).anyTimes();
    EasyMock.expect(req.getProtocol()).andReturn(HTTP_PROTOCOL).anyTimes();
    EasyMock.expect(req.getMethod()).andReturn(HttpMethod.PUT).anyTimes();
    EasyMock.replay(p, req, resp);
    Configurator.setLevel(REQUEST_LOG,
        Level.getLevel(INFO));
    JettyRequestLog logger = new JettyRequestLog();
    logger.log(req, resp);
    EasyMock.verify(req);
  }
}

