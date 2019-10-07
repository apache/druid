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

package org.apache.druid.server;

import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.name.Names;
import com.sun.jersey.api.container.grizzly2.GrizzlyServerFactory;
import com.sun.jersey.api.core.ClassNamesResourceConfig;
import com.sun.jersey.api.core.ResourceConfig;
import com.sun.jersey.core.spi.component.ioc.IoCComponentProviderFactory;
import com.sun.jersey.guice.spi.container.GuiceComponentProviderFactory;
import com.sun.jersey.spi.inject.SingletonTypeInjectableProvider;
import org.apache.druid.guice.GuiceInjectors;
import org.apache.druid.guice.annotations.Client;
import org.apache.druid.initialization.Initialization;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthTestUtils;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.AuthenticatorMapper;
import org.apache.druid.server.security.AuthorizerMapper;
import org.easymock.EasyMock;
import org.glassfish.grizzly.http.server.HttpServer;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.ext.Provider;
import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;

public class WebserverTestUtils
{

  public static URI createBaseUri()
  {
    final int port = ThreadLocalRandom.current().nextInt(1024, 65534);
    return UriBuilder.fromUri("http://localhost/").port(port).build();
  }

  public static HttpServer createServer(
      String SERVICE_NAME,
      URI baseUri,
      String resourceClassName,
      Consumer<Binder> extender
  )
      throws IOException
  {
    Injector injector = Initialization.makeInjectorWithModules(
        GuiceInjectors.makeStartupInjector(),
        ImmutableList.of(binder -> {
          binder.bindConstant().annotatedWith(Names.named("serviceName")).to(SERVICE_NAME);
          binder.bindConstant().annotatedWith(Names.named("servicePort")).to(baseUri.getPort());
          binder.bindConstant().annotatedWith(Names.named("tlsServicePort")).to(baseUri.getPort() + 1);
          binder.bind(Key.get(ServiceEmitter.class)).toInstance(new NoopServiceEmitter());
          binder.bind(Key.get(AuthConfig.class)).toInstance(new AuthConfig());
          binder.bind(AuthorizerMapper.class).toInstance(AuthTestUtils.TEST_AUTHORIZER_MAPPER);
          binder.bind(AuthenticatorMapper.class).toInstance(AuthTestUtils.TEST_AUTHENTICATOR_MAPPER);
          binder.bind(Key.get(HttpClient.class, Client.class)).toInstance(EasyMock.createMock(HttpClient.class));
          extender.accept(binder);
        })
    );
    ResourceConfig resourceConfig = new ClassNamesResourceConfig(
        resourceClassName
        + ';'
        + MockHttpServletRequest.class.getName()
    );
    IoCComponentProviderFactory ioc = new GuiceComponentProviderFactory(resourceConfig, injector);
    HttpServer server = GrizzlyServerFactory.createHttpServer(baseUri, resourceConfig, ioc);
    return server;
  }

  @Provider
  public static class MockHttpServletRequest extends
      SingletonTypeInjectableProvider<Context, HttpServletRequest>
  {
    public MockHttpServletRequest()
    {
      super(
          HttpServletRequest.class,
          createMockRequest()
      );
    }

    static HttpServletRequest createMockRequest()
    {
      HttpServletRequest mockRequest = EasyMock.createNiceMock(HttpServletRequest.class);
      AuthenticationResult authenticationResult = new AuthenticationResult(
          "druid",
          "druid",
          null,
          Collections.emptyMap()
      );

      EasyMock.expect(mockRequest.getAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED)).andReturn(null).anyTimes();
      EasyMock.expect(mockRequest.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT))
              .andReturn(authenticationResult)
              .anyTimes();
      EasyMock.replay(mockRequest);
      return mockRequest;
    }
  }
}
