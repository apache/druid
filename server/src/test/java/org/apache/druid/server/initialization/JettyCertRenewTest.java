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

package org.apache.druid.server.initialization;

import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.multibindings.Multibinder;
import org.apache.druid.guice.GuiceInjectors;
import org.apache.druid.guice.Jerseys;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.LifecycleModule;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.initialization.Initialization;
import org.apache.druid.java.util.common.lifecycle.Lifecycle;
import org.apache.druid.java.util.http.client.HttpClientConfig;
import org.apache.druid.java.util.http.client.HttpClientInit;
import org.apache.druid.metadata.PasswordProvider;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.initialization.jetty.JettyServerInitializer;
import org.apache.druid.server.initialization.jetty.ServletFilterHolder;
import org.apache.druid.server.security.AuthTestUtils;
import org.apache.druid.server.security.AuthorizerMapper;
import org.eclipse.jetty.server.Server;
import org.joda.time.Duration;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import javax.servlet.DispatcherType;
import javax.servlet.Filter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.EnumSet;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

public class JettyCertRenewTest extends JettyTest
{
  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  private HttpClientConfig sslConfig;

  private Injector injector;

  private LatchedRequestStateHolder latchedRequestState;

  @Override
  public void setProperties()
  {
    // call super.setProperties first in case it is setting the same property as this class
    super.setProperties();
    System.setProperty("druid.server.http.showDetailedJettyErrors", "true");
  }

  @Override
  @Before
  public void setup() throws Exception
  {
    setProperties();
    Injector injector = setupInjector();
    final DruidNode node = injector.getInstance(Key.get(DruidNode.class, Self.class));
    port = node.getPlaintextPort();
    tlsPort = node.getTlsPort();

    lifecycle = injector.getInstance(Lifecycle.class);
    lifecycle.start();
    BaseJettyTest.ClientHolder holder = injector.getInstance(BaseJettyTest.ClientHolder.class);
    server = injector.getInstance(Server.class);
    client = holder.getClient();

    File keyStore = new File(JettyCertRenewTest.class.getClassLoader().getResource("server-new.jks").getFile());
    Files.copy(keyStore.toPath(), new File(folder.newFolder(), "server.jks").toPath());
    File trustStore = new File(JettyCertRenewTest.class.getClassLoader().getResource("truststore-new.jks").getFile());
    Files.copy(trustStore.toPath(), new File(folder.newFolder(), "truststore.jks").toPath());

    Thread.sleep(2000);
  }

  @Override
  protected Injector setupInjector()
  {
    TLSServerConfig tlsConfig;
    try {
      File keyStore = new File(JettyCertRenewTest.class.getClassLoader().getResource("server.jks").getFile());
      Path tmpKeyStore = Files.copy(keyStore.toPath(), new File(folder.newFolder(), "server.jks").toPath());
      File trustStore = new File(JettyCertRenewTest.class.getClassLoader().getResource("truststore.jks").getFile());
      Path tmpTrustStore = Files.copy(trustStore.toPath(), new File(folder.newFolder(), "truststore.jks").toPath());
      PasswordProvider pp = () -> "druid123";
      tlsConfig = new TLSServerConfig()
      {
        @Override
        public String getKeyStorePath()
        {
          return tmpKeyStore.toString();
        }

        @Override
        public String getKeyStoreType()
        {
          return "jks";
        }

        @Override
        public PasswordProvider getKeyStorePasswordProvider()
        {
          return pp;
        }

        @Override
        public PasswordProvider getKeyManagerPasswordProvider()
        {
          return pp;
        }

        @Override
        public String getTrustStorePath()
        {
          return tmpTrustStore.toString();
        }

        @Override
        public String getTrustStoreAlgorithm()
        {
          return "PKIX";
        }

        @Override
        public PasswordProvider getTrustStorePasswordProvider()
        {
          return pp;
        }

        @Override
        public String getCertAlias()
        {
          return "druid";
        }

        @Override
        public boolean isRequireClientCertificate()
        {
          return false;
        }

        @Override
        public boolean isRequestClientCertificate()
        {
          return false;
        }

        @Override
        public boolean isValidateHostnames()
        {
          return false;
        }

        @Override
        public boolean isReloadSslContext()
        {
          return true;
        }

        @Override
        public int getReloadSslContextSeconds()
        {
          return 1;
        }
      };

      sslConfig =
          HttpClientConfig.builder()
                          .withSslContext(
                              HttpClientInit.sslContextWithTrustedKeyStore(tmpTrustStore.toString(), pp.getPassword())
                          )
                          .withWorkerCount(1)
                          .withReadTimeout(Duration.ZERO)
                          .build();
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }

    final int ephemeralPort = ThreadLocalRandom.current().nextInt(49152, 65535);

    latchedRequestState = new LatchedRequestStateHolder();
    injector = Initialization.makeInjectorWithModules(
        GuiceInjectors.makeStartupInjector(),
        ImmutableList.<Module>of(
            new Module()
            {
              @Override
              public void configure(Binder binder)
              {
                JsonConfigProvider.bindInstance(
                    binder,
                    Key.get(DruidNode.class, Self.class),
                    new DruidNode("test", "localhost", false, ephemeralPort, ephemeralPort + 1, true, true)
                );
                binder.bind(TLSServerConfig.class).toInstance(tlsConfig);
                binder.bind(JettyServerInitializer.class).to(JettyServerInit.class).in(LazySingleton.class);
                binder.bind(LatchedRequestStateHolder.class).toInstance(latchedRequestState);

                Multibinder<ServletFilterHolder> multibinder = Multibinder.newSetBinder(
                    binder,
                    ServletFilterHolder.class
                );

                multibinder.addBinding().toInstance(
                    new ServletFilterHolder()
                    {
                      @Override
                      public String getPath()
                      {
                        return "/*";
                      }

                      @Override
                      public Map<String, String> getInitParameters()
                      {
                        return null;
                      }

                      @Override
                      public Class<? extends Filter> getFilterClass()
                      {
                        return DummyAuthFilter.class;
                      }

                      @Override
                      public Filter getFilter()
                      {
                        return null;
                      }

                      @Override
                      public EnumSet<DispatcherType> getDispatcherType()
                      {
                        return null;
                      }
                    }
                );


                Jerseys.addResource(binder, SlowResource.class);
                Jerseys.addResource(binder, LatchedResource.class);
                Jerseys.addResource(binder, ExceptionResource.class);
                Jerseys.addResource(binder, DefaultResource.class);
                Jerseys.addResource(binder, DirectlyReturnResource.class);
                binder.bind(AuthorizerMapper.class).toInstance(AuthTestUtils.TEST_AUTHORIZER_MAPPER);
                LifecycleModule.register(binder, Server.class);
              }
            }
        )
    );

    return injector;
  }
}
