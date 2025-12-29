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

import com.google.inject.Binding;
import com.google.inject.Injector;
import com.google.inject.Provider;
import org.apache.druid.java.util.common.lifecycle.Lifecycle;
import org.apache.druid.java.util.metrics.StubServiceEmitter;
import org.apache.druid.metadata.PasswordProvider;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.initialization.ServerConfig;
import org.apache.druid.server.initialization.TLSServerConfig;
import org.apache.druid.server.security.TLSCertificateChecker;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.List;

public class JettyServerModuleTest
{
  @Test
  public void testJettyServerModule()
  {
    QueuedThreadPool jettyServerThreadPool = Mockito.mock(QueuedThreadPool.class);
    JettyServerModule.setJettyServerThreadPool(jettyServerThreadPool);
    Mockito.when(jettyServerThreadPool.getThreads()).thenReturn(100);
    Mockito.when(jettyServerThreadPool.getIdleThreads()).thenReturn(40);
    Mockito.when(jettyServerThreadPool.isLowOnThreads()).thenReturn(true);
    Mockito.when(jettyServerThreadPool.getMinThreads()).thenReturn(30);
    Mockito.when(jettyServerThreadPool.getMaxThreads()).thenReturn(100);
    Mockito.when(jettyServerThreadPool.getQueueSize()).thenReturn(50);
    Mockito.when(jettyServerThreadPool.getBusyThreads()).thenReturn(60);

    JettyServerModule.JettyMonitor jettyMonitor = new JettyServerModule.JettyMonitor();

    final StubServiceEmitter serviceEmitter = new StubServiceEmitter("service", "host");
    serviceEmitter.start();
    jettyMonitor.doMonitor(serviceEmitter);

    serviceEmitter.verifyValue("jetty/numOpenConnections", 0);
    serviceEmitter.verifyValue("jetty/threadPool/total", 100);
    serviceEmitter.verifyValue("jetty/threadPool/idle", 40);
    serviceEmitter.verifyValue("jetty/threadPool/isLowOnThreads", 1);
    serviceEmitter.verifyValue("jetty/threadPool/min", 30);
    serviceEmitter.verifyValue("jetty/threadPool/max", 100);
    serviceEmitter.verifyValue("jetty/threadPool/queueSize", 50);
    serviceEmitter.verifyValue("jetty/threadPool/busy", 60);
  }

  @Test
  public void test_isEnableTlsPort_withoutBinding()
  {
    String keystorePath = JettyServerModuleTest.class.getClassLoader().getResource("server.jks").getFile();

    PasswordProvider pp = Mockito.mock(PasswordProvider.class);
    Mockito.when(pp.getPassword()).thenReturn("druid123");

    TLSServerConfig tlsServerConfig = TLSServerConfig.builder()
                                                     .keyStorePath(keystorePath)
                                                     .keyStoreType("jks")
                                                     .keyStorePasswordProvider(pp)
                                                     .certAlias("druid")
                                                     .includeCipherSuites(List.of(
                                                         "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256"))
                                                     .includeProtocols(Arrays.asList("TLSv1.2", "TLSv1.3"))
                                                     .requireClientCertificate(false)
                                                     .requestClientCertificate(false)
                                                     .build();

    DruidNode node = new DruidNode("test", "localhost", false, 8080, 8443, true, true);
    ServerConfig serverConfig = new ServerConfig();
    Lifecycle lifecycle = new Lifecycle();
    Injector injector = Mockito.mock(Injector.class);
    TLSCertificateChecker certificateChecker = Mockito.mock(TLSCertificateChecker.class);

    JettyServerInitializer initializer = Mockito.mock(JettyServerInitializer.class);
    Mockito.when(injector.getInstance(JettyServerInitializer.class)).thenReturn(initializer);

    Server server = JettyServerModule.makeAndInitializeServer(
        injector,
        lifecycle,
        node,
        serverConfig,
        tlsServerConfig,
        null,
        certificateChecker
    );

    Assert.assertNotNull(server);
  }

  @Test
  public void test_isEnableTlsPort_withBinding_forceApplyConfig_isTrue()
  {
    String keystorePath = JettyServerModuleTest.class.getClassLoader().getResource("server.jks").getFile();

    PasswordProvider pp = Mockito.mock(PasswordProvider.class);
    Mockito.when(pp.getPassword()).thenReturn("druid123");

    TLSServerConfig tlsServerConfig = TLSServerConfig.builder()
                                                     .keyStorePath(keystorePath)
                                                     .keyStoreType("jks")
                                                     .keyStorePasswordProvider(pp)
                                                     .certAlias("druid")
                                                     .excludeCipherSuites(List.of("TLS_RSA_WITH_NULL_SHA256"))
                                                     .excludeProtocols(Arrays.asList("TLSv1", "TLSv1.1"))
                                                     .requireClientCertificate(false)
                                                     .requestClientCertificate(false)
                                                     .forceApplyConfig(true) // Force config to be applied
                                                     .build();

    DruidNode node = new DruidNode("test", "localhost", false, 8080, 8443, true, true);
    ServerConfig serverConfig = new ServerConfig();
    Lifecycle lifecycle = new Lifecycle();
    Injector injector = Mockito.mock(Injector.class);
    TLSCertificateChecker certificateChecker = Mockito.mock(TLSCertificateChecker.class);

    JettyServerInitializer initializer = Mockito.mock(JettyServerInitializer.class);
    Mockito.when(injector.getInstance(JettyServerInitializer.class)).thenReturn(initializer);

    // Create a custom SslContextFactory via binding
    SslContextFactory.Server customSslContextFactory = Mockito.mock(SslContextFactory.Server.class);

    @SuppressWarnings("unchecked")
    Binding<SslContextFactory.Server> sslContextFactoryBinding = Mockito.mock(Binding.class);
    @SuppressWarnings("unchecked")
    Provider<SslContextFactory.Server> provider = Mockito.mock(Provider.class);

    Mockito.when(sslContextFactoryBinding.getProvider()).thenReturn(provider);
    Mockito.when(provider.get()).thenReturn(customSslContextFactory);

    Server server = JettyServerModule.makeAndInitializeServer(
        injector,
        lifecycle,
        node,
        serverConfig,
        tlsServerConfig,
        sslContextFactoryBinding,
        certificateChecker
    );

    Assert.assertNotNull(server);

    // Verify that custom SSL context factory was used
    Mockito.verify(provider).get();

    // Verify that TLS config was still applied because forceApplyConfig=true
    Mockito.verify(customSslContextFactory).setKeyStorePath(keystorePath);
    Mockito.verify(customSslContextFactory).setKeyStoreType("jks");
    Mockito.verify(customSslContextFactory).setKeyStorePassword("druid123");
    Mockito.verify(customSslContextFactory).setCertAlias("druid");
    Mockito.verify(customSslContextFactory).setExcludeCipherSuites("TLS_RSA_WITH_NULL_SHA256");
    Mockito.verify(customSslContextFactory).setExcludeProtocols("TLSv1", "TLSv1.1");
  }

  @Test
  public void test_isEnableTlsPort_withBinding_forceApplyConfig_isFalse()
  {
    String keystorePath = JettyServerModuleTest.class.getClassLoader().getResource("server.jks").getFile();

    PasswordProvider pp = Mockito.mock(PasswordProvider.class);
    Mockito.when(pp.getPassword()).thenReturn("druid123");

    TLSServerConfig tlsServerConfig = TLSServerConfig.builder()
                                                     .keyStorePath(keystorePath)
                                                     .keyStoreType("jks")
                                                     .keyStorePasswordProvider(pp)
                                                     .certAlias("druid")
                                                     .excludeCipherSuites(List.of("TLS_RSA_WITH_NULL_SHA256"))
                                                     .excludeProtocols(Arrays.asList("TLSv1", "TLSv1.1"))
                                                     .requireClientCertificate(false)
                                                     .requestClientCertificate(false)
                                                     .forceApplyConfig(false)
                                                     .build();

    DruidNode node = new DruidNode("test", "localhost", false, 8080, 8443, true, true);
    ServerConfig serverConfig = new ServerConfig();
    Lifecycle lifecycle = new Lifecycle();
    Injector injector = Mockito.mock(Injector.class);
    TLSCertificateChecker certificateChecker = Mockito.mock(TLSCertificateChecker.class);

    JettyServerInitializer initializer = Mockito.mock(JettyServerInitializer.class);
    Mockito.when(injector.getInstance(JettyServerInitializer.class)).thenReturn(initializer);

    // Create a custom SslContextFactory via binding
    SslContextFactory.Server customSslContextFactory = Mockito.mock(SslContextFactory.Server.class);

    @SuppressWarnings("unchecked")
    Binding<SslContextFactory.Server> sslContextFactoryBinding = Mockito.mock(Binding.class);
    @SuppressWarnings("unchecked")
    Provider<SslContextFactory.Server> provider = Mockito.mock(Provider.class);

    Mockito.when(sslContextFactoryBinding.getProvider()).thenReturn(provider);
    Mockito.when(provider.get()).thenReturn(customSslContextFactory);

    Server server = JettyServerModule.makeAndInitializeServer(
        injector,
        lifecycle,
        node,
        serverConfig,
        tlsServerConfig,
        sslContextFactoryBinding,
        certificateChecker
    );

    Assert.assertNotNull(server);

    // Verify that custom SSL context factory was used
    Mockito.verify(provider).get();

    // Verify that TLS config was not still applied because forceApplyConfig=false
    Mockito.verify(customSslContextFactory, Mockito.never()).setKeyStorePath(keystorePath);
    Mockito.verify(customSslContextFactory, Mockito.never()).setKeyStoreType("jks");
    Mockito.verify(customSslContextFactory, Mockito.never()).setKeyStorePassword("druid123");
    Mockito.verify(customSslContextFactory, Mockito.never()).setCertAlias("druid");
    Mockito.verify(customSslContextFactory, Mockito.never()).setExcludeCipherSuites("TLS_RSA_WITH_NULL_SHA256");
    Mockito.verify(customSslContextFactory, Mockito.never()).setExcludeProtocols("TLSv1", "TLSv1.1");
  }
}
