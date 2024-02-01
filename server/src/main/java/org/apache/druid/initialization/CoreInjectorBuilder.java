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

package org.apache.druid.initialization;

import com.google.inject.Injector;
import org.apache.druid.curator.CuratorModule;
import org.apache.druid.curator.discovery.DiscoveryModule;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.guice.AnnouncerModule;
import org.apache.druid.guice.CoordinatorDiscoveryModule;
import org.apache.druid.guice.DruidInjectorBuilder;
import org.apache.druid.guice.DruidSecondaryModule;
import org.apache.druid.guice.ExpressionModule;
import org.apache.druid.guice.ExtensionsModule;
import org.apache.druid.guice.FirehoseModule;
import org.apache.druid.guice.JacksonConfigManagerModule;
import org.apache.druid.guice.JavaScriptModule;
import org.apache.druid.guice.LifecycleModule;
import org.apache.druid.guice.LocalDataStorageDruidModule;
import org.apache.druid.guice.MetadataConfigModule;
import org.apache.druid.guice.NestedDataModule;
import org.apache.druid.guice.ServerModule;
import org.apache.druid.guice.ServerViewModule;
import org.apache.druid.guice.StartupLoggingModule;
import org.apache.druid.guice.StorageNodeModule;
import org.apache.druid.guice.annotations.Client;
import org.apache.druid.guice.annotations.EscalatedClient;
import org.apache.druid.guice.http.HttpClientModule;
import org.apache.druid.guice.security.AuthenticatorModule;
import org.apache.druid.guice.security.AuthorizerModule;
import org.apache.druid.guice.security.DruidAuthModule;
import org.apache.druid.guice.security.EscalatorModule;
import org.apache.druid.metadata.storage.derby.DerbyMetadataStorageDruidModule;
import org.apache.druid.rpc.guice.ServiceClientModule;
import org.apache.druid.segment.writeout.SegmentWriteOutMediumModule;
import org.apache.druid.server.emitter.EmitterModule;
import org.apache.druid.server.initialization.AuthenticatorMapperModule;
import org.apache.druid.server.initialization.AuthorizerMapperModule;
import org.apache.druid.server.initialization.ExternalStorageAccessSecurityModule;
import org.apache.druid.server.initialization.jetty.JettyServerModule;
import org.apache.druid.server.metrics.MetricsModule;
import org.apache.druid.server.security.TLSCertificateCheckerModule;
import org.apache.druid.storage.StorageConnectorModule;

import java.util.Collections;
import java.util.Set;

/**
 * Builds the core (common) set of modules used by all Druid services and
 * commands. The basic injector just adds logging and the Druid lifecycle.
 * Call {@link #forServer()} to add the server-specific modules.
 */
public class CoreInjectorBuilder extends DruidInjectorBuilder
{
  public CoreInjectorBuilder(final Injector baseInjector)
  {
    this(baseInjector, Collections.emptySet());
  }

  public CoreInjectorBuilder(final Injector baseInjector, final Set<NodeRole> nodeRoles)
  {
    super(baseInjector, nodeRoles);
    add(DruidSecondaryModule.class);
  }

  public CoreInjectorBuilder withLogging()
  {
    // New modules should be added after Log4jShutterDownerModule
    add(new Log4jShutterDownerModule());
    return this;
  }

  public CoreInjectorBuilder withLifecycle()
  {
    add(new LifecycleModule());
    return this;
  }

  public CoreInjectorBuilder forServer()
  {
    withLogging();
    withLifecycle();
    add(
        ExtensionsModule.SecondaryModule.class,
        new DruidAuthModule(),
        TLSCertificateCheckerModule.class,
        EmitterModule.class,
        HttpClientModule.global(),
        HttpClientModule.escalatedGlobal(),
        new HttpClientModule("druid.broker.http", Client.class, true),
        new HttpClientModule("druid.broker.http", EscalatedClient.class, true),
        new CuratorModule(),
        new AnnouncerModule(),
        new MetricsModule(),
        new SegmentWriteOutMediumModule(),
        new ServerModule(),
        new StorageNodeModule(),
        new JettyServerModule(),
        new ExpressionModule(),
        new NestedDataModule(),
        new DiscoveryModule(),
        new ServerViewModule(),
        new MetadataConfigModule(),
        new DerbyMetadataStorageDruidModule(),
        new JacksonConfigManagerModule(),
        new CoordinatorDiscoveryModule(),
        new LocalDataStorageDruidModule(),
        new TombstoneDataStorageModule(),
        new FirehoseModule(),
        new JavaScriptModule(),
        new AuthenticatorModule(),
        new AuthenticatorMapperModule(),
        new EscalatorModule(),
        new AuthorizerModule(),
        new AuthorizerMapperModule(),
        new StartupLoggingModule(),
        new ExternalStorageAccessSecurityModule(),
        new ServiceClientModule(),
        new StorageConnectorModule()
    );
    return this;
  }
}
