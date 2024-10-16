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

package org.apache.druid.msq.dart.guice;

import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Provides;
import org.apache.druid.discovery.DruidNodeDiscoveryProvider;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.guice.Jerseys;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.LifecycleModule;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.guice.ManageLifecycleAnnouncements;
import org.apache.druid.guice.annotations.LoadScope;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.messages.server.MessageRelayMonitor;
import org.apache.druid.messages.server.MessageRelayResource;
import org.apache.druid.messages.server.Outbox;
import org.apache.druid.messages.server.OutboxImpl;
import org.apache.druid.msq.dart.Dart;
import org.apache.druid.msq.dart.DartResourcePermissionMapper;
import org.apache.druid.msq.dart.controller.messages.ControllerMessage;
import org.apache.druid.msq.dart.worker.DartDataSegmentProvider;
import org.apache.druid.msq.dart.worker.DartWorkerFactory;
import org.apache.druid.msq.dart.worker.DartWorkerFactoryImpl;
import org.apache.druid.msq.dart.worker.DartWorkerRunner;
import org.apache.druid.msq.dart.worker.http.DartWorkerResource;
import org.apache.druid.msq.exec.MemoryIntrospector;
import org.apache.druid.msq.querykit.DataSegmentProvider;
import org.apache.druid.msq.rpc.ResourcePermissionMapper;
import org.apache.druid.query.DruidProcessingConfig;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.security.AuthorizerMapper;

import java.io.File;
import java.util.Properties;
import java.util.concurrent.ExecutorService;

/**
 * Primary module for workers. Checks {@link DartModules#isDartEnabled(Properties)} before installing itself.
 */
@LoadScope(roles = NodeRole.HISTORICAL_JSON_NAME)
public class DartWorkerModule implements DruidModule
{
  @Inject
  private Properties properties;

  @Override
  public void configure(Binder binder)
  {
    if (DartModules.isDartEnabled(properties)) {
      binder.install(new ActualModule());
    }
  }

  public static class ActualModule implements Module
  {
    @Override
    public void configure(Binder binder)
    {
      JsonConfigProvider.bind(binder, DartModules.DART_PROPERTY_BASE + ".worker", DartWorkerConfig.class);
      Jerseys.addResource(binder, DartWorkerResource.class);
      LifecycleModule.register(binder, DartWorkerRunner.class);
      LifecycleModule.registerKey(binder, Key.get(MessageRelayMonitor.class, Dart.class));

      binder.bind(DartWorkerFactory.class)
            .to(DartWorkerFactoryImpl.class)
            .in(LazySingleton.class);

      binder.bind(DataSegmentProvider.class)
            .annotatedWith(Dart.class)
            .to(DartDataSegmentProvider.class)
            .in(LazySingleton.class);

      binder.bind(ResourcePermissionMapper.class)
            .annotatedWith(Dart.class)
            .to(DartResourcePermissionMapper.class);
    }

    @Provides
    @ManageLifecycle
    public DartWorkerRunner createWorkerRunner(
        @Self final DruidNode selfNode,
        final DartWorkerFactory workerFactory,
        final DruidNodeDiscoveryProvider discoveryProvider,
        final DruidProcessingConfig processingConfig,
        @Dart final ResourcePermissionMapper permissionMapper,
        final MemoryIntrospector memoryIntrospector,
        final AuthorizerMapper authorizerMapper
    )
    {
      final ExecutorService exec = Execs.multiThreaded(memoryIntrospector.numTasksInJvm(), "dart-worker-%s");
      final File baseTempDir =
          new File(processingConfig.getTmpDir(), StringUtils.format("dart_%s", selfNode.getPortToUse()));
      return new DartWorkerRunner(
          workerFactory,
          exec,
          discoveryProvider,
          permissionMapper,
          authorizerMapper,
          baseTempDir
      );
    }

    @Provides
    @Dart
    public MessageRelayMonitor createMessageRelayMonitor(
        final DruidNodeDiscoveryProvider discoveryProvider,
        final Outbox<ControllerMessage> outbox
    )
    {
      return new MessageRelayMonitor(discoveryProvider, outbox, NodeRole.BROKER);
    }

    /**
     * Create an {@link Outbox}.
     *
     * This is {@link ManageLifecycleAnnouncements} scoped so {@link OutboxImpl#stop()} gets called before attempting
     * to shut down the Jetty server. If this doesn't happen, then server shutdown is delayed by however long it takes
     * any currently-in-flight {@link MessageRelayResource#httpGetMessagesFromOutbox} to resolve.
     */
    @Provides
    @ManageLifecycleAnnouncements
    public Outbox<ControllerMessage> createOutbox()
    {
      return new OutboxImpl<>();
    }
  }
}
