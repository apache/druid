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

package org.apache.druid.guice;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Provides;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.lifecycle.Lifecycle;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.FakeQuery;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.initialization.ServerConfig;
import org.apache.druid.server.initialization.TLSServerConfig;
import org.apache.druid.server.initialization.jetty.JettyServerModule;
import org.apache.druid.server.security.TLSCertificateChecker;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.joda.time.Period;

import java.util.Collections;

public class PeonServerModule implements Module
{
  private static final Logger log = new Logger(PeonServerModule.class);

  /**
   * Used for the {@link FakeQuery} probe when the task has no datasource, since {@link TableDataSource} requires one.
   */
  private static final String PLACEHOLDER_DATASOURCE = "__fake";

  @Override
  public void configure(Binder binder)
  {
  }

  @Provides
  @LazySingleton
  public Server getServer(
      final Injector injector,
      final Lifecycle lifecycle,
      @Self final DruidNode node,
      final ServerConfig config,
      final TLSServerConfig tlsServerConfig,
      final Task task
  )
  {
    return JettyServerModule.makeAndInitializeServer(
        injector,
        lifecycle,
        node,
        adjustServerConfig(config, task),
        tlsServerConfig,
        injector.getExistingBinding(Key.get(SslContextFactory.Server.class)),
        injector.getInstance(TLSCertificateChecker.class)
    );
  }

  @VisibleForTesting
  static ServerConfig adjustServerConfig(final ServerConfig config, final Task task)
  {
    if (Period.ZERO.equals(config.getUnannouncePropagationDelay()) || isQueryable(task)) {
      return config;
    }

    log.info(
        "Ignoring unannouncePropagationDelay[%s] since task[%s] of type[%s] does not serve queries.",
        config.getUnannouncePropagationDelay(),
        task.getId(),
        task.getType()
    );
    return config.withUnannouncePropagationDelay(Period.ZERO);
  }

  /**
   * Whether the task answers queries over its datasource. {@link Task#getQueryRunner} returns null for tasks that
   * do not, so probe it with a {@link FakeQuery} on the task's datasource.
   */
  private static boolean isQueryable(final Task task)
  {
    final String dataSource = task.getDataSource() == null ? PLACEHOLDER_DATASOURCE : task.getDataSource();
    final FakeQuery query = new FakeQuery(
        new TableDataSource(dataSource),
        new MultipleIntervalSegmentSpec(Collections.singletonList(Intervals.ETERNITY)),
        Collections.emptyMap()
    );
    return task.getQueryRunner(query) != null;
  }
}
