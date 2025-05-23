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

import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Module;
import com.google.inject.Provides;
import org.apache.curator.framework.CuratorFramework;
import org.apache.druid.curator.CuratorConfig;
import org.apache.druid.curator.ZkEnablementConfig;
import org.apache.druid.curator.announcement.NodeAnnouncer;
import org.apache.druid.curator.announcement.PathChildrenAnnouncer;
import org.apache.druid.curator.announcement.ServiceAnnouncer;
import org.apache.druid.guice.annotations.DirectExecutorAnnouncer;
import org.apache.druid.guice.annotations.SingleThreadedAnnouncer;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.server.coordination.BatchDataSegmentAnnouncer;
import org.apache.druid.server.coordination.CuratorDataSegmentServerAnnouncer;
import org.apache.druid.server.coordination.DataSegmentAnnouncer;
import org.apache.druid.server.coordination.DataSegmentAnnouncerProvider;
import org.apache.druid.server.coordination.DataSegmentServerAnnouncer;
import org.apache.druid.server.initialization.BatchDataSegmentAnnouncerConfig;

import java.util.Properties;

/**
 */
public class AnnouncerModule implements Module
{
  private boolean isZkEnabled = true;

  @Inject
  public void configure(Properties properties)
  {
    isZkEnabled = ZkEnablementConfig.isEnabled(properties);
  }

  @Override
  public void configure(Binder binder)
  {
    JsonConfigProvider.bind(binder, "druid.announcer", BatchDataSegmentAnnouncerConfig.class);
    JsonConfigProvider.bind(binder, "druid.announcer", DataSegmentAnnouncerProvider.class);
    binder.bind(DataSegmentAnnouncer.class).toProvider(DataSegmentAnnouncerProvider.class);
    binder.bind(BatchDataSegmentAnnouncer.class).in(ManageLifecycleAnnouncements.class);

    if (isZkEnabled) {
      binder.bind(DataSegmentServerAnnouncer.class).to(CuratorDataSegmentServerAnnouncer.class).in(LazySingleton.class);
    } else {
      binder.bind(DataSegmentServerAnnouncer.class).to(DataSegmentServerAnnouncer.Noop.class).in(LazySingleton.class);
    }
  }

  @Provides
  @SingleThreadedAnnouncer
  @ManageLifecycleAnnouncements
  public ServiceAnnouncer getAnnouncerWithSingleThreadedExecutorService(CuratorFramework curator, CuratorConfig config)
  {
    boolean usingPathChildrenCacheAnnouncer = config.getPathChildrenCacheStrategy();
    if (usingPathChildrenCacheAnnouncer) {
      return new PathChildrenAnnouncer(curator, Execs.singleThreaded("Announcer-%s"));
    } else {
      return new NodeAnnouncer(curator, Execs.singleThreaded("Announcer-%s"));
    }
  }

  @Provides
  @DirectExecutorAnnouncer
  @ManageLifecycleAnnouncements
  public ServiceAnnouncer getAnnouncerWithDirectExecutorService(CuratorFramework curator, CuratorConfig config)
  {
    boolean usingPathChildrenCacheAnnouncer = config.getPathChildrenCacheStrategy();
    if (usingPathChildrenCacheAnnouncer) {
      return new PathChildrenAnnouncer(curator, Execs.directExecutor());
    } else {
      return new NodeAnnouncer(curator, Execs.directExecutor());
    }
  }
}
