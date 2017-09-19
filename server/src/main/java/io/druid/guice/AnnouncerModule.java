/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.guice;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import io.druid.concurrent.Execs;
import io.druid.curator.announcement.Announcer;
import io.druid.server.coordination.BatchDataSegmentAnnouncer;
import io.druid.server.coordination.CuratorDataSegmentServerAnnouncer;
import io.druid.server.coordination.DataSegmentAnnouncer;
import io.druid.server.coordination.DataSegmentAnnouncerProvider;
import io.druid.server.coordination.DataSegmentServerAnnouncer;
import io.druid.server.initialization.BatchDataSegmentAnnouncerConfig;
import org.apache.curator.framework.CuratorFramework;

/**
 */
public class AnnouncerModule implements Module
{
  @Override
  public void configure(Binder binder)
  {
    JsonConfigProvider.bind(binder, "druid.announcer", BatchDataSegmentAnnouncerConfig.class);
    JsonConfigProvider.bind(binder, "druid.announcer", DataSegmentAnnouncerProvider.class);
    binder.bind(DataSegmentAnnouncer.class).toProvider(DataSegmentAnnouncerProvider.class);
    binder.bind(BatchDataSegmentAnnouncer.class).in(LazySingleton.class);
    binder.bind(DataSegmentServerAnnouncer.class).to(CuratorDataSegmentServerAnnouncer.class).in(LazySingleton.class);
  }

  @Provides
  @ManageLifecycle
  public Announcer getAnnouncer(CuratorFramework curator)
  {
    return new Announcer(curator, Execs.singleThreaded("Announcer-%s"));
  }
}
