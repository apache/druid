package com.metamx.druid.guice;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.metamx.druid.concurrent.Execs;
import com.metamx.druid.coordination.BatchDataSegmentAnnouncer;
import com.metamx.druid.coordination.DataSegmentAnnouncer;
import com.metamx.druid.coordination.DataSegmentAnnouncerProvider;
import com.metamx.druid.coordination.SingleDataSegmentAnnouncer;
import com.metamx.druid.curator.announcement.Announcer;
import com.metamx.druid.initialization.BatchDataSegmentAnnouncerConfig;
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
    binder.bind(BatchDataSegmentAnnouncer.class).in(ManageLifecycleLast.class);
    binder.bind(SingleDataSegmentAnnouncer.class).in(ManageLifecycleLast.class);
  }

  @Provides
  @ManageLifecycle
  public Announcer getAnnouncer(CuratorFramework curator)
  {
    return new Announcer(curator, Execs.singleThreaded("Announcer-%s"));
  }
}
