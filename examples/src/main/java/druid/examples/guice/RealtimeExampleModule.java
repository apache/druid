package druid.examples.guice;

import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.TypeLiteral;
import com.metamx.common.logger.Logger;
import com.metamx.druid.client.DataSegment;
import com.metamx.druid.client.DruidServer;
import com.metamx.druid.client.InventoryView;
import com.metamx.druid.client.ServerView;
import com.metamx.druid.coordination.DataSegmentAnnouncer;
import com.metamx.druid.guice.FireDepartmentsProvider;
import com.metamx.druid.guice.JsonConfigProvider;
import com.metamx.druid.guice.ManageLifecycle;
import com.metamx.druid.guice.NoopSegmentPublisherProvider;
import com.metamx.druid.guice.RealtimeManagerConfig;
import com.metamx.druid.loading.DataSegmentPusher;
import com.metamx.druid.realtime.FireDepartment;
import com.metamx.druid.realtime.RealtimeManager;
import com.metamx.druid.realtime.SegmentPublisher;
import druid.examples.flights.FlightsFirehoseFactory;
import druid.examples.rand.RandomFirehoseFactory;
import druid.examples.twitter.TwitterSpritzerFirehoseFactory;
import druid.examples.web.WebFirehoseFactory;
import io.druid.initialization.DruidModule;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executor;

/**
 */
public class RealtimeExampleModule implements DruidModule
{
  private static final Logger log = new Logger(RealtimeExampleModule.class);

  @Override
  public void configure(Binder binder)
  {
    binder.bind(SegmentPublisher.class).toProvider(NoopSegmentPublisherProvider.class);
    binder.bind(DataSegmentPusher.class).to(NoopDataSegmentPusher.class);
    binder.bind(DataSegmentAnnouncer.class).to(NoopDataSegmentAnnouncer.class);
    binder.bind(InventoryView.class).to(NoopInventoryView.class);
    binder.bind(ServerView.class).to(NoopServerView.class);

    JsonConfigProvider.bind(binder, "druid.realtime", RealtimeManagerConfig.class);
    binder.bind(
        new TypeLiteral<List<FireDepartment>>()
        {
        }
    ).toProvider(FireDepartmentsProvider.class);
    binder.bind(RealtimeManager.class).in(ManageLifecycle.class);
  }

  @Override
  public List<com.fasterxml.jackson.databind.Module> getJacksonModules()
  {
    return Arrays.<com.fasterxml.jackson.databind.Module>asList(
        new SimpleModule("RealtimeExampleModule")
            .registerSubtypes(
                new NamedType(TwitterSpritzerFirehoseFactory.class, "twitzer"),
                new NamedType(FlightsFirehoseFactory.class, "flights"),
                new NamedType(RandomFirehoseFactory.class, "rand"),
                new NamedType(WebFirehoseFactory.class, "webstream")
            )
    );
  }

  private static class NoopServerView implements ServerView
  {
    @Override
    public void registerServerCallback(
        Executor exec, ServerCallback callback
    )
    {
      // do nothing
    }

    @Override
    public void registerSegmentCallback(
        Executor exec, SegmentCallback callback
    )
    {
      // do nothing
    }
  }

  private static class NoopInventoryView implements InventoryView
  {
    @Override
    public DruidServer getInventoryValue(String string)
    {
      return null;
    }

    @Override
    public Iterable<DruidServer> getInventory()
    {
      return ImmutableList.of();
    }
  }

  private static class NoopDataSegmentPusher implements DataSegmentPusher
  {
    @Override
    public DataSegment push(File file, DataSegment segment) throws IOException
    {
      return segment;
    }
  }

  private static class NoopDataSegmentAnnouncer implements DataSegmentAnnouncer
  {
    @Override
    public void announceSegment(DataSegment segment) throws IOException
    {
      // do nothing
    }

    @Override
    public void unannounceSegment(DataSegment segment) throws IOException
    {
      // do nothing
    }

    @Override
    public void announceSegments(Iterable<DataSegment> segments) throws IOException
    {
      // do nothing
    }

    @Override
    public void unannounceSegments(Iterable<DataSegment> segments) throws IOException
    {
      // do nothing
    }
  }
}
