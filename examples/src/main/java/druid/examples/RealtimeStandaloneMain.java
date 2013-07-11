package druid.examples;

import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.google.common.collect.ImmutableList;
import com.metamx.common.lifecycle.Lifecycle;
import com.metamx.common.logger.Logger;
import com.metamx.druid.client.DataSegment;
import com.metamx.druid.client.DruidServer;
import com.metamx.druid.client.InventoryView;
import com.metamx.druid.client.ServerView;
import com.metamx.druid.coordination.DataSegmentAnnouncer;
import com.metamx.druid.loading.DataSegmentPusher;
import com.metamx.druid.log.LogLevelAdjuster;
import com.metamx.druid.realtime.RealtimeNode;
import com.metamx.druid.realtime.SegmentPublisher;
import druid.examples.flights.FlightsFirehoseFactory;
import druid.examples.rand.RandomFirehoseFactory;
import druid.examples.twitter.TwitterSpritzerFirehoseFactory;
import druid.examples.web.WebFirehoseFactory;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Executor;

/**
 * Standalone Demo Realtime process.
 */
public class RealtimeStandaloneMain
{
  private static final Logger log = new Logger(RealtimeStandaloneMain.class);

  public static void main(String[] args) throws Exception
  {
    LogLevelAdjuster.register();

    final Lifecycle lifecycle = new Lifecycle();

    RealtimeNode rn = RealtimeNode.builder().build();
    lifecycle.addManagedInstance(rn);

    // register the Firehoses
    rn.registerJacksonSubtype(
        new NamedType(TwitterSpritzerFirehoseFactory.class, "twitzer"),
        new NamedType(FlightsFirehoseFactory.class, "flights"),
        new NamedType(RandomFirehoseFactory.class, "rand"),
        new NamedType(WebFirehoseFactory.class, "webstream")

    );

    // Create dummy objects for the various interfaces that interact with the DB, ZK and deep storage
    rn.setSegmentPublisher(new NoopSegmentPublisher());
    rn.setAnnouncer(new NoopDataSegmentAnnouncer());
    rn.setDataSegmentPusher(new NoopDataSegmentPusher());
    rn.setServerView(new NoopServerView());
    rn.setInventoryView(new NoopInventoryView());

    Runtime.getRuntime().addShutdownHook(
        new Thread(
            new Runnable()
            {
              @Override
              public void run()
              {
                log.info("Running shutdown hook");
                lifecycle.stop();
              }
            }
        )
    );

    try {
      lifecycle.start();
    }
    catch (Throwable t) {
      log.info(t, "Throwable caught at startup, committing seppuku");
      t.printStackTrace();
      System.exit(2);
    }

    lifecycle.join();
  }

  private static class NoopServerView implements ServerView
  {
    @Override
    public void registerServerCallback(
        Executor exec, ServerCallback callback
    )
    {

    }

    @Override
    public void registerSegmentCallback(
        Executor exec, SegmentCallback callback
    )
    {

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

  private static class NoopSegmentPublisher implements SegmentPublisher
  {
    @Override
    public void publishSegment(DataSegment segment) throws IOException
    {
      // do nothing
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