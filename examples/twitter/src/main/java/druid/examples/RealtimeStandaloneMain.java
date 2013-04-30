package druid.examples;

import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.metamx.common.lifecycle.Lifecycle;
import com.metamx.common.logger.Logger;
import com.metamx.druid.client.DataSegment;
import com.metamx.druid.coordination.DataSegmentAnnouncer;
import com.metamx.druid.loading.DataSegmentPusher;
import com.metamx.druid.log.LogLevelAdjuster;
import com.metamx.druid.realtime.RealtimeNode;
import com.metamx.druid.realtime.SegmentPublisher;
import druid.examples.twitter.TwitterSpritzerFirehoseFactory;

import java.io.File;
import java.io.IOException;

/** Standalone Demo Realtime process.
 * Created: 20121009T2050
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
    // register the Firehose
    rn.registerJacksonSubtype(new NamedType(TwitterSpritzerFirehoseFactory.class, "twitzer"));

    final DataSegmentAnnouncer dummySegmentAnnouncer =
      new DataSegmentAnnouncer()
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
      };
    SegmentPublisher dummySegmentPublisher =
        new SegmentPublisher()
        {
          @Override
          public void publishSegment(DataSegment segment) throws IOException
          {
            // do nothing
          }
        };

    // dummySegmentPublisher will not send updates to db because standalone demo has no db
    rn.setSegmentPublisher(dummySegmentPublisher);
    rn.setAnnouncer(dummySegmentAnnouncer);
    rn.setDataSegmentPusher(
        new DataSegmentPusher()
        {
          @Override
          public DataSegment push(File file, DataSegment segment) throws IOException
          {
            return segment;
          }
        }
    );

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
}