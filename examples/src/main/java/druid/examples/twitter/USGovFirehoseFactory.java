//package druid.examples.twitter;
//
//import com.fasterxml.jackson.annotation.JsonCreator;
//import com.google.common.collect.Maps;
//import com.metamx.common.logger.Logger;
//import com.metamx.druid.input.InputRow;
//import com.metamx.druid.input.MapBasedInputRow;
//import com.metamx.druid.realtime.firehose.Firehose;
//import com.metamx.druid.realtime.firehose.FirehoseFactory;
//import org.codehaus.jackson.annotate.JsonProperty;
//import org.codehaus.jackson.annotate.JsonTypeName;
//import twitter4j.ConnectionLifeCycleListener;
//import twitter4j.Status;
//
//import java.io.IOException;
//import java.util.LinkedList;
//import java.util.Map;
//import java.util.concurrent.ArrayBlockingQueue;
//import java.util.concurrent.BlockingQueue;
//import java.util.concurrent.TimeUnit;
//
//import static java.lang.Thread.sleep;
//
///**
//* Created with IntelliJ IDEA.
//* User: dhruvparthasarathy
//* Date: 6/18/13
//* Time: 4:52 PM
//* To change this template use File | Settings | File Templates.
//*/
//@JsonTypeName("Gov")
//
//public class USGovFirehoseFactory implements FirehoseFactory
//{
//  private static final Logger log = new Logger(TwitterSpritzerFirehoseFactory.class);
//
//  private final int maxEventCount;
//
//  private final int rowCount;
//
//  @JsonCreator
//  public USGovFirehoseFactory(
//      @JsonProperty("maxEventCount") Integer maxEventCount,
//      @JsonProperty("rowCount") Integer rowCount
//  )
//  {
//    this.maxEventCount=maxEventCount;
//    this.rowCount=rowCount;
//    log.info("maxEventCount=" + ((maxEventCount <= 0) ? "no limit" : maxEventCount));
//    log.info("rowCount=" + ((rowCount <= 0) ? "no limit" : rowCount));
//  }
//
//  @Override
//  public Firehose connect() throws IOException
//  {
//    final LinkedList<String> dimensions = new LinkedList<String>();
//    final int QUEUE_SIZE = 2000;
//    final BlockingQueue<Status> queue = new ArrayBlockingQueue<Status>(QUEUE_SIZE);
//    dimensions.add("device");
//    dimensions.add("country_code");
//    dimensions.add("known_user");
//    dimensions.add("base_url");
//    dimensions.add("referring_url");
//    dimensions.add("full_url");
//    dimensions.add("timestamp");
//    dimensions.add("city");
//    dimensions.add("tz");
//
//    WebListener listener = new WebListener() {
//      @Override
//      public void onUpdate(Update update)
//      {
//        if (Thread.currentThread().isInterrupted()) {
//          throw new RuntimeException("Interrupted, time to stop");
//        }
//        try {
//          boolean success = queue.offer(update, 15L, TimeUnit.SECONDS);
//          if (!success){
//            log.warn("queue too slow!");
//          }
//        }
//        catch (InterruptedException e){
//          throw new RuntimeException("InterruptedException", e);
//        }
//      }
//    };
//    return new Firehose()
//    {
//      final ConnectionLifeCycleListener connectionLifeCycleListener = new ConnectionLifeCycleListener() {
//        @Override
//        public void onConnect()
//        {
//          log.info("Connected_to_stream");
//        }
//
//        @Override
//        public void onDisconnect()
//        {
//          log.info("Disconnected_from_stream");
//        }
//
//        @Override
//        public void onCleanUp()
//        {
//          log.info("Cleanup_stream");
//        }
//      };
//      private final Runnable doNothingRunnable = new Runnable() {
//        public void run()
//        {
//        }
//      };
//      private boolean waitIfmax = true;
//      @Override
//      public boolean hasMore()
//      {
//        if (maxEventCount >=0 && rowCount >= maxEventCount){
//          return waitIfmax;
//        }
//        else
//        {
//          return true;
//        }
//      }
//
//      @Override
//      public InputRow nextRow()
//      {
//        if (maxEventCount >=0 && rowCount >=maxEventCount && waitIfmax){
//          try {
//            sleep(2000000000L);
//          }
//          catch (InterruptedException e) {
//            throw new RuntimeException("InterruptedException");
//          }
//        }
//        Update udpate;
//        try{
//          update=queue.take();
//        }
//        catch (InterruptedException e) {
//          throw new RuntimeException("InterruptedException", e);
//        }
//        final Map<String, Object> theMap = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
//        theMap.put("device", update.device());
//        theMap.put("country_code", update.country());
//        theMap.put("known_user", update.knownUser());
//        theMap.put("base_url", update.baseUrl());
//        theMap.put("referring_url", update.referringUrl());
//        theMap.put("full_url", update.fullUrl());
//        theMap.put("timestamp", update.timestamp());
//        theMap.put("city", update.city());
//        theMap.put("tz", update.tz());
//
//        return new MapBasedInputRow(update.timestamp,dimensions,theMap);  //To change body of implemented methods use File | Settings | File Templates.
//      }
//
//      @Override
//      public Runnable commit()
//      {
//        return doNothingRunnable;  //To change body of implemented methods use File | Settings | File Templates.
//      }
//
//      @Override
//      public void close() throws IOException
//      {
//        //To change body of implemented methods use File | Settings | File Templates.
//      }
//
//      };
//
//  }
//}
