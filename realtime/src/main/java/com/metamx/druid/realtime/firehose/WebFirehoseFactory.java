package com.metamx.druid.realtime.firehose;

import com.metamx.druid.input.InputRow;
import com.metamx.druid.input.MapBasedInputRow;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
* Created with IntelliJ IDEA.
* User: dhruvparthasarathy
* Date: 6/20/13
* Time: 2:03 PM
* To change this template use File | Settings | File Templates.
*/
public class WebFirehoseFactory implements FirehoseFactory
{

  @Override
  public Firehose connect() throws IOException
  {
    final UpdateListener updateListener;
    final int QUEUE_SIZE=2000;
    final BlockingQueue<Map<String,Object>> queue= new ArrayBlockingQueue<Map<String,Object>>(QUEUE_SIZE);
    final LinkedList<String> dimensions = new LinkedList<String>();

    dimensions.add("BITLY_HASH");
    dimensions.add("LATUTUDE-LONGITUDE");
    dimensions.add("COUNTRTY_CODE");
    dimensions.add("USER_AGENT");
    dimensions.add("CITY");
    dimensions.add("ENCODING_USER_LOGIN");
    dimensions.add("SHORT_URL_CNAME");
    dimensions.add("TIMESTAMP OF TIME HASH WAS CREATED");
    dimensions.add("ENCODING_USER_BITLY_HASH");
    dimensions.add("LONG_URL");
    dimensions.add("TIMEZONE");
    dimensions.add("TIMESTAMP OF TIME HASH WAS CREATED");
    dimensions.add("REFERRING_URL");
    dimensions.add("GEO_REGION");
    dimensions.add("KNOWN_USER");

    UpdateStream updateStream = new UpdateStream(queue);
    updateStream.start();

    return new Firehose() {
      private final Map<String, Object> theMap = new HashMap<String, Object>();

      private final Runnable doNothingRunnable = new Runnable() {
        public void run(){

        }
      };

      @Override
      public boolean hasMore(){
        if (queue.size()>0){
          return true;
        }
        return false;
      }

      @Override
      public InputRow nextRow()
      {
        if (Thread.currentThread().isInterrupted()) {
          throw new RuntimeException("Interrupted, time to stop");
        }
        Map<String,Object> update;
        try{
          update=queue.take();
        }
        catch (InterruptedException e) {
          throw new RuntimeException("InterrutpedException", e);
        }

        return new MapBasedInputRow(((Long) update.get("TIMESTAMP")).intValue(),dimensions,update);
      }
    };
  }
}
