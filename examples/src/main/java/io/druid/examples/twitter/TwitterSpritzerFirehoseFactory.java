/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.examples.twitter;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.collect.Lists;
import com.metamx.common.logger.Logger;
import io.druid.data.input.Firehose;
import io.druid.data.input.FirehoseFactory;
import io.druid.data.input.InputRow;
import io.druid.data.input.MapBasedInputRow;
import io.druid.data.input.impl.InputRowParser;
import twitter4j.ConnectionLifeCycleListener;
import twitter4j.HashtagEntity;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.User;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import static java.lang.Thread.sleep;

/**
 * Twitter "spritzer" Firehose Factory named "twitzer".
 * Builds a Firehose that emits a stream of
 * ??
 * with timestamps along with ??.
 * The generated tuples have the form (timestamp, ????)
 * where the timestamp is from the twitter event.
 *
 * Example spec file:
 *
 * Example query using POST to /druid/v2/?w  (where w is an arbitrary parameter and the date and time
 * is UTC):
 *
 * Notes on twitter.com HTTP (REST) API: v1.0 will be disabled around 2013-03 so v1.1 should be used;
 * twitter4j 3.0 (not yet released) will support the v1.1 api.
 * Specifically, we should be using https://stream.twitter.com/1.1/statuses/sample.json
 * See: http://jira.twitter4j.org/browse/TFJ-186
 *
 *  Notes on JSON parsing: as of twitter4j 2.2.x, the json parser has some bugs (ex: Status.toString()
 *  can have number format exceptions), so it might be necessary to extract raw json and process it
 *  separately.  If so, set twitter4.jsonStoreEnabled=true and look at DataObjectFactory#getRawJSON();
 *  com.fasterxml.jackson.databind.ObjectMapper should be used to parse.
 * @author pbaclace
 */
@JsonTypeName("twitzer")
public class TwitterSpritzerFirehoseFactory implements FirehoseFactory<InputRowParser> {
  private static final Logger log = new Logger(TwitterSpritzerFirehoseFactory.class);
  /**
   * max events to receive, -1 is infinite, 0 means nothing is delivered; use this to prevent
   * infinite space consumption or to prevent getting throttled at an inconvenient time
   * or to see what happens when a Firehose stops delivering
   * values, or to have hasMore() return false.  The Twitter Spritzer can deliver about
   * 1000 events per minute.
   */
  private final int maxEventCount;
  /**
   * maximum number of minutes to fetch Twitter events.  Use this to prevent getting
   * throttled at an inconvenient time. If zero or less, no time limit for run.
   */
  private final int maxRunMinutes;

  @JsonCreator
  public TwitterSpritzerFirehoseFactory(
      @JsonProperty("maxEventCount") Integer maxEventCount,
      @JsonProperty("maxRunMinutes") Integer maxRunMinutes
  )
  {
    this.maxEventCount = maxEventCount;
    this.maxRunMinutes = maxRunMinutes;
    log.info("maxEventCount=" + ((maxEventCount <= 0) ? "no limit" : maxEventCount));
    log.info("maxRunMinutes=" + ((maxRunMinutes <= 0) ? "no limit" : maxRunMinutes));
  }

  @Override
  public Firehose connect(InputRowParser parser) throws IOException
  {
    final ConnectionLifeCycleListener connectionLifeCycleListener = new ConnectionLifeCycleListener() {
      @Override
      public void onConnect()
      {
        log.info("Connected_to_Twitter");
      }

      @Override
      public void onDisconnect()
      {
        log.info("Disconnect_from_Twitter");
      }

      /**
       * called before thread gets cleaned up
       */
      @Override
      public void onCleanUp()
      {
        log.info("Cleanup_twitter_stream");
      }
    }; // ConnectionLifeCycleListener

    final TwitterStream twitterStream;
    final StatusListener statusListener;
    final int QUEUE_SIZE = 2000;
    /** This queue is used to move twitter events from the twitter4j thread to the druid ingest thread.   */
    final BlockingQueue<Status> queue = new ArrayBlockingQueue<Status>(QUEUE_SIZE);
    final LinkedList<String> dimensions = new LinkedList<String>();
    final long startMsec = System.currentTimeMillis();

    dimensions.add("htags");
    dimensions.add("lang");
    dimensions.add("utc_offset");

    //
    //   set up Twitter Spritzer
    //
    twitterStream = new TwitterStreamFactory().getInstance();
    twitterStream.addConnectionLifeCycleListener(connectionLifeCycleListener);
    statusListener = new StatusListener() {  // This is what really gets called to deliver stuff from twitter4j
      @Override
      public void onStatus(Status status)
      {
        // time to stop?
        if (Thread.currentThread().isInterrupted()) {
          throw new RuntimeException("Interrupted, time to stop");
        }
        try {
          boolean success = queue.offer(status, 15L, TimeUnit.SECONDS);
          if (!success) {
            log.warn("queue too slow!");
          }
        } catch (InterruptedException e) {
          throw new RuntimeException("InterruptedException", e);
        }
      }

      @Override
      public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice)
      {
        //log.info("Got a status deletion notice id:" + statusDeletionNotice.getStatusId());
      }

      @Override
      public void onTrackLimitationNotice(int numberOfLimitedStatuses)
      {
        // This notice will be sent each time a limited stream becomes unlimited.
        // If this number is high and or rapidly increasing, it is an indication that your predicate is too broad, and you should consider a predicate with higher selectivity.
        log.warn("Got track limitation notice:" + numberOfLimitedStatuses);
      }

      @Override
      public void onScrubGeo(long userId, long upToStatusId)
      {
        //log.info("Got scrub_geo event userId:" + userId + " upToStatusId:" + upToStatusId);
      }

      @Override
      public void onException(Exception ex)
      {
        ex.printStackTrace();
      }

      @Override
      public void onStallWarning(StallWarning warning) {
        System.out.println("Got stall warning:" + warning);
      }
    };

    twitterStream.addListener(statusListener);
    twitterStream.sample(); // creates a generic StatusStream
    log.info("returned from sample()");

    return new Firehose() {

      private final Runnable doNothingRunnable = new Runnable() {
        public void run()
        {
        }
      };

      private long rowCount = 0L;
      private boolean waitIfmax = (maxEventCount < 0L);
      private final Map<String, Object> theMap = new HashMap<String, Object>(2);
      // DIY json parsing // private final ObjectMapper omapper = new ObjectMapper();

      private boolean maxTimeReached()
      {
        if (maxRunMinutes <= 0) {
          return false;
        } else {
          return (System.currentTimeMillis() - startMsec) / 60000L >= maxRunMinutes;
        }
      }

      private boolean maxCountReached()
      {
        return maxEventCount >= 0 && rowCount >= maxEventCount;
      }

      @Override
      public boolean hasMore()
      {
        if (maxCountReached() || maxTimeReached()) {
          return waitIfmax;
        } else {
          return true;
        }
      }

      @Override
      public InputRow nextRow()
      {
        // Interrupted to stop?
        if (Thread.currentThread().isInterrupted()) {
          throw new RuntimeException("Interrupted, time to stop");
        }

        // all done?
        if (maxCountReached() || maxTimeReached()) {
          if (waitIfmax) {
            // sleep a long time instead of terminating
            try {
              log.info("reached limit, sleeping a long time...");
              sleep(2000000000L);
            } catch (InterruptedException e) {
              throw new RuntimeException("InterruptedException", e);
            }
          } else {
            // allow this event through, and the next hasMore() call will be false
          }
        }
        if (++rowCount % 1000 == 0) {
          log.info("nextRow() has returned %,d InputRows", rowCount);
        }

        Status status;
        try {
          status = queue.take();
        } catch (InterruptedException e) {
          throw new RuntimeException("InterruptedException", e);
        }

        HashtagEntity[] hts = status.getHashtagEntities();
        if (hts != null && hts.length > 0) {
          List<String> hashTags = Lists.newArrayListWithExpectedSize(hts.length);
          for (HashtagEntity ht : hts) {
            hashTags.add(ht.getText());
          }

          theMap.put("htags", Arrays.asList(hashTags.get(0)));
        }

        long retweetCount = status.getRetweetCount();
        theMap.put("retweet_count", retweetCount);
        User user = status.getUser();
        if (user != null) {
          theMap.put("follower_count", user.getFollowersCount());
          theMap.put("friends_count", user.getFriendsCount());
          theMap.put("lang", user.getLang());
          theMap.put("utc_offset", user.getUtcOffset());  // resolution in seconds, -1 if not available?
          theMap.put("statuses_count", user.getStatusesCount());
        }

        return new MapBasedInputRow(status.getCreatedAt().getTime(), dimensions, theMap);
      }

      @Override
      public Runnable commit()
      {
        // ephemera in, ephemera out.
        return doNothingRunnable; // reuse the same object each time
      }

      @Override
      public void close() throws IOException
      {
        log.info("CLOSE twitterstream");
        twitterStream.shutdown(); // invokes twitterStream.cleanUp()
      }
    };
  }

  @Override
  public InputRowParser getParser()
  {
    return null;
  }
}
