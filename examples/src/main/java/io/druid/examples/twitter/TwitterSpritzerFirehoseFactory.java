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

package io.druid.examples.twitter;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.druid.data.input.Firehose;
import io.druid.data.input.FirehoseFactory;
import io.druid.data.input.InputRow;
import io.druid.data.input.MapBasedInputRow;
import io.druid.data.input.impl.InputRowParser;
import io.druid.java.util.common.logger.Logger;
import io.druid.java.util.common.StringUtils;
import twitter4j.ConnectionLifeCycleListener;
import twitter4j.GeoLocation;
import twitter4j.HashtagEntity;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.User;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Twitter "spritzer" Firehose Factory named "twitzer".
 * Builds a Firehose that emits a stream of
 * ??
 * with timestamps along with ??.
 * The generated tuples have the form (timestamp, ????)
 * where the timestamp is from the twitter event.
 * <p/>
 * Example spec file:
 * <p/>
 * Example query using POST to /druid/v2/?w  (where w is an arbitrary parameter and the date and time
 * is UTC):
 * <p/>
 * Notes on twitter.com HTTP (REST) API: v1.0 will be disabled around 2013-03 so v1.1 should be used;
 * twitter4j 3.0 (not yet released) will support the v1.1 api.
 * Specifically, we should be using https://stream.twitter.com/1.1/statuses/sample.json
 * See: http://jira.twitter4j.org/browse/TFJ-186
 * <p/>
 * Notes on JSON parsing: as of twitter4j 2.2.x, the json parser has some bugs (ex: Status.toString()
 * can have number format exceptions), so it might be necessary to extract raw json and process it
 * separately.  If so, set twitter4.jsonStoreEnabled=true and look at DataObjectFactory#getRawJSON();
 * com.fasterxml.jackson.databind.ObjectMapper should be used to parse.
 *
 * @author pbaclace
 */
@JsonTypeName("twitzer")
public class TwitterSpritzerFirehoseFactory implements FirehoseFactory<InputRowParser>
{
  private static final Logger log = new Logger(TwitterSpritzerFirehoseFactory.class);
  private static final Pattern sourcePattern = Pattern.compile("<a[^>]*>(.*?)</a>", Pattern.CASE_INSENSITIVE);

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
  public Firehose connect(InputRowParser parser, File temporaryDirectory) throws IOException
  {
    final ConnectionLifeCycleListener connectionLifeCycleListener = new ConnectionLifeCycleListener()
    {
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
    final long startMsec = System.currentTimeMillis();

    //
    //   set up Twitter Spritzer
    //
    twitterStream = new TwitterStreamFactory().getInstance();
    twitterStream.addConnectionLifeCycleListener(connectionLifeCycleListener);
    statusListener = new StatusListener()
    {  // This is what really gets called to deliver stuff from twitter4j
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
        }
        catch (InterruptedException e) {
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
        log.error(ex, "Got exception");
      }

      @Override
      public void onStallWarning(StallWarning warning)
      {
        log.warn("Got stall warning: %s", warning);
      }
    };

    twitterStream.addListener(statusListener);
    twitterStream.sample(); // creates a generic StatusStream
    log.info("returned from sample()");

    return new Firehose()
    {

      private final Runnable doNothingRunnable = new Runnable()
      {
        @Override
        public void run()
        {
        }
      };

      private long rowCount = 0L;
      private boolean waitIfmax = (getMaxEventCount() < 0L);
      private final Map<String, Object> theMap = new TreeMap<>();
      // DIY json parsing // private final ObjectMapper omapper = new ObjectMapper();

      private boolean maxTimeReached()
      {
        if (getMaxRunMinutes() <= 0) {
          return false;
        } else {
          return (System.currentTimeMillis() - startMsec) / 60000L >= getMaxRunMinutes();
        }
      }

      private boolean maxCountReached()
      {
        return getMaxEventCount() >= 0 && rowCount >= getMaxEventCount();
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
              Thread.sleep(2000000000L);
            }
            catch (InterruptedException e) {
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
        }
        catch (InterruptedException e) {
          throw new RuntimeException("InterruptedException", e);
        }

        theMap.clear();

        HashtagEntity[] hts = status.getHashtagEntities();
        String text = status.getText();
        theMap.put("text", (null == text) ? "" : text);
        theMap.put(
            "htags", (hts.length > 0) ? Lists.transform(
                Arrays.asList(hts), new Function<HashtagEntity, String>()
                {
                  @Nullable
                  @Override
                  public String apply(HashtagEntity input)
                  {
                    return input.getText();
                  }
                }
            ) : ImmutableList.<String>of()
        );

        long[] lcontrobutors = status.getContributors();
        List<String> contributors = new ArrayList<>();
        for (long contrib : lcontrobutors) {
          contributors.add(StringUtils.format("%d", contrib));
        }
        theMap.put("contributors", contributors);

        GeoLocation geoLocation = status.getGeoLocation();
        if (null != geoLocation) {
          double lat = status.getGeoLocation().getLatitude();
          double lon = status.getGeoLocation().getLongitude();
          theMap.put("lat", lat);
          theMap.put("lon", lon);
        } else {
          theMap.put("lat", null);
          theMap.put("lon", null);
        }

        if (status.getSource() != null) {
          Matcher m = sourcePattern.matcher(status.getSource());
          theMap.put("source", m.find() ? m.group(1) : status.getSource());
        }

        theMap.put("retweet", status.isRetweet());

        if (status.isRetweet()) {
          Status original = status.getRetweetedStatus();
          theMap.put("retweet_count", original.getRetweetCount());

          User originator = original.getUser();
          theMap.put("originator_screen_name", originator != null ? originator.getScreenName() : "");
          theMap.put("originator_follower_count", originator != null ? originator.getFollowersCount() : "");
          theMap.put("originator_friends_count", originator != null ? originator.getFriendsCount() : "");
          theMap.put("originator_verified", originator != null ? originator.isVerified() : "");
        }

        User user = status.getUser();
        final boolean hasUser = (null != user);
        theMap.put("follower_count", hasUser ? user.getFollowersCount() : 0);
        theMap.put("friends_count", hasUser ? user.getFriendsCount() : 0);
        theMap.put("lang", hasUser ? user.getLang() : "");
        theMap.put("utc_offset", hasUser ? user.getUtcOffset() : -1);  // resolution in seconds, -1 if not available?
        theMap.put("statuses_count", hasUser ? user.getStatusesCount() : 0);
        theMap.put("user_id", hasUser ? StringUtils.format("%d", user.getId()) : "");
        theMap.put("screen_name", hasUser ? user.getScreenName() : "");
        theMap.put("location", hasUser ? user.getLocation() : "");
        theMap.put("verified", hasUser ? user.isVerified() : "");

        theMap.put("ts",status.getCreatedAt().getTime());

        List<String> dimensions = Lists.newArrayList(theMap.keySet());

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

  @JsonProperty
  public int getMaxEventCount()
  {
    return maxEventCount;
  }

  @JsonProperty
  public int getMaxRunMinutes()
  {
    return maxRunMinutes;
  }
}
