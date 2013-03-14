package com.metamx.druid.realtime;

import com.google.common.collect.ImmutableMap;
import com.metamx.common.lifecycle.LifecycleStart;
import com.metamx.common.lifecycle.LifecycleStop;
import com.metamx.common.logger.Logger;
import com.metamx.druid.client.DataSegment;
import com.metamx.phonebook.PhoneBook;
import org.joda.time.DateTime;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

public class ZkSegmentAnnouncer implements SegmentAnnouncer
{
  private static final Logger log = new Logger(ZkSegmentAnnouncer.class);

  private final Object lock = new Object();

  private final ZkSegmentAnnouncerConfig config;
  private final PhoneBook yp;
  private final String servedSegmentsLocation;

  private volatile boolean started = false;

  public ZkSegmentAnnouncer(
      ZkSegmentAnnouncerConfig config,
      PhoneBook yp
  )
  {
    this.config = config;
    this.yp = yp;
    this.servedSegmentsLocation = yp.combineParts(
        Arrays.asList(
            config.getServedSegmentsLocation(), config.getServerName()
        )
    );
  }

  public Map<String, String> getStringProps()
  {
    return ImmutableMap.of(
        "name", config.getServerName(),
        "host", config.getHost(),
        "maxSize", String.valueOf(config.getMaxSize()),
        "type", "realtime"
    );
  }

  @LifecycleStart
  public void start()
  {
    synchronized (lock) {
      if (started) {
        return;
      }

      log.info("Starting zkCoordinator for server[%s] with config[%s]", config.getServerName(), config);
      if (yp.lookup(servedSegmentsLocation, Object.class) == null) {
        yp.post(
            config.getServedSegmentsLocation(),
            config.getServerName(),
            ImmutableMap.of("created", new DateTime().toString())
        );
      }

      yp.announce(
          config.getAnnounceLocation(),
          config.getServerName(),
          getStringProps()
      );

      started = true;
    }
  }

  @LifecycleStop
  public void stop()
  {
    synchronized (lock) {
      if (!started) {
        return;
      }

      log.info("Stopping ZkSegmentAnnouncer with config[%s]", config);
      yp.unannounce(config.getAnnounceLocation(), config.getServerName());

      started = false;
    }
  }

  public void announceSegment(DataSegment segment) throws IOException
  {
    log.info("Announcing realtime segment %s", segment.getIdentifier());
    yp.announce(servedSegmentsLocation, segment.getIdentifier(), segment);
  }

  public void unannounceSegment(DataSegment segment) throws IOException
  {
    log.info("Unannouncing realtime segment %s", segment.getIdentifier());
    yp.unannounce(servedSegmentsLocation, segment.getIdentifier());
  }
}
