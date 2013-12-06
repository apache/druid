package io.druid.segment.realtime.plumber;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.metamx.common.concurrent.ScheduledExecutors;
import com.metamx.emitter.EmittingLogger;
import com.metamx.emitter.service.ServiceEmitter;
import io.druid.common.guava.ThreadRenamingCallable;
import io.druid.query.QueryRunnerFactoryConglomerate;
import io.druid.segment.IndexGranularity;
import io.druid.segment.realtime.FireDepartmentMetrics;
import io.druid.segment.realtime.Schema;
import io.druid.server.coordination.DataSegmentAnnouncer;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Period;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 */
public class FlushingPlumber extends RealtimePlumber
{
  private static final EmittingLogger log = new EmittingLogger(FlushingPlumber.class);

  private final Duration flushDuration;

  private volatile ScheduledExecutorService flushScheduledExec = null;
  private volatile boolean stopped = false;

  public FlushingPlumber(
      Duration flushDuration,
      Period windowPeriod,
      File basePersistDirectory,
      IndexGranularity segmentGranularity,
      Schema schema,
      FireDepartmentMetrics metrics,
      RejectionPolicy rejectionPolicy,
      ServiceEmitter emitter,
      QueryRunnerFactoryConglomerate conglomerate,
      DataSegmentAnnouncer segmentAnnouncer,
      ExecutorService queryExecutorService,
      VersioningPolicy versioningPolicy
  )
  {
    super(
        windowPeriod,
        basePersistDirectory,
        segmentGranularity,
        schema,
        metrics,
        rejectionPolicy,
        emitter,
        conglomerate,
        segmentAnnouncer,
        queryExecutorService,
        versioningPolicy,
        null,
        null,
        null
    );

    this.flushDuration = flushDuration;
  }

  @Override
  public void startJob()
  {
    log.info("Starting job for %s", getSchema().getDataSource());

    computeBaseDir(getSchema()).mkdirs();
    initializeExecutors();

    if (flushScheduledExec == null) {
      flushScheduledExec = Executors.newScheduledThreadPool(
          1,
          new ThreadFactoryBuilder()
              .setDaemon(true)
              .setNameFormat("flushing_scheduled_%d")
              .build()
      );
    }

    bootstrapSinksFromDisk();
    startFlushThread();
  }

  protected void flushAfterDuration(final long truncatedTime, final Sink sink)
  {
    log.info(
        "Abandoning segment %s at %s",
        sink.getSegment().getIdentifier(),
        new DateTime().plusMillis(flushDuration.toPeriod().getMillis())
    );

    ScheduledExecutors.scheduleWithFixedDelay(
        flushScheduledExec,
        flushDuration,
        new Callable<ScheduledExecutors.Signal>()
        {
          @Override
          public ScheduledExecutors.Signal call() throws Exception
          {
            log.info("Abandoning segment %s", sink.getSegment().getIdentifier());
            abandonSegment(truncatedTime, sink);
            return ScheduledExecutors.Signal.STOP;
          }
        }
    );
  }

  private void startFlushThread()
  {
    final long truncatedNow = getSegmentGranularity().truncate(new DateTime()).getMillis();
    final long windowMillis = getWindowPeriod().toStandardDuration().getMillis();

    log.info(
        "Expect to run at [%s]",
        new DateTime().plus(
            new Duration(System.currentTimeMillis(), getSegmentGranularity().increment(truncatedNow) + windowMillis)
        )
    );

    ScheduledExecutors
        .scheduleAtFixedRate(
            flushScheduledExec,
            new Duration(System.currentTimeMillis(), getSegmentGranularity().increment(truncatedNow) + windowMillis),
            new Duration(truncatedNow, getSegmentGranularity().increment(truncatedNow)),
            new ThreadRenamingCallable<ScheduledExecutors.Signal>(
                String.format(
                    "%s-flusher-%d",
                    getSchema().getDataSource(),
                    getSchema().getShardSpec().getPartitionNum()
                )
            )
            {
              @Override
              public ScheduledExecutors.Signal doCall()
              {
                if (stopped) {
                  log.info("Stopping flusher thread");
                  return ScheduledExecutors.Signal.STOP;
                }

                long minTimestamp = getSegmentGranularity().truncate(
                    getRejectionPolicy().getCurrMaxTime().minus(windowMillis)
                ).getMillis();

                List<Map.Entry<Long, Sink>> sinksToPush = Lists.newArrayList();
                for (Map.Entry<Long, Sink> entry : getSinks().entrySet()) {
                  final Long intervalStart = entry.getKey();
                  if (intervalStart < minTimestamp) {
                    log.info("Adding entry[%s] to flush.", entry);
                    sinksToPush.add(entry);
                  }
                }

                for (final Map.Entry<Long, Sink> entry : sinksToPush) {
                  flushAfterDuration(entry.getKey(), entry.getValue());
                }

                if (stopped) {
                  log.info("Stopping flusher thread");
                  return ScheduledExecutors.Signal.STOP;
                } else {
                  return ScheduledExecutors.Signal.REPEAT;
                }
              }
            }
        );
  }

  @Override
  public void finishJob()
  {
    log.info("Stopping job");

    for (final Map.Entry<Long, Sink> entry : getSinks().entrySet()) {
      flushAfterDuration(entry.getKey(), entry.getValue());
    }
    shutdownExecutors();

    if (flushScheduledExec != null) {
      flushScheduledExec.shutdown();
    }

    stopped = true;
  }
}
