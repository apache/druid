package io.druid.segment.realtime.plumber;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.metamx.common.concurrent.ScheduledExecutors;
import com.metamx.emitter.EmittingLogger;
import com.metamx.emitter.service.ServiceEmitter;
import io.druid.query.QueryRunnerFactoryConglomerate;
import io.druid.segment.IndexGranularity;
import io.druid.segment.realtime.FireDepartmentMetrics;
import io.druid.segment.realtime.Schema;
import io.druid.server.coordination.DataSegmentAnnouncer;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Period;

import java.io.File;
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

  private final ScheduledExecutorService flushScheduledExec = Executors.newScheduledThreadPool(
      1,
      new ThreadFactoryBuilder()
          .setDaemon(true)
          .setNameFormat("flushing_scheduled_%d")
          .build()
  );

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
    log.info("Starting job");

    computeBaseDir(getSchema()).mkdirs();
    initializeExecutors();
    bootstrapSinksFromDisk();
    startPersistThread();
  }

  protected void flushAfterDuration(final long truncatedTime, final Sink sink)
  {
    log.info("Abandoning segment at %s", new DateTime().plusMillis(flushDuration.toPeriod().getMillis()));
    
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

  @Override
  public void finishJob()
  {
    log.info("Stopping job");

    for (final Map.Entry<Long, Sink> entry : getSinks().entrySet()) {
      flushAfterDuration(entry.getKey(), entry.getValue());
    }
    shutdownExecutors();
  }
}
