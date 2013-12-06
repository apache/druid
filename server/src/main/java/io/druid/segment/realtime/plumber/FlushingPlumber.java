package io.druid.segment.realtime.plumber;

import com.metamx.common.concurrent.ScheduledExecutors;
import com.metamx.emitter.EmittingLogger;
import com.metamx.emitter.service.ServiceEmitter;
import io.druid.query.QueryRunnerFactoryConglomerate;
import io.druid.segment.IndexGranularity;
import io.druid.segment.realtime.FireDepartmentMetrics;
import io.druid.segment.realtime.Schema;
import io.druid.server.coordination.DataSegmentAnnouncer;
import org.joda.time.Duration;
import org.joda.time.Period;

import java.io.File;
import java.util.Map;
import java.util.concurrent.ExecutorService;

/**
 */
public class FlushingPlumber extends RealtimePlumber
{
  private static final EmittingLogger log = new EmittingLogger(FlushingPlumber.class);

  private final Duration flushDuration;

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
    computeBaseDir(getSchema()).mkdirs();
    initializeExecutors();
    bootstrapSinksFromDisk();
    startPersistThread();
  }

  protected void flushAfterDuration(final long truncatedTime, final Sink sink)
  {
    ScheduledExecutors.scheduleWithFixedDelay(
        getScheduledExecutor(),
        flushDuration,
        new Runnable()
        {
          @Override
          public void run()
          {
            abandonSegment(truncatedTime, sink);
          }
        }

    );
  }

  @Override
  public void finishJob()
  {
    log.info("Shutting down...");

    for (final Map.Entry<Long, Sink> entry : getSinks().entrySet()) {
      flushAfterDuration(entry.getKey(), entry.getValue());
    }
    shutdownScheduledExecutor();
  }
}
