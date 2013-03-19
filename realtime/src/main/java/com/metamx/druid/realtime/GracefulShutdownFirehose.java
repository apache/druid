package com.metamx.druid.realtime;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.metamx.common.concurrent.ScheduledExecutors;
import com.metamx.druid.index.v1.IndexGranularity;
import com.metamx.druid.input.InputRow;
import com.metamx.druid.realtime.plumber.IntervalRejectionPolicyFactory;
import com.metamx.druid.realtime.plumber.RejectionPolicy;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.joda.time.Period;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 */
public class GracefulShutdownFirehose implements Firehose
{
  private final Firehose firehose;
  private final IndexGranularity segmentGranularity;
  private final long windowMillis;
  private final ScheduledExecutorService scheduledExecutor;
  private final RejectionPolicy rejectionPolicy;

  private volatile boolean shutdown = false;

  public GracefulShutdownFirehose(
      Firehose firehose,
      IndexGranularity segmentGranularity,
      Period windowPeriod
  )
  {
    this.firehose = firehose;
    this.segmentGranularity = segmentGranularity;
    this.windowMillis = windowPeriod.toStandardDuration().getMillis() * 2;
    this.scheduledExecutor = Executors.newScheduledThreadPool(
        1,
        new ThreadFactoryBuilder()
            .setDaemon(true)
            .setNameFormat("firehose_scheduled_%d")
            .build()
    );

    final long truncatedNow = segmentGranularity.truncate(new DateTime()).getMillis();
    final long end = segmentGranularity.increment(truncatedNow);

    this.rejectionPolicy = new IntervalRejectionPolicyFactory(new Interval(truncatedNow, end)).create(windowPeriod);
  }

  public void shutdown() throws IOException
  {
    final long truncatedNow = segmentGranularity.truncate(new DateTime()).getMillis();

    ScheduledExecutors.scheduleWithFixedDelay(
        scheduledExecutor,
        new Duration(System.currentTimeMillis(), segmentGranularity.increment(truncatedNow) + windowMillis),
        new Callable<ScheduledExecutors.Signal>()
        {
          @Override
          public ScheduledExecutors.Signal call() throws Exception
          {
            try {
              firehose.close();
            }
            catch (Exception e) {
              throw Throwables.propagate(e);
            }

            return ScheduledExecutors.Signal.STOP;
          }
        }
    );

    shutdown = true;
  }

  @Override
  public boolean hasMore()
  {
    return firehose.hasMore();
  }

  @Override
  public InputRow nextRow()
  {
    InputRow next = firehose.nextRow();

    if (!shutdown || rejectionPolicy.accept(next.getTimestampFromEpoch())) {
      return next;
    }

    return null;
  }

  @Override
  public Runnable commit()
  {
    return firehose.commit();
  }

  @Override
  public void close() throws IOException
  {
    firehose.close();
  }
}
