/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
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

package com.metamx.druid.realtime.firehose;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.metamx.common.concurrent.ScheduledExecutors;
import com.metamx.common.logger.Logger;
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
import java.util.concurrent.atomic.AtomicBoolean;

/**
 */
public class GracefulShutdownFirehose implements Firehose
{
  private static final Logger log = new Logger(GracefulShutdownFirehose.class);

  private final Firehose firehose;
  private final IndexGranularity segmentGranularity;
  private final long windowMillis;
  private final ScheduledExecutorService scheduledExecutor;
  private final RejectionPolicy rejectionPolicy;

  // when this is set to false, the firehose will have no more rows
  private final AtomicBoolean valveOn = new AtomicBoolean(true);

  // when this is set to true, the firehose will begin rejecting events
  private volatile boolean beginRejectionPolicy = false;

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
    final long end = segmentGranularity.increment(truncatedNow) + windowMillis;
    final Duration timeUntilShutdown = new Duration(System.currentTimeMillis(), end);

    log.info("Shutdown at approx. %s (in %s)", new DateTime(end), timeUntilShutdown);

    ScheduledExecutors.scheduleWithFixedDelay(
        scheduledExecutor,
        timeUntilShutdown,
        new Callable<ScheduledExecutors.Signal>()
        {
          @Override
          public ScheduledExecutors.Signal call() throws Exception
          {
            try {
              valveOn.set(false);
            }
            catch (Exception e) {
              throw Throwables.propagate(e);
            }

            return ScheduledExecutors.Signal.STOP;
          }
        }
    );

    beginRejectionPolicy = true;
  }

  @Override
  public boolean hasMore()
  {
    return valveOn.get() && firehose.hasMore();
  }

  @Override
  public InputRow nextRow()
  {
    InputRow next = firehose.nextRow();

    if (!beginRejectionPolicy || rejectionPolicy.accept(next.getTimestampFromEpoch())) {
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
