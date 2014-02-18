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

package io.druid.segment.realtime.firehose;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.metamx.emitter.EmittingLogger;
import io.druid.data.input.Firehose;
import io.druid.data.input.FirehoseFactory;
import io.druid.data.input.InputRow;
import io.druid.data.input.impl.InputRowParser;
import org.joda.time.DateTime;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Creates firehoses that shut off at a particular time. Useful for limiting the lifespan of a realtime job.
 */
public class TimedShutoffFirehoseFactory implements FirehoseFactory<InputRowParser>
{
  private static final EmittingLogger log = new EmittingLogger(FirehoseFactory.class);
  private final FirehoseFactory delegateFactory;
  private final DateTime shutoffTime;

  @JsonCreator
  public TimedShutoffFirehoseFactory(
      @JsonProperty("delegate") FirehoseFactory delegateFactory,
      @JsonProperty("shutoffTime") DateTime shutoffTime
  )
  {
    this.delegateFactory = delegateFactory;
    this.shutoffTime = shutoffTime;
  }

  @Override
  public Firehose connect(InputRowParser parser) throws IOException
  {
    return new TimedShutoffFirehose(parser);
  }

  @Override
  public InputRowParser getParser()
  {
    return delegateFactory.getParser();
  }

  public class TimedShutoffFirehose implements Firehose
  {
    private final Firehose firehose;
    private final ScheduledExecutorService exec;
    private final Object shutdownLock = new Object();
    private volatile boolean shutdown = false;

    public TimedShutoffFirehose(InputRowParser parser) throws IOException
    {
      firehose = delegateFactory.connect(parser);

      exec = Executors.newScheduledThreadPool(
          1,
          new ThreadFactoryBuilder().setDaemon(true)
                                    .setNameFormat("timed-shutoff-firehose-%d")
                                    .build()
      );

      exec.schedule(
          new Runnable()
          {
            @Override
            public void run()
            {
              log.info("Closing delegate firehose.");

              shutdown = true;
              try {
                firehose.close();
              }
              catch (IOException e) {
                log.warn(e, "Failed to close delegate firehose, ignoring.");
              }
            }
          },
          shutoffTime.getMillis() - System.currentTimeMillis(),
          TimeUnit.MILLISECONDS
      );

      log.info("Firehose created, will shut down at: %s", shutoffTime);
    }

    @Override
    public boolean hasMore()
    {
      return firehose.hasMore();
    }

    @Override
    public InputRow nextRow()
    {
      return firehose.nextRow();
    }

    @Override
    public Runnable commit()
    {
      return firehose.commit();
    }

    @Override
    public void close() throws IOException
    {
      synchronized (shutdownLock) {
        if (!shutdown) {
          shutdown = true;
          firehose.close();
        }
      }
    }
  }

  @JsonProperty("delegate")
  public FirehoseFactory getDelegateFactory()
  {
    return delegateFactory;
  }

  @JsonProperty("shutoffTime")
  public DateTime getShutoffTime()
  {
    return shutoffTime;
  }
}
