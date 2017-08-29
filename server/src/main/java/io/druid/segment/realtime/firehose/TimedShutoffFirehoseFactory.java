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

package io.druid.segment.realtime.firehose;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.metamx.emitter.EmittingLogger;
import io.druid.concurrent.Execs;
import io.druid.data.input.Firehose;
import io.druid.data.input.FirehoseFactory;
import io.druid.data.input.InputRow;
import io.druid.data.input.impl.InputRowParser;
import org.joda.time.DateTime;

import java.io.File;
import java.io.IOException;
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
  public Firehose connect(InputRowParser parser, File temporaryDirectory) throws IOException
  {
    return new TimedShutoffFirehose(parser, temporaryDirectory);
  }

  class TimedShutoffFirehose implements Firehose
  {
    private final Firehose firehose;
    private final ScheduledExecutorService exec;
    private final Object shutdownLock = new Object();
    private volatile boolean shutdown = false;

    TimedShutoffFirehose(InputRowParser parser, File temporaryDirectory) throws IOException
    {
      firehose = delegateFactory.connect(parser, temporaryDirectory);

      exec = Execs.scheduledSingleThreaded("timed-shutoff-firehose-%d");

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
