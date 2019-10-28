/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.segment.realtime.firehose;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import org.apache.druid.data.input.Firehose;
import org.apache.druid.data.input.FirehoseFactory;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowPlusRaw;
import org.apache.druid.data.input.impl.InputRowParser;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.utils.CloseableUtils;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Creates firehoses that shut off at a particular time. Useful for limiting the lifespan of a realtime job.
 *
 * Each {@link Firehose} created by this factory spins up and manages one thread for calling {@link Firehose#close()}
 * asynchronously at the specified {@link #shutoffTime}.
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
    return new TimedShutoffFirehose(parser, temporaryDirectory, false);
  }

  @Override
  public Firehose connectForSampler(InputRowParser parser, File temporaryDirectory) throws IOException
  {
    return new TimedShutoffFirehose(parser, temporaryDirectory, true);
  }

  class TimedShutoffFirehose implements Firehose
  {
    private final Firehose firehose;
    private final ScheduledExecutorService shutdownExec;
    @GuardedBy("this")
    private boolean closed = false;

    TimedShutoffFirehose(InputRowParser parser, File temporaryDirectory, boolean sampling) throws IOException
    {
      firehose = sampling
                 ? delegateFactory.connectForSampler(parser, temporaryDirectory)
                 : delegateFactory.connect(parser, temporaryDirectory);

      shutdownExec = Execs.scheduledSingleThreaded("timed-shutoff-firehose-%d");

      shutdownExec.schedule(
          () -> {
            log.info("Closing delegate firehose.");

            try {
              TimedShutoffFirehose.this.close();
            }
            catch (IOException e) {
              log.warn(e, "Failed to close delegate firehose, ignoring.");
            }
          },
          shutoffTime.getMillis() - System.currentTimeMillis(),
          TimeUnit.MILLISECONDS
      );

      log.info("Firehose created, will shut down at: %s", shutoffTime);
    }

    @Override
    public boolean hasMore() throws IOException
    {
      return firehose.hasMore();
    }

    @Nullable
    @Override
    public InputRow nextRow() throws IOException
    {
      return firehose.nextRow();
    }

    @Override
    public InputRowPlusRaw nextRowWithRaw() throws IOException
    {
      return firehose.nextRowWithRaw();
    }

    /**
     * This method is synchronized because it might be called concurrently from multiple threads: from {@link
     * #shutdownExec}, and explicitly on this Firehose object.
     */
    @Override
    public synchronized void close() throws IOException
    {
      if (!closed) {
        closed = true;
        CloseableUtils.closeBoth(firehose, shutdownExec::shutdownNow);
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
