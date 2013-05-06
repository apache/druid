package com.metamx.druid.realtime.firehose;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.metamx.druid.input.InputRow;
import com.metamx.emitter.EmittingLogger;
import org.joda.time.DateTime;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Creates firehoses that shut off at a particular time. Useful for limiting the lifespan of a realtime job.
 */
public class TimedShutoffFirehoseFactory implements FirehoseFactory
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
  public Firehose connect() throws IOException
  {
    return new TimedShutoffFirehose();
  }

  public class TimedShutoffFirehose implements Firehose
  {
    private final Firehose firehose;
    private final ScheduledExecutorService exec;
    private final Object shutdownLock = new Object();
    private volatile boolean shutdown = false;

    public TimedShutoffFirehose() throws IOException
    {
      firehose = delegateFactory.connect();

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
              } catch (IOException e) {
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
