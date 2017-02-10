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

package io.druid.indexing.common.task;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.primitives.Ints;
import com.metamx.emitter.EmittingLogger;
import io.druid.data.input.Committer;
import io.druid.data.input.Firehose;
import io.druid.data.input.FirehoseFactory;
import io.druid.data.input.InputRow;
import io.druid.indexing.appenderator.ActionBasedSegmentAllocator;
import io.druid.indexing.appenderator.ActionBasedUsedSegmentChecker;
import io.druid.indexing.appenderator.AppenderatorCreator;
import io.druid.indexing.common.TaskStatus;
import io.druid.indexing.common.TaskToolbox;
import io.druid.indexing.common.actions.SegmentTransactionalInsertAction;
import io.druid.indexing.common.actions.TaskActionClient;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.guava.Sequence;
import io.druid.query.NoopQueryRunner;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.segment.realtime.FireDepartment;
import io.druid.segment.realtime.FireDepartmentMetrics;
import io.druid.segment.realtime.SegmentPublisher;
import io.druid.segment.realtime.appenderator.Appenderator;
import io.druid.segment.realtime.appenderator.FiniteAppenderatorDriver;
import io.druid.segment.realtime.appenderator.SegmentsAndMetadata;
import io.druid.segment.realtime.appenderator.TransactionalSegmentPublisher;
import io.druid.segment.realtime.firehose.ClippedFirehoseFactory;
import io.druid.segment.realtime.firehose.EventReceiverFirehoseFactory;
import io.druid.segment.realtime.firehose.TimedShutoffFirehoseFactory;
import io.druid.segment.realtime.plumber.Plumber;
import io.druid.timeline.DataSegment;
import org.apache.commons.codec.digest.DigestUtils;
import org.joda.time.DateTime;

import java.io.IOException;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.RejectedExecutionException;

/**
 * Similar to {@link RealtimeIndexTask} but works on top of {@link io.druid.segment.realtime.appenderator.Appenderator}
 * instead of {@link Plumber}
 * <p/>
 * Accepts same specification as {@link RealtimeIndexTask} so that it is easy to switch between the two types just by
 * changing the type property in the indexing specification
 */

public class AppenderatorIndexTask extends AbstractTask
{
  private static final EmittingLogger log = new EmittingLogger(AppenderatorIndexTask.class);
  private final static Random random = new Random();

  private static String makeTaskId(FireDepartment fireDepartment)
  {
    return makeTaskId(
        fireDepartment.getDataSchema().getDataSource(),
        fireDepartment.getTuningConfig().getShardSpec().getPartitionNum(),
        new DateTime(),
        random.nextInt()
    );
  }

  static String makeTaskId(String dataSource, int partitionNumber, DateTime timestamp, int randomBits)
  {
    final StringBuilder suffix = new StringBuilder(8);
    for (int i = 0; i < Ints.BYTES * 2; ++i) {
      suffix.append((char) ('a' + ((randomBits >>> (i * 4)) & 0x0F)));
    }
    return String.format(
        "index_appenderator_%s_%d_%s_%s",
        dataSource,
        partitionNumber,
        timestamp,
        suffix
    );
  }

  private static String makeDatasource(FireDepartment fireDepartment)
  {
    return fireDepartment.getDataSchema().getDataSource();
  }

  @JsonIgnore
  private final FireDepartment spec;

  @JsonIgnore
  private volatile Firehose firehose = null;

  @JsonIgnore
  private volatile FireDepartmentMetrics metrics = null;

  @JsonIgnore
  private volatile boolean gracefullyStopped = false;

  @JsonIgnore
  private volatile boolean finishingJob = false;

  @JsonIgnore
  private volatile Thread runThread = null;

  private volatile Appenderator appenderator = null;
  private volatile AppenderatorCreator appenderatorCreator = null;

  @JsonCreator
  public AppenderatorIndexTask(
      @JsonProperty("id") String id,
      @JsonProperty("resource") TaskResource taskResource,
      @JsonProperty("spec") FireDepartment fireDepartment,
      @JsonProperty("context") Map<String, Object> context,
      @JsonProperty("appenderator") AppenderatorCreator appenderatorCreator
  )
  {
    super(
        id == null ? makeTaskId(fireDepartment) : id,
        String.format("index_realtime_%s", makeDatasource(fireDepartment)),
        taskResource,
        makeDatasource(fireDepartment),
        context
    );
    this.spec = fireDepartment;
    this.metrics = new FireDepartmentMetrics();
    Preconditions.checkNotNull(appenderatorCreator, "No appenderator found!");
    this.appenderatorCreator= appenderatorCreator;
  }

  @Override
  public String getType()
  {
    return "appenderator";
  }

  @Override
  public String getNodeType()
  {
    // TODO need to check where this is used
    return null;
  }

  @Override
  public <T> QueryRunner<T> getQueryRunner(Query<T> query)
  {
    if (appenderator == null) {
      return new NoopQueryRunner<>();
    }
    return new QueryRunner<T>()
    {
      @Override
      public Sequence<T> run(
          Query<T> query, Map<String, Object> responseContext
      )
      {
        return query.run(appenderator, responseContext);
      }
    };
  }

  @Override
  public boolean isReady(TaskActionClient taskActionClient) throws Exception
  {
    return true;
  }

  @Override
  public TaskStatus run(final TaskToolbox toolbox) throws Exception
  {
    final Supplier<Committer> committerSupplier = new Supplier<Committer>()
    {
      @Override
      public Committer get()
      {
        return new Committer()
        {
          @Override
          public Object getMetadata()
          {
            return null;
          }

          @Override
          public void run()
          {

          }
        };
      }
    };

    final String dataSchema = toolbox.getObjectMapper().writeValueAsString(spec.getDataSchema());
    final String tuningConfig = toolbox.getObjectMapper().writeValueAsString(spec.getIOConfig());
    final String ioConfig = toolbox.getObjectMapper().writeValueAsString(spec.getTuningConfig());

    final String sequenceName = DigestUtils.sha1Hex(dataSchema + tuningConfig + ioConfig).substring(0, 15);

    try (
        final Appenderator appenderator0 = appenderatorCreator.build(
            spec.getDataSchema(),
            spec.getTuningConfig(),
            metrics,
            toolbox
        );
        final FiniteAppenderatorDriver driver = makeDriver(appenderator0, toolbox)
    ) {
      driver.startJob();
      appenderator = appenderator0;
      final Firehose firehose = spec.connect();
      while (firehose.hasMore()) {
        final InputRow row = firehose.nextRow();
        driver.add(row, sequenceName, committerSupplier);
      }
      final SegmentsAndMetadata published =
          driver.finish(
              new TransactionalSegmentPublisher()
              {
                @Override
                public boolean publishSegments(Set<DataSegment> segments, Object commitMetadata) throws IOException
                {
                  final SegmentTransactionalInsertAction insertAction = new SegmentTransactionalInsertAction(
                      segments,
                      null,
                      null
                  );
                  log.info("Publishing segments [%s]", segments);
                  return toolbox.getTaskActionClient().submit(insertAction).isSuccess();
                }
              },
              committerSupplier.get()
          );
      if (published == null) {
        throw new ISE("Transaction failure publishing segments, aborting");
      } else {
        log.info(
            "Published segments[%s] with metadata[%s].",
            Joiner.on(", ").join(
                Iterables.transform(
                    published.getSegments(),
                    new Function<DataSegment, String>()
                    {
                      @Override
                      public String apply(DataSegment input)
                      {
                        return input.getIdentifier();
                      }
                    }
                )
            ),
            published.getCommitMetadata()
        );
      }
    }
    catch (InterruptedException | RejectedExecutionException e) {
      // handle the InterruptedException that gets wrapped in a RejectedExecutionException
      if (e instanceof RejectedExecutionException
          && (e.getCause() == null || !(e.getCause() instanceof InterruptedException))) {
        throw e;
      }

      log.info("The task was asked to stop before completing");
    }
    return TaskStatus.success(getId());
  }

  private FiniteAppenderatorDriver makeDriver(Appenderator appenderator, TaskToolbox toolbox)
  {
    return new FiniteAppenderatorDriver(
        appenderator,
        new ActionBasedSegmentAllocator(toolbox.getTaskActionClient(), spec.getDataSchema()),
        toolbox.getSegmentHandoffNotifierFactory(),
        new ActionBasedUsedSegmentChecker(toolbox.getTaskActionClient()),
        toolbox.getObjectMapper(),
        spec.getTuningConfig().getMaxRowsInMemory(),
        spec.getTuningConfig().getHandoffConditionTimeout()
    );
  }

  @Override
  public boolean canRestore()
  {
    return false;
  }

  @Override
  public void stopGracefully()
  {
    try {
      synchronized (this) {
        if (!gracefullyStopped) {
          gracefullyStopped = true;
          if (firehose == null) {
            log.info("stopGracefully: Firehose not started yet, so nothing to stop.");
          } else if (finishingJob) {
            log.info("stopGracefully: Interrupting finishJob.");
            runThread.interrupt();
          } else if (isFirehoseDrainableByClosing(spec.getIOConfig().getFirehoseFactory())) {
            log.info("stopGracefully: Draining firehose.");
            firehose.close();
          } else {
            log.info("stopGracefully: Cannot drain firehose by closing, interrupting run thread.");
            runThread.interrupt();
          }
        }
      }
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * Public for tests.
   */
  @JsonIgnore
  public Firehose getFirehose()
  {
    return firehose;
  }

  /**
   * Public for tests.
   */
  @JsonIgnore
  public FireDepartmentMetrics getMetrics()
  {
    return metrics;
  }

  @JsonProperty("spec")
  public FireDepartment getRealtimeIngestionSchema()
  {
    return spec;
  }

  @JsonProperty("appenderator")
  public AppenderatorCreator getAppenderatorCreator()
  {
    return appenderatorCreator;
  }

  /**
   * Is a firehose from this factory drainable by closing it? If so, we should drain on stopGracefully rather than
   * abruptly stopping.
   * <p/>
   * This is a hack to get around the fact that the Firehose and FirehoseFactory interfaces do not help us do this.
   * <p/>
   * Protected for tests.
   */
  protected boolean isFirehoseDrainableByClosing(FirehoseFactory firehoseFactory)
  {
    return firehoseFactory instanceof EventReceiverFirehoseFactory
           || (firehoseFactory instanceof TimedShutoffFirehoseFactory
               && isFirehoseDrainableByClosing(((TimedShutoffFirehoseFactory) firehoseFactory).getDelegateFactory()))
           || (firehoseFactory instanceof ClippedFirehoseFactory
               && isFirehoseDrainableByClosing(((ClippedFirehoseFactory) firehoseFactory).getDelegate()));
  }

  public static class TaskActionSegmentPublisher implements SegmentPublisher
  {
    final Task task;
    final TaskToolbox taskToolbox;

    public TaskActionSegmentPublisher(Task task, TaskToolbox taskToolbox)
    {
      this.task = task;
      this.taskToolbox = taskToolbox;
    }

    @Override
    public void publishSegment(DataSegment segment) throws IOException
    {
      taskToolbox.publishSegments(ImmutableList.of(segment));
    }
  }
}
