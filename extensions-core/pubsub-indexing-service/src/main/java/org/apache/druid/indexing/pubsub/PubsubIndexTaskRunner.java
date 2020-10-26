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

package org.apache.druid.indexing.pubsub;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.pubsub.v1.ReceivedMessage;
import org.apache.druid.data.input.Committer;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.impl.InputRowParser;
import org.apache.druid.discovery.DiscoveryDruidNode;
import org.apache.druid.discovery.LookupNodeService;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.StreamChunkParser;
import org.apache.druid.indexing.common.LockGranularity;
import org.apache.druid.indexing.common.TaskLockType;
import org.apache.druid.indexing.common.TaskRealtimeMetricsMonitorBuilder;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.actions.SegmentLockAcquireAction;
import org.apache.druid.indexing.common.actions.TimeChunkLockAcquireAction;
import org.apache.druid.indexing.common.stats.RowIngestionMeters;
import org.apache.druid.indexing.common.stats.RowIngestionMetersFactory;
import org.apache.druid.indexing.common.task.IndexTaskUtils;
import org.apache.druid.indexing.common.task.RealtimeIndexTask;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.collect.Utils;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.segment.indexing.RealtimeIOConfig;
import org.apache.druid.segment.realtime.FireDepartment;
import org.apache.druid.segment.realtime.FireDepartmentMetrics;
import org.apache.druid.segment.realtime.appenderator.Appenderator;
import org.apache.druid.segment.realtime.appenderator.AppenderatorDriverAddResult;
import org.apache.druid.segment.realtime.appenderator.AppenderatorsManager;
import org.apache.druid.segment.realtime.appenderator.SegmentsAndCommitMetadata;
import org.apache.druid.segment.realtime.appenderator.StreamAppenderatorDriver;
import org.apache.druid.segment.realtime.firehose.ChatHandler;
import org.apache.druid.segment.realtime.firehose.ChatHandlerProvider;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.utils.CircularBuffer;
import org.joda.time.DateTime;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

/**
 * Pubsub indexing task runner
 */
public class PubsubIndexTaskRunner implements ChatHandler
{
  private static final EmittingLogger log = new EmittingLogger(PubsubIndexTaskRunner.class);
  protected final Lock pollRetryLock = new ReentrantLock();
  private final PubsubIndexTaskIOConfig ioConfig;
  private final PubsubIndexTaskTuningConfig tuningConfig;
  private final PubsubIndexTask task;
  private final InputRowParser<ByteBuffer> parser;
  private final AuthorizerMapper authorizerMapper;
  private final Optional<ChatHandlerProvider> chatHandlerProvider;
  private final CircularBuffer<Throwable> savedParseExceptions;
  private final RowIngestionMetersFactory rowIngestionMetersFactory;
  private final AppenderatorsManager appenderatorsManager;
  private final LockGranularity lockGranularityToUse;
  private final InputRowSchema inputRowSchema;
  private final InputFormat inputFormat;
  private final RowIngestionMeters rowIngestionMeters;

  //TODO
  private volatile TaskToolbox toolbox;
  private volatile Appenderator appenderator;
  private volatile StreamAppenderatorDriver driver;
  private final String sequenceName;
  private volatile DateTime startTime = DateTimes.MAX;
  private volatile boolean isFinalized = false;

  PubsubIndexTaskRunner(
      PubsubIndexTask task,
      @Nullable InputRowParser<ByteBuffer> parser,
      AuthorizerMapper authorizerMapper,
      Optional<ChatHandlerProvider> chatHandlerProvider,
      CircularBuffer<Throwable> savedParseExceptions,
      RowIngestionMetersFactory rowIngestionMetersFactory,
      AppenderatorsManager appenderatorsManager,
      LockGranularity lockGranularityToUse
  )
  {
    this.task = task;
    this.ioConfig = task.getIOConfig();
    this.tuningConfig = task.getTuningConfig();
    this.parser = parser;
    this.authorizerMapper = authorizerMapper;
    this.chatHandlerProvider = chatHandlerProvider;
    this.savedParseExceptions = savedParseExceptions;
    this.rowIngestionMetersFactory = rowIngestionMetersFactory;
    this.appenderatorsManager = appenderatorsManager;
    this.lockGranularityToUse = lockGranularityToUse;
    this.inputFormat = ioConfig.getInputFormat(parser == null ? null : parser.getParseSpec());
    this.rowIngestionMeters = rowIngestionMetersFactory.createRowIngestionMeters();

    this.inputRowSchema = new InputRowSchema(
        task.getDataSchema().getTimestampSpec(),
        task.getDataSchema().getDimensionsSpec(),
        Arrays.stream(task.getDataSchema().getAggregators())
              .map(AggregatorFactory::getName)
              .collect(Collectors.toList())
    );
    this.sequenceName = DateTimes.nowUtc() + "-seq";
  }

  public TaskStatus run(TaskToolbox toolbox)
  {
    try {
      log.info("running pubsub task");
      return runInternal(toolbox);
    }
    catch (Exception e) {
      log.error(e, "Encountered exception while running task.");
      final String errorMsg = Throwables.getStackTraceAsString(e);
      return TaskStatus.failure(
          task.getId(),
          errorMsg
      );
    }
  }

  @Nonnull
  protected List<ReceivedMessage> getRecords(
      PubsubRecordSupplier recordSupplier,
      TaskToolbox toolbox
  ) throws Exception
  {
    // Handles OffsetOutOfRangeException, which is thrown if the seeked-to
    // offset is not present in the topic-partition. This can happen if we're asking a task to read from data
    // that has not been written yet (which is totally legitimate). So let's wait for it to show up.
    List<ReceivedMessage> records = new ArrayList<>();
    try {
      records = recordSupplier.poll(task.getIOConfig().getPollTimeout());
    }
    catch (Exception e) {
      log.warn("OffsetOutOfRangeException with message [%s]", e.getMessage());
    }

    return records;
  }

  @VisibleForTesting
  public void setToolbox(TaskToolbox toolbox)
  {
    this.toolbox = toolbox;
  }

  private TaskStatus runInternal(TaskToolbox toolbox) throws Exception
  {
    startTime = DateTimes.nowUtc();
    setToolbox(toolbox);
    PubsubRecordSupplier recordSupplier = task.newTaskRecordSupplier();

    // Now we can initialize StreamChunkReader with the given toolbox.
    final StreamChunkParser parser = new StreamChunkParser(
        this.parser,
        inputFormat,
        inputRowSchema,
        task.getDataSchema().getTransformSpec(),
        toolbox.getIndexingTmpDir()
    );

    if (chatHandlerProvider.isPresent()) {
      log.debug("prefix: Found chat handler of class[%s] registering [%s]", chatHandlerProvider.get().getClass().getName(), task.getId());
      chatHandlerProvider.get().register(task.getId(), this, true);
    } else {
      log.warn("prefix: No chat handler detected.");
    }

    // Set up FireDepartmentMetrics
    final FireDepartment fireDepartmentForMetrics = new FireDepartment(
        task.getDataSchema(),
        new RealtimeIOConfig(null, null),
        null
    );
    FireDepartmentMetrics fireDepartmentMetrics = fireDepartmentForMetrics.getMetrics();
    toolbox.getMonitorScheduler()
           .addMonitor(TaskRealtimeMetricsMonitorBuilder.build(task, fireDepartmentForMetrics, rowIngestionMeters));

    final String lookupTier = task.getContextValue(RealtimeIndexTask.CTX_KEY_LOOKUP_TIER);
    final LookupNodeService lookupNodeService = lookupTier == null ?
                                                toolbox.getLookupNodeService() :
                                                new LookupNodeService(lookupTier);

    final DiscoveryDruidNode discoveryDruidNode = new DiscoveryDruidNode(
        toolbox.getDruidNode(),
        NodeRole.PEON,
        ImmutableMap.of(
            toolbox.getDataNodeService().getName(), toolbox.getDataNodeService(),
            lookupNodeService.getName(), lookupNodeService
        )
    );

    if (appenderatorsManager.shouldTaskMakeNodeAnnouncements()) {
      toolbox.getDataSegmentServerAnnouncer().announce();
      toolbox.getDruidNodeAnnouncer().announce(discoveryDruidNode);
    }
    appenderator = task.newAppenderator(fireDepartmentMetrics, toolbox);
    driver = task.newDriver(appenderator, toolbox, fireDepartmentMetrics);
    driver.startJob(
        segmentId -> {
          try {
            if (lockGranularityToUse == LockGranularity.SEGMENT) {
              return toolbox.getTaskActionClient().submit(
                  new SegmentLockAcquireAction(
                      TaskLockType.EXCLUSIVE,
                      segmentId.getInterval(),
                      segmentId.getVersion(),
                      segmentId.getShardSpec().getPartitionNum(),
                      1000L
                  )
              ).isOk();
            } else {
              return toolbox.getTaskActionClient().submit(
                  new TimeChunkLockAcquireAction(
                      TaskLockType.EXCLUSIVE,
                      segmentId.getInterval(),
                      1000L
                  )
              ) != null;
            }
          }
          catch (IOException e) {
            throw new RuntimeException(e);
          }
        }
    );

    // Set up committer.
    final Supplier<Committer> committerSupplier = () -> {
      return new Committer()
      {
        @Override
        public Object getMetadata()
        {
          // Do nothing.
          return ImmutableMap.of();
        }

        @Override
        public void run()
        {
          // Do nothing.
        }
      };
    };
    while (!isFinalized) {
      boolean isPersistRequired = false;
      List<ReceivedMessage> records = getRecords(recordSupplier, toolbox);
      for (ReceivedMessage record : records) {
        final List<byte[]> valueBytess = Collections.singletonList(record.getMessage().getData().toByteArray());
        final List<InputRow> rows;
        if (valueBytess == null || valueBytess.isEmpty()) {
          rows = Utils.nullableListOf((InputRow) null);
        } else {
          rows = parser.parse(valueBytess);
        }
        log.debug("got %s rows from pubsub", rows.size());
        for (InputRow row : rows) {
          final AppenderatorDriverAddResult addResult = driver.add(
              row,
              getSequenceName(),
              committerSupplier,
              true,
              // do not allow incremental persists to happen until all the rows from this batch
              // of rows are indexed
              false
          );
          if (addResult.isOk()) {
            isPersistRequired |= addResult.isPersistRequired();
          } else {
            throw new ISE("failed to add row %s", row);
          }
        }
      }
      if (isPersistRequired) {
        driver.persist(committerSupplier.get());
      }
    }
    driver.persist(committerSupplier.get());
    publishAndRegisterHandoff(committerSupplier.get());
    driver.close();
    if (chatHandlerProvider.isPresent()) {
      chatHandlerProvider.get().unregister(task.getId());
    }
    return TaskStatus.success(task.getId());
  }

  public String getSequenceName()
  {
    return sequenceName;
  }

  private void publishAndRegisterHandoff(Committer committer)
  {
    final ListenableFuture<SegmentsAndCommitMetadata> publishFuture = Futures.transform(
        driver.publish(
            new PubsubTransactionalSegmentPublisher(this, toolbox, false),
            committer,
            Arrays.asList(getSequenceName())
        ),
        publishedSegmentsAndCommitMetadata -> {
          if (publishedSegmentsAndCommitMetadata == null) {
            throw new ISE(
                "Transaction failure publishing segments for sequence"
            );
          } else {
            return publishedSegmentsAndCommitMetadata;
          }
        }
    );

    // Create a handoffFuture for every publishFuture. The created handoffFuture must fail if publishFuture fails.
    final SettableFuture<SegmentsAndCommitMetadata> handoffFuture = SettableFuture.create();

    Futures.addCallback(
        publishFuture,
        new FutureCallback<SegmentsAndCommitMetadata>()
        {
          @Override
          public void onSuccess(SegmentsAndCommitMetadata publishedSegmentsAndCommitMetadata)
          {
            Futures.transform(
                driver.registerHandoff(publishedSegmentsAndCommitMetadata),
                new Function<SegmentsAndCommitMetadata, Void>()
                {
                  @Nullable
                  @Override
                  public Void apply(@Nullable SegmentsAndCommitMetadata handoffSegmentsAndCommitMetadata)
                  {
                    if (handoffSegmentsAndCommitMetadata == null) {
                      log.warn(
                          "Failed to hand off segments: %s",
                          String.join(
                              ", ",
                              Lists.transform(publishedSegmentsAndCommitMetadata.getSegments(), DataSegment::toString)
                          )
                      );
                    }
                    handoffFuture.set(handoffSegmentsAndCommitMetadata);
                    return null;
                  }
                }
            );
          }

          @Override
          public void onFailure(Throwable t)
          {
            log.error(t, "Error while publishing segments for sequenceNumber");
            handoffFuture.setException(t);
          }
        }
    );

    try {
      publishFuture.get();
      handoffFuture.get();
    }
    catch (InterruptedException e) {
      log.error(e, "pubsub error");
    }
    catch (ExecutionException e) {
      log.error(e, "pubsub error");
    }
  }

  @GET
  @Path("/time/start")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getStartTime(@Context final HttpServletRequest req)
  {
    IndexTaskUtils.datasourceAuthorizationCheck(req, Action.READ, task.getDataSource(), authorizerMapper);
    return Response.ok(startTime).build();
  }

  @POST
  @Path("/finalize")
  public Response finalize(@Context final HttpServletRequest req)
  {
    IndexTaskUtils.datasourceAuthorizationCheck(req, Action.WRITE, task.getDataSource(), authorizerMapper);
    isFinalized = true;
    return Response.status(Response.Status.OK).build();
  }
}

