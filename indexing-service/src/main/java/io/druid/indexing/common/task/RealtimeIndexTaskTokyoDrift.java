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

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.collect.Queues;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import com.metamx.common.Granularity;
import com.metamx.common.IAE;
import com.metamx.common.ISE;
import com.metamx.common.guava.Sequence;
import com.metamx.common.logger.Logger;
import io.druid.common.utils.JodaUtils;
import io.druid.concurrent.Execs;
import io.druid.data.input.Committer;
import io.druid.data.input.InputRow;
import io.druid.data.input.impl.InputRowParser;
import io.druid.indexing.common.TaskLock;
import io.druid.indexing.common.TaskStatus;
import io.druid.indexing.common.TaskToolbox;
import io.druid.indexing.common.actions.LockTryAcquireAction;
import io.druid.indexing.common.actions.SegmentAllocateAction;
import io.druid.indexing.common.actions.SegmentInsertAction;
import io.druid.indexing.common.actions.TaskActionClient;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.segment.indexing.DataSchema;
import io.druid.segment.indexing.RealtimeTuningConfig;
import io.druid.segment.realtime.FireDepartmentMetrics;
import io.druid.segment.realtime.appenderator.Appenderator;
import io.druid.segment.realtime.appenderator.Appenderators;
import io.druid.segment.realtime.appenderator.SegmentIdentifier;
import io.druid.segment.realtime.appenderator.SegmentNotWritableException;
import io.druid.segment.realtime.appenderator.SegmentsAndMetadata;
import io.druid.segment.realtime.appenderator.ServerViewWatcher;
import io.druid.segment.realtime.firehose.ChatHandler;
import io.druid.segment.realtime.firehose.ChatHandlerProvider;
import io.druid.segment.realtime.plumber.Committers;
import io.druid.server.coordination.DruidServerMetadata;
import io.druid.timeline.DataSegment;
import org.eclipse.jetty.util.ConcurrentHashSet;
import org.joda.time.DateTime;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Random;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Push-based realtime task that uses Appenderators to write data.
 */
public class RealtimeIndexTaskTokyoDrift extends AbstractTask
{
  private static final String TYPE = "index_realtime_tokyo_drift";
  private static final Logger log = new Logger(RealtimeIndexTaskTokyoDrift.class);
  private static final Random random = new Random();
  private static final long POLL_TIMEOUT = 100;
  private static final int DEFAULT_BUFFER_SIZE = 100000;
  private static final int MAX_SEGMENT_ROWS = 10000;
  private static final int MAX_SEGMENTS = 5;

  private final InputRowParser<Map<String, Object>> parser;
  private final int slotNum;
  private final DataSchema dataSchema;
  private final RealtimeTuningConfig tuningConfig;
  private final ChatHandlerProvider chatHandlerProvider;
  private final ListeningExecutorService publishExecutor;
  private final AtomicLong currentlyPublishing = new AtomicLong(0L);
  private final Committer committer = Committers.nil();
  private final Supplier<Committer> committerSupplier = Suppliers.ofInstance(committer);
  private final RequestBuffer buffer;

  // Keys = Starts of segment intervals. Values = Active segments (currently adding data to). Only populated on leaders.
  private final NavigableMap<Long, ActiveSegment> activeSegments = new TreeMap<>();

  // SegmentIdentifiers we are currently trying to publish.
  private final ConcurrentHashSet<SegmentIdentifier> outgoingSegments = new ConcurrentHashSet<>();

  // Next handoff is at this time.
  private volatile long nextHandoffTime = JodaUtils.MAX_INSTANT;

  private volatile Appenderator appenderator = null;
  private volatile long currentEpoch = 0;
  private volatile boolean leading = false;
  private volatile boolean stopped = false;

  @JsonCreator
  public RealtimeIndexTaskTokyoDrift(
      @JsonProperty("id") String id,
      @JsonProperty("resource") TaskResource taskResource,
      @JsonProperty("maxBufferSize") Integer maxBufferSize,
      @JsonProperty("slotNum") Integer slotNum,
      @JsonProperty("dataSchema") DataSchema dataSchema,
      @JsonProperty("tuningConfig") RealtimeTuningConfig tuningConfig,
      @JsonProperty("context") Map<String, Object> context,
      @JacksonInject ChatHandlerProvider chatHandlerProvider
  )
  {
    super(
        id == null ? makeTaskId(dataSchema.getDataSource(), slotNum, random.nextInt()) : id,
        String.format("%s_%s", TYPE, dataSchema.getDataSource()),
        taskResource,
        dataSchema.getDataSource(),
        context
    );

    this.parser = Preconditions.checkNotNull((InputRowParser<Map<String, Object>>) dataSchema.getParser(), "parser");
    this.buffer = new RequestBuffer(maxBufferSize == null ? DEFAULT_BUFFER_SIZE : maxBufferSize);
    this.slotNum = Preconditions.checkNotNull(slotNum, "slotNum");
    this.dataSchema = Preconditions.checkNotNull(dataSchema, "dataSchema");
    this.tuningConfig = Preconditions.checkNotNull(tuningConfig, "tuningConfig");
    this.chatHandlerProvider = chatHandlerProvider;
    this.publishExecutor = MoreExecutors.listeningDecorator(Execs.newBlockingSingleThreaded(getId() + "[publish]", 1));
  }

  private static String makeTaskId(String dataSource, int slotNum, int randomBits)
  {
    final StringBuilder suffix = new StringBuilder(8);
    for (int i = 0; i < Ints.BYTES * 2; ++i) {
      suffix.append((char) ('a' + ((randomBits >>> (i * 4)) & 0x0F)));
    }
    return String.format(
        "%s_%s_%d_%s",
        TYPE,
        dataSource,
        slotNum,
        suffix
    );
  }

  @Override
  public String getType()
  {
    return TYPE;
  }

  @Override
  public String getNodeType()
  {
    return "realtime";
  }

  @Override
  public <T> QueryRunner<T> getQueryRunner(Query<T> query)
  {
    if (appenderator == null) {
      return null;
    }

    return new QueryRunner<T>()
    {
      @Override
      public Sequence<T> run(final Query<T> query, final Map<String, Object> responseContext)
      {
        return query.run(appenderator, responseContext);
      }
    };
  }

  @JsonProperty
  public int getSlotNum()
  {
    return slotNum;
  }

  @JsonProperty
  public DataSchema getDataSchema()
  {
    return dataSchema;
  }

  @JsonProperty
  public RealtimeTuningConfig getTuningConfig()
  {
    return tuningConfig;
  }

  @Override
  public boolean isReady(TaskActionClient taskActionClient) throws Exception
  {
    return true;
  }

  @Override
  public TaskStatus run(TaskToolbox toolbox) throws Exception
  {
    log.info("Starting up!");

    final FireDepartmentMetrics metrics = new FireDepartmentMetrics();
    final BlockingQueue<SegmentIdentifier> handoffs = Queues.newLinkedBlockingQueue();
    try (
        Appenderator appenderator = newAppenderator(metrics, toolbox);
        ServerViewWatcher serverViewWatcher = newServerViewWatcher(appenderator, toolbox, handoffs);
        Resource resource = new Resource()
    ) {
      this.appenderator = appenderator;

      while (!stopped) {
        final RttdRequest untypedRequest = buffer.poll(POLL_TIMEOUT, TimeUnit.MILLISECONDS);

        if (untypedRequest instanceof AppenderationRequest) {
          final AppenderationRequest request = (AppenderationRequest) untypedRequest;
          final AppenderationResponse response = handleAppenderationRequest(request, appenderator, metrics, toolbox);
          request.getResponse().set(response);
        } else if (untypedRequest instanceof ShutDownRequest) {
          final ShutDownRequest request = (ShutDownRequest) untypedRequest;
          final ShutDownResponse response = handleShutDownRequest(request);
          request.getResponse().set(response);
        } else if (untypedRequest != null) {
          log.error("WTF?! Unrecognized request type.");
          untypedRequest.getResponse().setException(new ISE("Unrecognized request type."));
        }

        moveElderSegmentsOut();
        publishOutgoingSegments(appenderator, toolbox);
        dropPublishedSegments(appenderator, handoffs);
      }

      // Stopped
      // TODO: Push, wait for handoff of remaining segments
      // TODO: Keep responding to requests after being told to shutDown; we might be a follower that needs a promotion to leader
      RttdRequest request = null;
      while ((request = buffer.poll()) != null) {
        request.getResponse().setException(new ISE("Stopped"));
      }
    }

    return TaskStatus.success(getId());
  }

  public <T> T performRequest(final RttdRequest<T> request) throws InterruptedException
  {
    return buffer.perform(request);
  }

  private AppenderationResponse handleAppenderationRequest(
      final AppenderationRequest request,
      final Appenderator appenderator,
      final FireDepartmentMetrics metrics,
      final TaskToolbox toolbox
  ) throws IOException, SegmentNotWritableException
  {
    synchronized (activeSegments) {
      if (request.getEpoch() < currentEpoch) {
        return new AppenderationResponse(false, currentEpoch, ImmutableList.<SegmentIdentifier>of());
      } else if (leading && (request.getEpoch() != currentEpoch || !request.isLeader())) {
        log.warn(
            "WTF?! Received request with isLeader[%s], epoch[%,d] but I was already leading with epoch[%,d]. Rejecting.",
            request.isLeader(),
            request.getEpoch(),
            currentEpoch
        );
        return new AppenderationResponse(false, currentEpoch, ImmutableList.<SegmentIdentifier>of());
      } else if (!leading && request.isLeader() && request.getEpoch() == currentEpoch) {
        log.warn(
            "WTF?! Received request with isLeader[%s], epoch[%,d] but currentEpoch is already [%,d]. Rejecting.",
            request.isLeader(),
            request.getEpoch(),
            currentEpoch
        );
        return new AppenderationResponse(false, currentEpoch, ImmutableList.<SegmentIdentifier>of());
      } else if (request.isLeader()) {
        if (!leading) {
          // TODO: Use dataSource metadata to ensure clean transition to a new epoch? Prevent old leader from publishing.
          log.info("You have raised your voices in an unmistakable chorus.");

          assert request.getEpoch() > currentEpoch;
          assert activeSegments.isEmpty();
          assert outgoingSegments.isEmpty();

          currentEpoch = request.getEpoch();
          leading = true;

          // Acquire locks for all pending segments.
          for (final SegmentIdentifier identifier : appenderator.getSegments()) {
            final TaskLock tryLock = toolbox.getTaskActionClient()
                                            .submit(new LockTryAcquireAction(identifier.getInterval()));

            if (tryLock != null) {
              log.info("Taking over segment[%s] from prior epoch.", identifier);
              outgoingSegments.add(identifier);
            } else {
              // TODO: Something better than dropping the segment.
              // TODO: Priority locking would help here, so we can always get the lock.
              log.warn("Could not reacquire lock for segment[%s], dropping.", identifier);
              try {
                appenderator.drop(identifier).get();
              }
              catch (ExecutionException | InterruptedException e) {
                throw Throwables.propagate(e);
              }
            }
          }

          log.info(
              "Leading slotNum[%,d], epoch[%,d] with %,d segments from prior epochs to manage.",
              slotNum,
              currentEpoch,
              appenderator.getSegments().size()
          );
        } else {
          currentEpoch = request.getEpoch();
        }

        final List<SegmentIdentifier> retVal = Lists.newArrayList();

        for (SegmentAndRows segmentAndRows : request.getRows()) {
          for (Map<String, Object> object : segmentAndRows.getRows()) {
            final InputRow row;

            try {
              row = parser.parse(object);
            }
            catch (Exception e) {
              if (log.isDebugEnabled()) {
                log.debug("Dropping unparseable row: %s", object);
              }
              metrics.incrementUnparseable();
              retVal.add(null);
              continue;
            }

            // The request won't have an identifier, so let's determine our own.
            final SegmentIdentifier identifier = getSegment(toolbox.getTaskActionClient(), row.getTimestamp());

            if (identifier != null) {
              // Add the row, possibly publish the segment if it's full.
              final int numRows = appenderator.add(identifier, row, committerSupplier);
              if (numRows >= MAX_SEGMENT_ROWS) {
                moveSegmentsOut(ImmutableList.of(identifier));
              }

              retVal.add(identifier);
            } else {
              if (log.isDebugEnabled()) {
                log.debug("Dropping row: %s", object);
              }
              metrics.incrementThrownAway();
              retVal.add(null);
            }
          }
        }

        if (retVal.size() != request.getRowCount()) {
          throw new ISE("WTF?! Return size[%,d] does not match input size[%,d].", retVal.size(), request.getRowCount());
        }

        return new AppenderationResponse(
            true,
            currentEpoch,
            retVal
        );
      } else {
        assert !leading;
        currentEpoch = request.getEpoch();

        final List<SegmentIdentifier> retVal = Lists.newArrayList();

        for (SegmentAndRows segmentAndRows : request.getRows()) {
          for (Map<String, Object> object : segmentAndRows.getRows()) {
            final InputRow row;
            try {
              row = Preconditions.checkNotNull(parser.parse(object), "row");
            }
            catch (Exception e) {
              if (log.isDebugEnabled()) {
                log.debug("Dropping unparseable row: %s", object);
              }
              metrics.incrementUnparseable();
              retVal.add(null);
              continue;
            }

            // Use identifier from the request.
            final SegmentIdentifier identifier = segmentAndRows.getIdentifier();

            // Add without checking MAX_SEGMENT_ROWS; we're following the leader on this one.
            appenderator.add(identifier, row, committerSupplier);
            retVal.add(identifier);
          }
        }

        return new AppenderationResponse(
            true,
            currentEpoch,
            retVal
        );
      }
    }
  }

  private void moveElderSegmentsOut()
  {
    synchronized (activeSegments) {
      // Publish any segments that should be handed off.
      final long now = System.currentTimeMillis();
      if (now >= nextHandoffTime) {
        final List<SegmentIdentifier> toPublish = Lists.newArrayList();
        for (Map.Entry<Long, ActiveSegment> entry : activeSegments.entrySet()) {
          if (now >= entry.getValue().getHandoffTime().getMillis()) {
            toPublish.add(entry.getValue().getIdentifier());
          }
        }

        moveSegmentsOut(toPublish);
      }
    }
  }

  private void moveSegmentsOut(List<SegmentIdentifier> identifiers)
  {
    synchronized (activeSegments) {
      for (SegmentIdentifier identifier : identifiers) {
        final long key = identifier.getInterval().getStartMillis();
        final ActiveSegment current = activeSegments.get(key);
        if (current != null && current.getIdentifier().equals(identifier)) {
          activeSegments.remove(key);
          log.info("About to handoff segment[%s].", identifier);
          outgoingSegments.add(identifier);
          updateNextHandoffTime();
        } else {
          log.info("Asked to handoff segment[%s], but it was not active. Skipping.", identifier);
        }
      }
    }
  }

  private void updateNextHandoffTime()
  {
    if (activeSegments.isEmpty()) {
      nextHandoffTime = JodaUtils.MAX_INSTANT;
    } else {
      DateTime minHandoffTime = null;
      for (ActiveSegment activeSegment : activeSegments.values()) {
        if (minHandoffTime == null || activeSegment.getHandoffTime().isBefore(minHandoffTime)) {
          minHandoffTime = activeSegment.getHandoffTime();
        }
      }
      nextHandoffTime = minHandoffTime.getMillis();
    }
  }

  private void publishOutgoingSegments(
      final Appenderator appenderator,
      final TaskToolbox toolbox
  )
  {
    if (currentlyPublishing.get() == 0 && !outgoingSegments.isEmpty()) {
      final List<SegmentIdentifier> theList = ImmutableList.copyOf(outgoingSegments);

      currentlyPublishing.incrementAndGet();
      publishExecutor.submit(
          new Runnable()
          {
            @Override
            public void run()
            {
              try {
                final SegmentsAndMetadata segmentsAndMetadata = appenderator.push(theList, committer).get();
                toolbox.getTaskActionClient().submit(
                    new SegmentInsertAction(ImmutableSet.copyOf(segmentsAndMetadata.getSegments()))
                );
                for (SegmentIdentifier identifier : theList) {
                  outgoingSegments.remove(identifier);
                }
              }
              catch (Exception e) {
                log.warn(e, "Failed to publish outgoing segments. Will try again soon.");
              }
              finally {
                currentlyPublishing.decrementAndGet();
              }
            }
          }
      );
    }
  }

  private void dropPublishedSegments(
      final Appenderator appenderator,
      final BlockingQueue<SegmentIdentifier> handoffs
  )
  {
    // Try to drop one published segment.
    SegmentIdentifier toDrop;
    while ((toDrop = handoffs.poll()) != null) {
      // TODO: This could fail, potentially, and then the segment will never get dropped.
      appenderator.drop(toDrop);
    }
  }

  private ShutDownResponse handleShutDownRequest(final ShutDownRequest request) throws IOException
  {
    if (request.getEpoch() < currentEpoch) {
      log.info(
          "Received shutDown request for epoch[%,d] < currentEpoch[%,d], not shutting down.",
          request.getEpoch(),
          currentEpoch
      );
      return new ShutDownResponse(false, currentEpoch);
    } else {
      log.info("Shutting down...");
      currentEpoch = request.getEpoch();
      stopped = true;
      buffer.close();
      return new ShutDownResponse(true, currentEpoch);
    }
  }

  private Appenderator newAppenderator(FireDepartmentMetrics metrics, TaskToolbox toolbox)
  {
    final Appenderator appenderator = Appenderators.realtime(
        dataSchema,
        tuningConfig.withBasePersistDirectory(new File(toolbox.getTaskWorkDir(), "persist")),
        metrics,
        toolbox.getSegmentPusher(),
        toolbox.getObjectMapper(),
        toolbox.getIndexIO(),
        toolbox.getIndexMerger(),
        toolbox.getQueryRunnerFactoryConglomerate(),
        toolbox.getSegmentAnnouncer(),
        toolbox.getEmitter(),
        toolbox.getQueryExecutorService()
    );

    log.info("We have an appenderator. Starting the job!");
    appenderator.startJob();

    return appenderator;
  }

  private ServerViewWatcher newServerViewWatcher(
      final Appenderator appenderator,
      final TaskToolbox toolbox,
      final BlockingQueue<SegmentIdentifier> handoffs
  )
  {
    ServerViewWatcher watcher = new ServerViewWatcher(
        appenderator,
        toolbox.getNewSegmentServerView(),
        MoreExecutors.sameThreadExecutor(),
        new ServerViewWatcher.Callback()
        {
          @Override
          public void notify(
              SegmentIdentifier pending,
              DruidServerMetadata server,
              DataSegment servedSegment
          ) throws Exception
          {
            handoffs.put(pending);
          }
        }
    );

    watcher.start();
    return watcher;
  }

  private SegmentIdentifier getSegment(
      final TaskActionClient taskActionClient,
      final DateTime timestamp
  ) throws IOException
  {
    synchronized (activeSegments) {
      final SegmentIdentifier identifier;

      // See if we already have an active segment for this timestamp.
      final Map.Entry<Long, ActiveSegment> candidateEntry = activeSegments.floorEntry(timestamp.getMillis());
      if (candidateEntry != null && candidateEntry.getValue().getIdentifier().getInterval().contains(timestamp)) {
        identifier = candidateEntry.getValue().getIdentifier();
      } else {
        // Need a new segment.

        if (activeSegments.size() >= MAX_SEGMENTS) {
          // Push eldest one to make room.
          final SegmentIdentifier eldest = activeSegments.firstEntry().getValue().getIdentifier();
          moveSegmentsOut(ImmutableList.of(eldest));
        }

        // Allocate new segment.
        // TODO: Release locks, ever.
        identifier = allocateSegment(taskActionClient, timestamp);

        if (identifier != null) {
          final DateTime handoffTime = computeHandoffTime(
              identifier,
              new DateTime(),
              dataSchema.getGranularitySpec().getSegmentGranularity()
          );

          log.info("New segment[%s], handing off at[%s].", identifier, handoffTime);
          final ActiveSegment holder = new ActiveSegment(identifier, handoffTime);

          if (activeSegments.containsKey(holder.getKey())) {
            final ActiveSegment conflicting = activeSegments.get(holder.getKey());
            throw new ISE(
                "WTF?! Allocated segment[%s] which conflicts with existing segment[%s].",
                holder.getIdentifier().getIdentifierAsString(),
                conflicting.getIdentifier().getIdentifierAsString()
            );
          }

          activeSegments.put(holder.getKey(), holder);
          updateNextHandoffTime();
        } else {
          // Well, we tried.
          // TODO: Blacklist the interval for a while, so we don't waste time continuously trying.
          log.warn("Cannot allocate segment for timestamp[%s].", timestamp);
        }
      }

      return identifier;
    }
  }

  private SegmentIdentifier allocateSegment(
      final TaskActionClient taskActionClient,
      final DateTime timestamp
  ) throws IOException
  {
    // Random sequenceName- we just want a new segment allocated.
    final String sequenceName = UUID.randomUUID().toString();

    return taskActionClient.submit(
        new SegmentAllocateAction(
            getDataSource(),
            timestamp,
            dataSchema.getGranularitySpec().getQueryGranularity(),
            dataSchema.getGranularitySpec().getSegmentGranularity(),
            sequenceName,
            null
        )
    );
  }

  private DateTime computeHandoffTime(
      final SegmentIdentifier identifier,
      final DateTime creationTime,
      final Granularity segmentGranularity
  )
  {
    return new DateTime(
        Longs.max(
            identifier.getInterval().getEnd().plus(tuningConfig.getWindowPeriod()).getMillis(),
            segmentGranularity.bucket(creationTime).getEnd().plus(tuningConfig.getWindowPeriod()).getMillis()
        )
    );
  }

  public class Resource implements ChatHandler, AutoCloseable
  {
    public Resource()
    {
      // TODO: Causes clutter in service discovery; want some better way of getting this info out of the overlord
      chatHandlerProvider.register(getId(), this);
    }

    @GET
    @Path("/status")
    @Produces(MediaType.APPLICATION_JSON)
    public Map<String, Object> getStatus()
    {
      synchronized (activeSegments) {
        return ImmutableMap.of(
            "bufferSize", buffer.size(),
            "activeSegments", Ordering.natural().sortedCopy(
                Iterables.transform(
                    activeSegments.values(),
                    new Function<ActiveSegment, String>()
                    {
                      @Override
                      public String apply(ActiveSegment activeSegment)
                      {
                        return activeSegment.getIdentifier().getIdentifierAsString();
                      }
                    }
                )
            ),
            "allSegments", Ordering.natural().sortedCopy(
                Iterables.transform(
                    appenderator.getSegments(),
                    new Function<SegmentIdentifier, String>()
                    {
                      @Override
                      public String apply(SegmentIdentifier identifier)
                      {
                        return identifier.getIdentifierAsString();
                      }
                    }
                )
            ),
            "currentEpoch", currentEpoch,
            "leading", leading
        );
      }
    }

    @POST
    @Path("/shutDown")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public ShutDownResponse shutDown(final ShutDownRequest request) throws InterruptedException
    {
      return performRequest(request);
    }

    @POST
    @Path("/appenderate")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public AppenderationResponse appenderate(final AppenderationRequest request) throws InterruptedException
    {
      // TODO: Server threads can all get blocked on .handle if the main loop is exerting backpressure
      // TODO: Confirm nothing important is going to get blocked by that
      return performRequest(request);
    }

    @Override
    public void close()
    {
      chatHandlerProvider.unregister(getId());
    }
  }

  public static class ActiveSegment
  {
    private final SegmentIdentifier identifier;
    private final DateTime handoffTime;

    public ActiveSegment(SegmentIdentifier identifier, DateTime handoffTime)
    {
      this.identifier = identifier;
      this.handoffTime = handoffTime;
    }

    public SegmentIdentifier getIdentifier()
    {
      return identifier;
    }

    public DateTime getHandoffTime()
    {
      return handoffTime;
    }

    public Long getKey()
    {
      return identifier.getInterval().getStartMillis();
    }
  }

  public static class SegmentAndRows
  {
    private final SegmentIdentifier identifier;
    private final List<Map<String, Object>> rows;

    @JsonCreator
    public SegmentAndRows(
        @JsonProperty("identifier") SegmentIdentifier identifier,
        @JsonProperty("rows") List<Map<String, Object>> rows
    )
    {
      this.identifier = identifier;
      this.rows = rows;
    }

    @JsonProperty
    public SegmentIdentifier getIdentifier()
    {
      return identifier;
    }

    @JsonProperty
    public List<Map<String, Object>> getRows()
    {
      return rows;
    }
  }

  public interface RttdRequest<T>
  {
    long getEpoch();

    int getRowCount();

    SettableFuture<T> getResponse();
  }

  public static class ShutDownRequest implements RttdRequest<ShutDownResponse>
  {
    private final long epoch;
    private final SettableFuture<ShutDownResponse> response;

    @JsonCreator
    public ShutDownRequest(long epoch)
    {
      this.epoch = epoch;
      this.response = SettableFuture.create();
    }

    @Override
    @JsonProperty
    public long getEpoch()
    {
      return epoch;
    }

    @Override
    public SettableFuture<ShutDownResponse> getResponse()
    {
      return response;
    }

    @Override
    public int getRowCount()
    {
      return 1;
    }
  }

  public static class ShutDownResponse
  {
    private final boolean success;
    private final long epoch;

    @JsonCreator
    public ShutDownResponse(boolean success, long epoch)
    {
      this.success = success;
      this.epoch = epoch;
    }

    @JsonProperty
    public boolean isSuccess()
    {
      return success;
    }

    @JsonProperty
    public long getEpoch()
    {
      return epoch;
    }
  }

  /**
   * Sent by event pushers when we are leading.
   */
  public static class AppenderationRequest implements RttdRequest<AppenderationResponse>
  {
    private final long epoch;
    private final boolean leader;
    private final List<SegmentAndRows> rows;
    private final SettableFuture<AppenderationResponse> response;

    @JsonCreator
    public AppenderationRequest(
        @JsonProperty("epoch") long epoch,
        @JsonProperty("leader") boolean leader,
        @JsonProperty("rows") List<SegmentAndRows> rows
    )
    {
      this.epoch = epoch;
      this.leader = leader;
      this.rows = rows;
      this.response = SettableFuture.create();
    }

    @Override
    @JsonProperty("epoch")
    public long getEpoch()
    {
      return epoch;
    }

    @JsonProperty("leader")
    public boolean isLeader()
    {
      return leader;
    }

    @JsonProperty("rows")
    public List<SegmentAndRows> getRows()
    {
      return rows;
    }

    @Override
    public int getRowCount()
    {
      int rowCount = 0;
      for (SegmentAndRows segmentAndRows : rows) {
        rowCount += segmentAndRows.getRows().size();
      }
      return rowCount;
    }

    @Override
    public SettableFuture<AppenderationResponse> getResponse()
    {
      return response;
    }
  }

  // TODO: Serialized form is too large
  public static class AppenderationResponse
  {
    private final boolean success;
    private final long epoch;
    private final List<SegmentIdentifier> segments;

    @JsonCreator
    public AppenderationResponse(
        @JsonProperty("success") boolean success,
        @JsonProperty("epoch") long epoch,
        @JsonProperty("segments") List<SegmentIdentifier> segments
    )
    {
      this.success = success;
      this.epoch = epoch;
      this.segments = segments;
    }

    @JsonProperty
    public boolean isSuccess()
    {
      return success;
    }

    @JsonProperty
    public long getEpoch()
    {
      return epoch;
    }

    @JsonProperty
    public List<SegmentIdentifier> getSegments()
    {
      return segments;
    }
  }

  public static class RequestBuffer implements Closeable
  {
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition await = lock.newCondition();
    private final ArrayDeque<RttdRequest> deque = new ArrayDeque<>();
    private final int maxBufferSize;

    private volatile int bufferedMessages = 0;
    private volatile boolean open = true;

    public RequestBuffer(int maxBufferSize)
    {
      this.maxBufferSize = maxBufferSize;
    }

    public <T> T perform(RttdRequest<T> request) throws InterruptedException
    {
      put(request);

      // TODO: Bail out if buffer is closed while waiting for a resposne
      try {
        return request.getResponse().get();
      }
      catch (Exception e) {
        Throwables.propagateIfInstanceOf(e, InterruptedException.class);
        throw Throwables.propagate(e);
      }
    }

    public void put(RttdRequest request) throws InterruptedException
    {
      Preconditions.checkNotNull(request, "request");

      final int requestMessages = request.getRowCount();
      if (requestMessages > maxBufferSize) {
        throw new IAE("Cannot add [%,d] messages with maxBufferSize[%,d].", requestMessages, maxBufferSize);
      }

      lock.lockInterruptibly();
      try {
        while (open && requestMessages + bufferedMessages > maxBufferSize) {
          await.await();
        }

        if (!open) {
          throw new ISE("Cannot add to closed buffer");
        }

        deque.add(request);
        bufferedMessages += requestMessages;
      }
      finally {
        lock.unlock();
      }
    }

    public RttdRequest poll()
    {
      lock.lock();
      try {
        return deque.poll();
      }
      finally {
        lock.unlock();
      }
    }

    public RttdRequest poll(long timeout, TimeUnit unit) throws InterruptedException
    {
      long timeoutNanos = unit.toNanos(timeout);

      lock.lockInterruptibly();
      try {
        while (deque.isEmpty()) {
          if (timeoutNanos <= 0) {
            return null;
          } else {
            timeoutNanos = await.awaitNanos(timeoutNanos);
          }
        }

        final RttdRequest retVal = Preconditions.checkNotNull(deque.poll(), "WTF?! Null poll?");
        bufferedMessages -= retVal.getRowCount();
        return retVal;
      }
      finally {
        lock.unlock();
      }
    }

    public int size()
    {
      return bufferedMessages;
    }

    @Override
    public void close() throws IOException
    {
      lock.lock();
      try {
        open = false;
        await.signalAll();
      }
      finally {
        lock.unlock();
      }
    }
  }
}
