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

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.jaxrs.smile.SmileMediaTypes;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.CountingInputStream;
import com.google.common.util.concurrent.Uninterruptibles;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import org.apache.druid.concurrent.Threads;
import org.apache.druid.data.input.Firehose;
import org.apache.druid.data.input.FirehoseFactory;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.impl.InputRowParser;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.guice.annotations.Smile;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.server.metrics.EventReceiverFirehoseMetric;
import org.apache.druid.server.metrics.EventReceiverFirehoseRegister;
import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.AuthorizationUtils;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceAction;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Builds firehoses that accept events through the {@link EventReceiver} interface. Can also register these
 * firehoses with an {@link ServiceAnnouncingChatHandlerProvider}.
 */
public class EventReceiverFirehoseFactory implements FirehoseFactory<InputRowParser<Map<String, Object>>>
{
  private static final EmittingLogger log = new EmittingLogger(EventReceiverFirehoseFactory.class);

  public static final int MAX_FIREHOSE_PRODUCERS = 10_000;

  private static final int DEFAULT_BUFFER_SIZE = 100_000;

  /**
   * A "poison pill" object for {@link EventReceiverFirehose}'s internal buffer.
   */
  private static final Object FIREHOSE_CLOSED = new Object();

  private final String serviceName;
  private final int bufferSize;

  /**
   * Doesn't really support max idle times finer than 1 second due to how {@link
   * EventReceiverFirehose#delayedCloseExecutor} is implemented, see a comment inside {@link
   * EventReceiverFirehose#createDelayedCloseExecutor()}. This aspect is not reflected in docs because it's unlikely
   * that anybody configures or cares about finer max idle times, and also because this is an implementation detail of
   * {@link EventReceiverFirehose} that may change in the future.
   */
  private final long maxIdleTimeMillis;
  private final ChatHandlerProvider chatHandlerProvider;
  private final ObjectMapper jsonMapper;
  private final ObjectMapper smileMapper;
  private final EventReceiverFirehoseRegister eventReceiverFirehoseRegister;
  private final AuthorizerMapper authorizerMapper;

  @JsonCreator
  public EventReceiverFirehoseFactory(
      @JsonProperty("serviceName") String serviceName,
      @JsonProperty("bufferSize") Integer bufferSize,
      // Keeping the legacy 'maxIdleTime' property name for backward compatibility. When the project is updated to
      // Jackson 2.9 it could be changed, see https://github.com/apache/incubator-druid/issues/7152
      @JsonProperty("maxIdleTime") @Nullable Long maxIdleTimeMillis,
      @JacksonInject ChatHandlerProvider chatHandlerProvider,
      @JacksonInject @Json ObjectMapper jsonMapper,
      @JacksonInject @Smile ObjectMapper smileMapper,
      @JacksonInject EventReceiverFirehoseRegister eventReceiverFirehoseRegister,
      @JacksonInject AuthorizerMapper authorizerMapper
  )
  {
    Preconditions.checkNotNull(serviceName, "serviceName");

    this.serviceName = serviceName;
    this.bufferSize = bufferSize == null || bufferSize <= 0 ? DEFAULT_BUFFER_SIZE : bufferSize;
    this.maxIdleTimeMillis = (maxIdleTimeMillis == null || maxIdleTimeMillis <= 0) ? Long.MAX_VALUE : maxIdleTimeMillis;
    this.chatHandlerProvider = chatHandlerProvider;
    this.jsonMapper = jsonMapper;
    this.smileMapper = smileMapper;
    this.eventReceiverFirehoseRegister = eventReceiverFirehoseRegister;
    this.authorizerMapper = authorizerMapper;
  }

  @Override
  public Firehose connect(
      InputRowParser<Map<String, Object>> firehoseParser,
      File temporaryDirectory
  )
  {
    log.info("Connecting firehose: %s", serviceName);
    final EventReceiverFirehose firehose = new EventReceiverFirehose(firehoseParser);

    if (chatHandlerProvider != null) {
      log.info("Found chathandler of class[%s]", chatHandlerProvider.getClass().getName());
      chatHandlerProvider.register(serviceName, firehose);
      int lastIndexOfColon = serviceName.lastIndexOf(':');
      if (lastIndexOfColon > 0) {
        chatHandlerProvider.register(serviceName.substring(lastIndexOfColon + 1), firehose);
      }
    } else {
      log.warn("No chathandler detected");
    }

    eventReceiverFirehoseRegister.register(serviceName, firehose);

    return firehose;
  }

  @JsonProperty
  public String getServiceName()
  {
    return serviceName;
  }

  @JsonProperty
  public int getBufferSize()
  {
    return bufferSize;
  }

  /**
   * Keeping the legacy 'maxIdleTime' property name for backward compatibility. When the project is updated to Jackson
   * 2.9 it could be changed, see https://github.com/apache/incubator-druid/issues/7152
   */
  @JsonProperty("maxIdleTime")
  public long getMaxIdleTimeMillis()
  {
    return maxIdleTimeMillis;
  }

  /**
   * Apart from adhering to {@link Firehose} contract regarding concurrency, this class has two methods that might be
   * called concurrently with any other methods and each other, from arbitrary number of threads: {@link #addAll} and
   * {@link #shutdown}.
   *
   * Concurrent data flow: in {@link #addAll} (can be called concurrently with any other methods and other calls to
   * {@link #addAll}) rows are pushed into {@link #buffer}. The single Firehose "consumer" thread calls {@link #hasMore}
   * and {@link #nextRow()}, where rows are taken out from the other end of the {@link #buffer} queue.
   *
   * This class creates and manages one thread ({@link #delayedCloseExecutor}) for calling {@link #close()}
   * asynchronously in response to a {@link #shutdown} request, or after this Firehose has been idle (no calls to {@link
   * #addAll}) for {@link #maxIdleTimeMillis}.
   */
  @VisibleForTesting
  public class EventReceiverFirehose implements ChatHandler, Firehose, EventReceiverFirehoseMetric
  {
    /**
     * How does this thread work (and its interruption policy) is described in the comment for {@link
     * #createDelayedCloseExecutor}.
     */
    @GuardedBy("this")
    private @Nullable Thread delayedCloseExecutor;

    /**
     * Contains {@link InputRow} objects, the last one is {@link #FIREHOSE_CLOSED} which is a "poison pill". Poison pill
     * is used to notify the thread that calls {@link #hasMore()} and {@link #nextRow()} that the EventReceiverFirehose
     * is closed without heuristic 500 ms timed blocking in a loop instead of a simple {@link BlockingQueue#take()}
     * call (see {@link #hasMore} code).
     */
    private final BlockingQueue<Object> buffer;
    private final InputRowParser<Map<String, Object>> parser;

    /**
     * This field needs to be volatile to ensure progress in {@link #addRows} method where it is read in a loop, and
     * also in testing code calling {@link #isClosed()}.
     */
    private volatile boolean closed = false;

    /**
     * This field and {@link #rowsRunOut} are not volatile because they are accessed only from {@link #hasMore()} and
     * {@link #nextRow()} methods that are called from a single thread according to {@link Firehose} spec.
     */
    @Nullable
    private InputRow nextRow = null;
    private boolean rowsRunOut = false;

    private final AtomicLong bytesReceived = new AtomicLong(0);
    private final AtomicLong lastBufferAddFailLoggingTimeNs = new AtomicLong(System.nanoTime());
    private final ConcurrentHashMap<String, Long> producerSequences = new ConcurrentHashMap<>();

    /**
     * This field and {@link #requestedShutdownTimeNs} use nanoseconds instead of milliseconds not to deal with the fact
     * that {@link System#currentTimeMillis()} can "go backward", e. g. due to time correction on the server.
     *
     * This field and {@link #requestedShutdownTimeNs} must be volatile because they are de facto lazily initialized
     * fields that are used concurrently in {@link #delayedCloseExecutor} (see {@link #createDelayedCloseExecutor()}).
     * If they were not volatile, NPE would be possible in {@link #delayedCloseExecutor}. See
     * https://shipilev.net/blog/2016/close-encounters-of-jmm-kind/#wishful-hb-actual for explanations.
     */
    @Nullable
    private volatile Long idleCloseTimeNs = null;
    @Nullable
    private volatile Long requestedShutdownTimeNs = null;

    EventReceiverFirehose(InputRowParser<Map<String, Object>> parser)
    {
      this.buffer = new ArrayBlockingQueue<>(bufferSize);
      this.parser = parser;

      if (maxIdleTimeMillis != Long.MAX_VALUE) {
        idleCloseTimeNs = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(maxIdleTimeMillis);
        synchronized (this) {
          createDelayedCloseExecutor();
        }
      }
    }

    @VisibleForTesting
    synchronized @Nullable Thread getDelayedCloseExecutor()
    {
      return delayedCloseExecutor;
    }

    /**
     * Creates and starts a {@link #delayedCloseExecutor} thread, either right from the EventReceiverFirehose's
     * constructor if {@link #maxIdleTimeMillis} is specified, or otherwise lazily from {@link #shutdown}.
     *
     * The thread waits until the time when the Firehose should be closed because either {@link #addAll} was not called
     * for the specified max idle time (see {@link #idleCloseTimeNs}), or until the shutoff time requested last
     * via {@link #shutdown} (see {@link #requestedShutdownTimeNs}), whatever is sooner. Then the thread does
     * two things:
     *  1. if the Firehose is already closed (or in the process of closing, but {@link #closed} flag is already set), it
     *    silently exits.
     *  2. It checks both deadlines again:
     *       a) if either of them has arrived, it calls {@link #close()} and exits.
     *       b) otherwise, it waits until the nearest deadline again, and so on in a loop.
     *
     * This way the thread works predictably and robustly regardless of how both deadlines change (for example, shutoff
     * time specified via {@link #shutdown} may jump in both directions).
     *
     * Other methods notify {@link #delayedCloseExecutor} that the Firehose state in some way that is important for this
     * thread (that is, when {@link #close()} is called, {@link #delayedCloseExecutor} is no longer needed and should
     * exit as soon as possible to release system resources; when {@link #shutdown} is called, the thread may need to
     * wake up sooner if the shutoff time has been moved sooner) by simply interrupting it. The thread wakes up and
     * continues its loop.
     */
    @GuardedBy("this")
    private Thread createDelayedCloseExecutor()
    {
      Thread delayedCloseExecutor = new Thread(
          () -> {
            // The closed = true is visible after close() because there is a happens-before edge between
            // delayedCloseExecutor.interrupt() call in close() and catching InterruptedException below in this loop.
            while (!closed) {
              if (idleCloseTimeNs == null && requestedShutdownTimeNs == null) {
                // This is not possible unless there are bugs in the code of EventReceiverFirehose. AssertionError could
                // have been thrown instead, but it doesn't seem to make a lot of sense in a background thread. Instead,
                // we long the error and continue a loop after some pause.
                log.error(
                    "Either idleCloseTimeNs or requestedShutdownTimeNs must be non-null. "
                    + "Please file a bug at https://github.com/apache/incubator-druid/issues"
                );
              }
              if (idleCloseTimeNs != null && idleCloseTimeNs - System.nanoTime() <= 0) { // overflow-aware comparison
                log.info("Firehose has been idle for %d ms, closing.", maxIdleTimeMillis);
                close();
              } else if (requestedShutdownTimeNs != null &&
                         requestedShutdownTimeNs - System.nanoTime() <= 0) { // overflow-aware comparison
                log.info("Closing Firehose after a shutdown request");
                close();
              }
              try {
                // It is possible to write code that sleeps until the next the next idleCloseTimeNs or
                // requestedShutdownTimeNs, whatever is non-null and sooner, but that's fairly complicated code. That
                // complexity perhaps overweighs the minor inefficiency of simply waking up every second.
                Threads.sleepFor(1, TimeUnit.SECONDS);
              }
              catch (InterruptedException ignore) {
                // Interruption is a wakeup, continue the loop
              }
            }
          },
          "event-receiver-firehose-closer"
      );
      delayedCloseExecutor.setDaemon(true);
      this.delayedCloseExecutor = delayedCloseExecutor;
      delayedCloseExecutor.start();
      return delayedCloseExecutor;
    }

    /**
     * This method might be called concurrently from multiple threads, if multiple requests arrive to the server at the
     * same time (possibly exact duplicates). Concurrency is controlled in {@link #checkProducerSequence}, where only
     * requests with "X-Firehose-Producer-Seq" number greater than the max "X-Firehose-Producer-Seq" in previously
     * arrived requests are allowed to proceed. After that check requests don't synchronize with each other and
     * therefore if two large batches are sent with little interval, the events from the batches might be mixed up in
     * {@link #buffer} (if two {@link #addRows(Iterable)} are executed concurrently).
     */
    @POST
    @Path("/push-events")
    @Consumes({MediaType.APPLICATION_JSON, SmileMediaTypes.APPLICATION_JACKSON_SMILE})
    @Produces({MediaType.APPLICATION_JSON, SmileMediaTypes.APPLICATION_JACKSON_SMILE})
    public Response addAll(InputStream in, @Context final HttpServletRequest req) throws JsonProcessingException
    {
      idleCloseTimeNs = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(maxIdleTimeMillis);
      Access accessResult = AuthorizationUtils.authorizeResourceAction(
          req,
          new ResourceAction(
              Resource.STATE_RESOURCE,
              Action.WRITE
          ),
          authorizerMapper
      );
      if (!accessResult.isAllowed()) {
        return Response.status(403).build();
      }

      final String reqContentType = req.getContentType();
      final boolean isSmile = SmileMediaTypes.APPLICATION_JACKSON_SMILE.equals(reqContentType);
      final String contentType = isSmile ? SmileMediaTypes.APPLICATION_JACKSON_SMILE : MediaType.APPLICATION_JSON;

      ObjectMapper objectMapper = isSmile ? smileMapper : jsonMapper;

      Response producerSequenceResponse = checkProducerSequence(req, reqContentType, objectMapper);
      if (producerSequenceResponse != null) {
        return producerSequenceResponse;
      }

      CountingInputStream countingInputStream = new CountingInputStream(in);
      Collection<Map<String, Object>> events;
      try {
        events = objectMapper.readValue(
            countingInputStream,
            new TypeReference<Collection<Map<String, Object>>>()
            {
            }
        );
      }
      catch (IOException e) {
        return Response.serverError().entity(ImmutableMap.<String, Object>of("error", e.getMessage())).build();
      }
      finally {
        bytesReceived.addAndGet(countingInputStream.getCount());
      }
      log.debug("Adding %,d events to firehose: %s", events.size(), serviceName);

      final List<InputRow> rows = new ArrayList<>();
      for (final Map<String, Object> event : events) {
        // Might throw an exception. We'd like that to happen now, instead of while adding to the row buffer.
        rows.addAll(parser.parseBatch(event));
      }

      try {
        addRows(rows);
        return Response.ok(
            objectMapper.writeValueAsString(ImmutableMap.of("eventCount", events.size())),
            contentType
        ).build();
      }
      catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }
    }

    @Override
    public boolean hasMore()
    {
      if (rowsRunOut) {
        return false;
      }
      if (nextRow != null) {
        return true;
      }
      Object next;
      try {
        next = buffer.take();
      }
      catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }
      //noinspection ObjectEquality
      if (next == FIREHOSE_CLOSED) {
        rowsRunOut = true;
        return false;
      }
      nextRow = (InputRow) next;
      return true;
    }

    @Nullable
    @Override
    public InputRow nextRow()
    {
      final InputRow row = nextRow;

      if (row == null) {
        throw new NoSuchElementException();
      } else {
        nextRow = null;
        return row;
      }
    }

    @Override
    public int getCurrentBufferSize()
    {
      return buffer.size();
    }

    @Override
    public int getCapacity()
    {
      return bufferSize;
    }

    @Override
    public long getBytesReceived()
    {
      return bytesReceived.get();
    }

    /**
     * This method is synchronized because it might be called concurrently from multiple threads: from {@link
     * #delayedCloseExecutor}, and from the thread that creates and uses the Firehose object.
     */
    @Override
    public synchronized void close()
    {
      if (closed) {
        return;
      }
      closed = true;
      log.info("Firehose closing.");

      // Critical to add the poison pill to the queue, don't allow interruption.
      Uninterruptibles.putUninterruptibly(buffer, FIREHOSE_CLOSED);

      eventReceiverFirehoseRegister.unregister(serviceName);
      if (chatHandlerProvider != null) {
        chatHandlerProvider.unregister(serviceName);
      }
      if (delayedCloseExecutor != null && !delayedCloseExecutor.equals(Thread.currentThread())) {
        // Interrupt delayedCloseExecutor to let it discover that closed flag is already set and exit.
        delayedCloseExecutor.interrupt();
      }
    }

    @VisibleForTesting
    void addRows(Iterable<InputRow> rows) throws InterruptedException
    {
      for (final InputRow row : rows) {
        boolean added = false;
        while (!closed && !added) {
          added = buffer.offer(row, 500, TimeUnit.MILLISECONDS);
          if (!added) {
            long currTimeNs = System.nanoTime();
            long lastTimeNs = lastBufferAddFailLoggingTimeNs.get();
            if (currTimeNs - lastTimeNs > TimeUnit.SECONDS.toNanos(10) &&
                lastBufferAddFailLoggingTimeNs.compareAndSet(lastTimeNs, currTimeNs)) {
              log.warn("Failed to add event to buffer with current size [%s] . Retrying...", buffer.size());
            }
          }
        }

        if (!added) {
          throw new IllegalStateException("Cannot add events to closed firehose!");
        }
      }
    }

    /**
     * This method might be called concurrently from multiple threads, if multiple shutdown requests arrive at the same
     * time. No attempts are made to synchronize such requests, or prioritize them a-la "latest shutdown time wins" or
     * "soonest shutdown time wins". {@link #delayedCloseExecutor}'s logic (see {@link #createDelayedCloseExecutor()})
     * is indifferent to shutdown times jumping in arbitrary directions. But once a shutdown request is made, it can't
     * be cancelled entirely, the shutdown time could only be rescheduled with a new request.
     */
    @POST
    @Path("/shutdown")
    @Consumes({MediaType.APPLICATION_JSON, SmileMediaTypes.APPLICATION_JACKSON_SMILE})
    @Produces({MediaType.APPLICATION_JSON, SmileMediaTypes.APPLICATION_JACKSON_SMILE})
    public Response shutdown(
        @QueryParam("shutoffTime") final String shutoffTimeMillis,
        @Context final HttpServletRequest req
    )
    {
      Access accessResult = AuthorizationUtils.authorizeResourceAction(
          req,
          new ResourceAction(
              Resource.STATE_RESOURCE,
              Action.WRITE
          ),
          authorizerMapper
      );
      if (!accessResult.isAllowed()) {
        return Response.status(403).build();
      }

      try {
        DateTime shutoffAt = shutoffTimeMillis == null ? DateTimes.nowUtc() : DateTimes.of(shutoffTimeMillis);
        log.info("Setting Firehose shutoffTime to %s", shutoffTimeMillis);
        long shutoffTimeoutMillis = Math.max(shutoffAt.getMillis() - System.currentTimeMillis(), 0);

        requestedShutdownTimeNs = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(shutoffTimeoutMillis);
        Thread delayedCloseExecutor;
        // Need to interrupt delayedCloseExecutor because a newly specified shutdown time might be closer than idle
        // timeout or previously specified shutdown. Interruption of delayedCloseExecutor lets it adjust the sleep time
        // (see the logic of this thread in createDelayedCloseExecutor()).
        boolean needToInterruptDelayedCloseExecutor = true;
        synchronized (this) {
          delayedCloseExecutor = this.delayedCloseExecutor;
          if (delayedCloseExecutor == null) {
            delayedCloseExecutor = createDelayedCloseExecutor();
            // Don't need to interrupt a freshly created thread
            needToInterruptDelayedCloseExecutor = false;
          }
        }
        if (needToInterruptDelayedCloseExecutor) {
          delayedCloseExecutor.interrupt();
        }
        return Response.ok().build();
      }
      catch (IllegalArgumentException e) {
        return Response.status(Response.Status.BAD_REQUEST)
                       .entity(ImmutableMap.<String, Object>of("error", e.getMessage()))
                       .build();

      }
    }

    @VisibleForTesting
    boolean isClosed()
    {
      return closed;
    }

    /**
     * Checks the request for a producer ID and sequence value.  If the producer ID is specified, a corresponding
     * sequence value must be specified as well.  If the incoming sequence is less than or equal to the last seen
     * sequence for that producer ID, the request is ignored.
     *
     * This method might be called concurrently from multiple threads.
     *
     * @param req Http request
     * @param responseContentType Response content type
     * @param responseMapper Response object mapper
     * @return an error response to return or null if the request can proceed
     */
    @Nullable
    private Response checkProducerSequence(
        final HttpServletRequest req,
        final String responseContentType,
        final ObjectMapper responseMapper
    )
    {
      final String producerId = req.getHeader("X-Firehose-Producer-Id");

      if (producerId == null) {
        return null;
      }

      final String sequenceValue = req.getHeader("X-Firehose-Producer-Seq");

      if (sequenceValue == null) {
        return Response
            .status(Response.Status.BAD_REQUEST)
            .entity(ImmutableMap.<String, Object>of("error", "Producer sequence value is missing"))
            .build();
      }

      Long producerSequence = producerSequences.computeIfAbsent(producerId, key -> Long.MIN_VALUE);

      if (producerSequences.size() >= MAX_FIREHOSE_PRODUCERS) {
        return Response
            .status(Response.Status.FORBIDDEN)
            .entity(
                ImmutableMap.of(
                    "error",
                    "Too many individual producer IDs for this firehose.  Max is " + MAX_FIREHOSE_PRODUCERS
                )
            )
            .build();
      }

      try {
        Long newSequence = Long.parseLong(sequenceValue);

        while (true) {
          if (newSequence <= producerSequence) {
            return Response.ok(
                responseMapper.writeValueAsString(ImmutableMap.of("eventCount", 0, "skipped", true)),
                responseContentType
            ).build();
          }
          if (producerSequences.replace(producerId, producerSequence, newSequence)) {
            return null;
          }
          producerSequence = producerSequences.get(producerId);
        }
      }
      catch (JsonProcessingException ex) {
        throw new RuntimeException(ex);
      }
      catch (NumberFormatException ex) {
        return Response
            .status(Response.Status.BAD_REQUEST)
            .entity(ImmutableMap.<String, Object>of("error", "Producer sequence must be a number"))
            .build();
      }
    }
  }
}
