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

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.jaxrs.smile.SmileMediaTypes;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.io.CountingInputStream;
import com.metamx.emitter.EmittingLogger;
import io.druid.java.util.common.concurrent.Execs;
import io.druid.data.input.Firehose;
import io.druid.data.input.FirehoseFactory;
import io.druid.data.input.InputRow;
import io.druid.data.input.impl.InputRowParser;
import io.druid.guice.annotations.Json;
import io.druid.guice.annotations.Smile;
import io.druid.java.util.common.DateTimes;
import io.druid.server.metrics.EventReceiverFirehoseMetric;
import io.druid.server.metrics.EventReceiverFirehoseRegister;
import io.druid.server.security.Access;
import io.druid.server.security.Action;
import io.druid.server.security.AuthorizationUtils;
import io.druid.server.security.AuthorizerMapper;
import io.druid.server.security.Resource;
import io.druid.server.security.ResourceAction;
import io.druid.server.security.ResourceType;
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
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Builds firehoses that accept events through the {@link EventReceiver} interface. Can also register these
 * firehoses with an {@link ServiceAnnouncingChatHandlerProvider}.
 */
public class EventReceiverFirehoseFactory implements FirehoseFactory<InputRowParser<Map<String, Object>>>
{
  public static final int MAX_FIREHOSE_PRODUCERS = 10_000;

  private static final EmittingLogger log = new EmittingLogger(EventReceiverFirehoseFactory.class);
  private static final int DEFAULT_BUFFER_SIZE = 100_000;

  private final String serviceName;
  private final int bufferSize;
  private final Optional<ChatHandlerProvider> chatHandlerProvider;
  private final ObjectMapper jsonMapper;
  private final ObjectMapper smileMapper;
  private final EventReceiverFirehoseRegister eventReceiverFirehoseRegister;
  private final AuthorizerMapper authorizerMapper;

  @JsonCreator
  public EventReceiverFirehoseFactory(
      @JsonProperty("serviceName") String serviceName,
      @JsonProperty("bufferSize") Integer bufferSize,
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
    this.chatHandlerProvider = Optional.ofNullable(chatHandlerProvider);
    this.jsonMapper = jsonMapper;
    this.smileMapper = smileMapper;
    this.eventReceiverFirehoseRegister = eventReceiverFirehoseRegister;
    this.authorizerMapper = authorizerMapper;
  }

  @Override
  public Firehose connect(
      InputRowParser<Map<String, Object>> firehoseParser,
      File temporaryDirectory
  ) throws IOException
  {
    log.info("Connecting firehose: %s", serviceName);
    final EventReceiverFirehose firehose = new EventReceiverFirehose(firehoseParser);

    if (chatHandlerProvider.isPresent()) {
      log.info("Found chathandler of class[%s]", chatHandlerProvider.get().getClass().getName());
      chatHandlerProvider.get().register(serviceName, firehose);
      if (serviceName.contains(":")) {
        chatHandlerProvider.get().register(serviceName.replaceAll(".*:", ""), firehose); // rofl
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

  public class EventReceiverFirehose implements ChatHandler, Firehose, EventReceiverFirehoseMetric
  {
    private final ScheduledExecutorService exec;
    private final BlockingQueue<InputRow> buffer;
    private final InputRowParser<Map<String, Object>> parser;

    private final Object readLock = new Object();

    private volatile InputRow nextRow = null;
    private volatile boolean closed = false;
    private final AtomicLong bytesReceived = new AtomicLong(0);
    private final AtomicLong lastBufferAddFailMsgTime = new AtomicLong(0);
    private final ConcurrentMap<String, Long> producerSequences = new ConcurrentHashMap<>();

    public EventReceiverFirehose(InputRowParser<Map<String, Object>> parser)
    {
      this.buffer = new ArrayBlockingQueue<>(bufferSize);
      this.parser = parser;
      exec = Execs.scheduledSingleThreaded("event-receiver-firehose-%d");
    }

    @POST
    @Path("/push-events")
    @Consumes({MediaType.APPLICATION_JSON, SmileMediaTypes.APPLICATION_JACKSON_SMILE})
    @Produces({MediaType.APPLICATION_JSON, SmileMediaTypes.APPLICATION_JACKSON_SMILE})
    public Response addAll(
        InputStream in,
        @Context final HttpServletRequest req
    )
    {
      Access accessResult = AuthorizationUtils.authorizeResourceAction(
          req,
          new ResourceAction(
              new Resource("STATE", ResourceType.STATE),
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

      Optional<Response> producerSequenceResponse = checkProducerSequence(req, reqContentType, objectMapper);
      if (producerSequenceResponse.isPresent()) {
        return producerSequenceResponse.get();
      }

      CountingInputStream countingInputStream = new CountingInputStream(in);
      Collection<Map<String, Object>> events = null;
      try {
        events = objectMapper.readValue(
            countingInputStream, new TypeReference<Collection<Map<String, Object>>>()
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

      final List<InputRow> rows = Lists.newArrayList();
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
        throw Throwables.propagate(e);
      }
      catch (JsonProcessingException e) {
        throw Throwables.propagate(e);
      }
    }

    @Override
    public boolean hasMore()
    {
      synchronized (readLock) {
        try {
          while (nextRow == null) {
            nextRow = buffer.poll(500, TimeUnit.MILLISECONDS);
            if (closed) {
              break;
            }
          }
        }
        catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw Throwables.propagate(e);
        }

        return nextRow != null;
      }
    }

    @Nullable
    @Override
    public InputRow nextRow()
    {
      synchronized (readLock) {
        final InputRow row = nextRow;

        if (row == null) {
          throw new NoSuchElementException();
        } else {
          nextRow = null;
          return row;
        }
      }
    }

    @Override
    public Runnable commit()
    {
      return new Runnable()
      {
        @Override
        public void run()
        {
          // Nothing
        }
      };
    }

    @Override
    public int getCurrentBufferSize()
    {
      // ArrayBlockingQueue's implementation of size() is thread-safe, so we can use that
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

    @Override
    public void close() throws IOException
    {
      if (!closed) {
        log.info("Firehose closing.");
        closed = true;

        eventReceiverFirehoseRegister.unregister(serviceName);
        if (chatHandlerProvider.isPresent()) {
          chatHandlerProvider.get().unregister(serviceName);
        }
        exec.shutdown();
      }
    }

    // public for tests
    public void addRows(Iterable<InputRow> rows) throws InterruptedException
    {
      for (final InputRow row : rows) {
        boolean added = false;
        while (!closed && !added) {
          added = buffer.offer(row, 500, TimeUnit.MILLISECONDS);
          if (!added) {
            long currTime = System.currentTimeMillis();
            long lastTime = lastBufferAddFailMsgTime.get();
            if (currTime - lastTime > 10000 && lastBufferAddFailMsgTime.compareAndSet(lastTime, currTime)) {
              log.warn("Failed to add event to buffer with current size [%s] . Retrying...", buffer.size());
            }
          }
        }

        if (!added) {
          throw new IllegalStateException("Cannot add events to closed firehose!");
        }
      }
    }

    @POST
    @Path("/shutdown")
    @Consumes({MediaType.APPLICATION_JSON, SmileMediaTypes.APPLICATION_JACKSON_SMILE})
    @Produces({MediaType.APPLICATION_JSON, SmileMediaTypes.APPLICATION_JACKSON_SMILE})
    public Response shutdown(
        @QueryParam("shutoffTime") final String shutoffTime,
        @Context final HttpServletRequest req
    )
    {
      Access accessResult = AuthorizationUtils.authorizeResourceAction(
          req,
          new ResourceAction(
              new Resource("STATE", ResourceType.STATE),
              Action.WRITE
          ),
          authorizerMapper
      );
      if (!accessResult.isAllowed()) {
        return Response.status(403).build();
      }

      try {
        DateTime shutoffAt = shutoffTime == null ? DateTimes.nowUtc() : DateTimes.of(shutoffTime);
        log.info("Setting Firehose shutoffTime to %s", shutoffTime);
        exec.schedule(
            new Runnable()
            {
              @Override
              public void run()
              {
                try {
                  close();
                }
                catch (IOException e) {
                  log.warn(e, "Failed to close delegate firehose, ignoring.");
                }
              }
            },
            shutoffAt.getMillis() - System.currentTimeMillis(),
            TimeUnit.MILLISECONDS
        );
        return Response.ok().build();
      }
      catch (IllegalArgumentException e) {
        return Response.status(Response.Status.BAD_REQUEST)
                       .entity(ImmutableMap.<String, Object>of("error", e.getMessage()))
                       .build();

      }
    }

    @VisibleForTesting
    public boolean isClosed()
    {
      return closed;
    }

    /**
     * Checks the request for a producer ID and sequence value.  If the producer ID is specified, a corresponding
     * sequence value must be specified as well.  If the incoming sequence is less than or equal to the last seen
     * sequence for that producer ID, the request is ignored
     *
     * @param req Http request
     * @param responseContentType Response content type
     * @param responseMapper Response object mapper
     * @return Optional of a response to return of an empty optional if the request can proceed
     */
    private Optional<Response> checkProducerSequence(
        final HttpServletRequest req,
        final String responseContentType,
        final ObjectMapper responseMapper
    )
    {
      final String producerId = req.getHeader("X-Firehose-Producer-Id");

      if (producerId == null) {
        return Optional.empty();
      }

      final String sequenceValue = req.getHeader("X-Firehose-Producer-Seq");

      if (sequenceValue == null) {
        return Optional.of(
            Response.status(Response.Status.BAD_REQUEST)
                       .entity(ImmutableMap.<String, Object>of("error", "Producer sequence value is missing"))
                       .build()
        );
      }

      Long producerSequence = producerSequences.computeIfAbsent(producerId, key -> Long.MIN_VALUE);

      if (producerSequences.size() >= MAX_FIREHOSE_PRODUCERS) {
        return Optional.of(
            Response.status(Response.Status.FORBIDDEN)
                    .entity(
                        ImmutableMap.<String, Object>of(
                            "error",
                            "Too many individual producer IDs for this firehose.  Max is " + MAX_FIREHOSE_PRODUCERS
                        )
                    )
                    .build()
        );
      }

      try {
        Long newSequence = Long.parseLong(sequenceValue);
        if (newSequence <= producerSequence) {
          return Optional.of(
              Response.ok(
                responseMapper.writeValueAsString(
                    ImmutableMap.of("eventCount", 0, "skipped", true)
                ),
                responseContentType
            ).build()
          );
        }

        producerSequences.put(producerId, newSequence);
      }
      catch (JsonProcessingException ex) {
        throw Throwables.propagate(ex);
      }
      catch (NumberFormatException ex) {
        return Optional.of(
            Response.status(Response.Status.BAD_REQUEST)
                    .entity(ImmutableMap.<String, Object>of("error", "Producer sequence must be a number"))
                    .build()
        );
      }

      return Optional.empty();
    }
  }
}
