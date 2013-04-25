package com.metamx.druid.merger.common.index;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.metamx.druid.indexer.data.MapInputRowParser;
import com.metamx.druid.input.InputRow;
import com.metamx.druid.realtime.firehose.Firehose;
import com.metamx.druid.realtime.firehose.FirehoseFactory;
import com.metamx.emitter.EmittingLogger;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Builds firehoses that accept events through the {@link EventReceiver} interface. Can also register these
 * firehoses with an {@link EventReceiverProvider}.
 */
@JsonTypeName("receiver")
public class EventReceiverFirehoseFactory implements FirehoseFactory
{
  private static final EmittingLogger log = new EmittingLogger(EventReceiverFirehoseFactory.class);
  private static final int DEFAULT_BUFFER_SIZE = 100000;

  private final String firehoseId;
  private final int bufferSize;
  private final MapInputRowParser parser;
  private final Optional<EventReceiverProvider> eventReceiverProvider;

  @JsonCreator
  public EventReceiverFirehoseFactory(
      @JsonProperty("firehoseId") String firehoseId,
      @JsonProperty("bufferSize") Integer bufferSize,
      @JsonProperty("parser") MapInputRowParser parser,
      @JacksonInject("eventReceiverProvider") EventReceiverProvider eventReceiverProvider
  )
  {
    this.firehoseId = Preconditions.checkNotNull(firehoseId, "firehoseId");
    this.bufferSize = bufferSize == null || bufferSize <= 0 ? DEFAULT_BUFFER_SIZE : bufferSize;
    this.parser = Preconditions.checkNotNull(parser, "parser");
    this.eventReceiverProvider = Optional.fromNullable(eventReceiverProvider);
  }

  @Override
  public Firehose connect() throws IOException
  {
    log.info("Connecting firehose: %s", firehoseId);

    final EventReceiverFirehose firehose = new EventReceiverFirehose();

    if (eventReceiverProvider.isPresent()) {
      eventReceiverProvider.get().register(firehoseId, firehose);
    }

    return firehose;
  }

  @JsonProperty
  public String getFirehoseId()
  {
    return firehoseId;
  }

  @JsonProperty
  public int getBufferSize()
  {
    return bufferSize;
  }

  @JsonProperty
  public MapInputRowParser getParser()
  {
    return parser;
  }

  public class EventReceiverFirehose implements EventReceiver, Firehose
  {
    private final BlockingQueue<Map<String, Object>> buffer;
    private final Object readLock = new Object();
    private volatile Map<String, Object> nextEvent = null;
    private volatile boolean closed = false;

    public EventReceiverFirehose()
    {
      this.buffer = new ArrayBlockingQueue<Map<String, Object>>(bufferSize);
    }

    @Override
    public void addAll(Collection<Map<String, Object>> events)
    {
      log.debug("Adding %,d events to firehose: %s", events.size(), firehoseId);

      try {
        for (final Map<String, Object> event : events) {
          boolean added = false;
          while (!closed && !added) {
            added = buffer.offer(event, 500, TimeUnit.MILLISECONDS);
          }

          if (!added) {
            throw new IllegalStateException("Cannot add events to closed firehose!");
          }
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw Throwables.propagate(e);
      }
    }

    @Override
    public boolean hasMore()
    {
      synchronized (readLock) {
        try {
          while (!closed && nextEvent == null) {
            nextEvent = buffer.poll(500, TimeUnit.MILLISECONDS);
          }
        }
        catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw Throwables.propagate(e);
        }

        return nextEvent != null;
      }
    }

    @Override
    public InputRow nextRow()
    {
      synchronized (readLock) {
        final Map<String, Object> event = nextEvent;

        if (event == null) {
          throw new NoSuchElementException();
        } else {
          // If nextEvent is unparseable, don't return it again
          nextEvent = null;
          return parser.parse(event);
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
    public void close() throws IOException
    {
      log.info("Firehose closing.");
      closed = true;

      if (eventReceiverProvider.isPresent()) {
        eventReceiverProvider.get().unregister(firehoseId);
      }
    }
  }
}
