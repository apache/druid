/*
 * Druid - a distributed column store.
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
 *
 * This file Copyright (C) 2014 N3TWORK, Inc. and contributed to the Druid project
 * under the Druid Corporate Contributor License Agreement.
 */

package io.druid.indexing.common.index;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.metamx.emitter.EmittingLogger;
import io.druid.data.input.Firehose;
import io.druid.data.input.FirehoseFactory;
import io.druid.data.input.InputRow;
import io.druid.data.input.impl.MapInputRowParser;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.utils.Runnables;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Collection;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Builds firehoses that accept events through the {@link io.druid.indexing.common.index.EventReceiver} interface. Can also register these
 * firehoses with an {@link io.druid.indexing.common.index.ServiceAnnouncingChatHandlerProvider}.
 * <p/>
 * This class is similar to EventReceiverFirehoseFactory, except it is designed to be used with IndexTasks. The
 * data added can be re-read, and it accepts a "/finish" REST call to indicate we're done writing.
 */
@JsonTypeName("finite_receiver")
public class FiniteEventReceiverFirehoseFactory implements FirehoseFactory
{
  private static final EmittingLogger log = new EmittingLogger(FiniteEventReceiverFirehoseFactory.class);

  private final String serviceName;
  private final MapInputRowParser parser;
  private final Optional<ChatHandlerProvider> chatHandlerProvider;
  private final DefaultObjectMapper objectMapper;
  private volatile File dataFile;
  private volatile boolean inputFinished = false;
  private volatile boolean registered = false;

  @JsonCreator
  public FiniteEventReceiverFirehoseFactory(
      @JsonProperty("serviceName") String serviceName,
      @JsonProperty("parser") MapInputRowParser parser,
      @JacksonInject ChatHandlerProvider chatHandlerProvider,
      @JacksonInject DefaultObjectMapper objectMapper
  )
  {
    this.serviceName = Preconditions.checkNotNull(serviceName, "serviceName");
    this.parser = Preconditions.checkNotNull(parser, "parser");
    this.chatHandlerProvider = Optional.fromNullable(chatHandlerProvider);
    this.objectMapper = Preconditions.checkNotNull(objectMapper, "defaultObjectMapper");
  }

  @Override
  public Firehose connect() throws IOException
  {
    log.info("Connecting firehose: %s", serviceName);
    if (dataFile == null && !inputFinished) {
      dataFile = File.createTempFile("druid-" + serviceName + "-", ".tmp");
      dataFile.deleteOnExit();

    }

    final FiniteEventReceiverFirehose firehose = new FiniteEventReceiverFirehose();

    if (!registered && !inputFinished) {
      if (chatHandlerProvider.isPresent()) {
        log.info("Found chathandler of class[%s]", chatHandlerProvider.get().getClass().getName());
        chatHandlerProvider.get().register(serviceName, firehose);
        if (serviceName.contains(":")) {
          chatHandlerProvider.get().register(serviceName.replaceAll(".*:", ""), firehose); // rofl
        }
        registered = true;
      } else {
        log.info("No chathandler detected");
      }
    }
    return firehose;
  }

  @JsonProperty
  public String getServiceName()
  {
    return serviceName;
  }

  @JsonProperty
  public MapInputRowParser getParser()
  {
    return parser;
  }

  public class FiniteEventReceiverFirehose implements ChatHandler, Firehose
  {
    private volatile boolean closed = false;
    private PrintWriter writer;
    private BufferedReader reader;
    private InputRow nextRow;
    private boolean doneReading = false;

    public FiniteEventReceiverFirehose() throws FileNotFoundException
    {
      if (!inputFinished) {
        writer = new PrintWriter(dataFile);
      }
    }

    @POST
    @Path("/push-events")
    @Produces("application/json")
    public Response addAll(Collection<Map<String, Object>> events)
    {
      if (closed || inputFinished) {
        throw new IllegalStateException("Cannot add events to closed firehose!");
      }

      log.debug("Adding %,d events to firehose: %s", events.size(), serviceName);

      try {
        for (final Map<String, Object> event : events) {
          // Might throw an exception. We'd like that to happen now, instead of while adding to the row buffer.
          parser.parse(event);
          writer.println(objectMapper.writeValueAsString(event));
        }
        writer.flush();

        return Response.ok().entity(ImmutableMap.of("eventCount", events.size())).build();
      } catch (IOException e) {
        return Response.serverError().entity(ImmutableMap.of("error", e.getMessage())).build();
      }
    }

    @Override
    public boolean hasMore()
    {
      try {
        while (!closed && !inputFinished) {
          try {
            dataFile.wait(1000);
          } catch (IllegalMonitorStateException ignored) {
          }
        }

        if (reader == null) {
          reader = new BufferedReader(new FileReader(dataFile));
        }

        if (nextRow == null && !doneReading) {
          String line = reader.readLine();
          if (line == null) {
            doneReading = true;
          } else {
            nextRow = parser.parse(objectMapper.readValue(line, Map.class));
          }
        }

        return nextRow != null;

      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw Throwables.propagate(e);
      } catch (IOException e) {
        throw Throwables.propagate(e);
      }


    }


    @Override
    public InputRow nextRow()
    {
      if (!hasMore()) { // reads next row if necessary
        throw new NoSuchElementException();
      }

      InputRow result = nextRow;
      nextRow = null;
      return result;
    }

    @Override
    public Runnable commit()
    {
      return Runnables.getNoopRunnable();
    }

    @POST
    @Path("/finish")
    @Produces("application/json")
    public Response finish()
    {
      if (!inputFinished) {
        log.info("Input finished. No more data, please.");
        inputFinished = true;

        if (writer != null) {
          writer.close();
        }

        try {
          dataFile.notifyAll();
        } catch (IllegalMonitorStateException ignored) {
        }
      }

      return Response.ok().build();
    }

    @Override
    public void close() throws IOException
    {
      log.info("Firehose closing.");
      finish();
      closed = true;


      if (registered && chatHandlerProvider.isPresent()) {
        chatHandlerProvider.get().unregister(serviceName);
        registered = false;
      }
    }
  }
}
