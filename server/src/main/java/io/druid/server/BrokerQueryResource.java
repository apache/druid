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

package io.druid.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.jaxrs.smile.SmileMediaTypes;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.metamx.common.guava.Sequences;
import com.metamx.emitter.service.ServiceEmitter;
import io.druid.client.TimelineServerView;
import io.druid.client.selector.ServerSelector;
import io.druid.guice.annotations.Json;
import io.druid.guice.annotations.Smile;
import io.druid.query.LocatedSegmentDescriptor;
import io.druid.query.Query;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.SegmentDescriptor;
import io.druid.server.coordination.DruidServerMetadata;
import io.druid.server.initialization.ServerConfig;
import io.druid.server.log.RequestLogger;
import io.druid.timeline.TimelineLookup;
import io.druid.timeline.TimelineObjectHolder;
import io.druid.timeline.partition.PartitionChunk;
import org.joda.time.Interval;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

/**
 */
@Path("/druid/v2/")
public class BrokerQueryResource extends QueryResource
{
  private final TimelineServerView brokerServerView;

  @Inject
  public BrokerQueryResource(
      ServerConfig config,
      @Json ObjectMapper jsonMapper,
      @Smile ObjectMapper smileMapper,
      QuerySegmentWalker texasRanger,
      ServiceEmitter emitter,
      RequestLogger requestLogger,
      QueryManager queryManager,
      TimelineServerView brokerServerView
  )
  {
    super(config, jsonMapper, smileMapper, texasRanger, emitter, requestLogger, queryManager);
    this.brokerServerView = brokerServerView;
  }

  @POST
  @Path("/candidates")
  @Produces({MediaType.APPLICATION_JSON, SmileMediaTypes.APPLICATION_JACKSON_SMILE})
  @Consumes({MediaType.APPLICATION_JSON, SmileMediaTypes.APPLICATION_JACKSON_SMILE, APPLICATION_SMILE})
  public Response getQueryTargets(
      InputStream in,
      @QueryParam("pretty") String pretty,
      @Context final HttpServletRequest req // used only to get request content-type and remote address
  ) throws IOException
  {
    final String reqContentType = req.getContentType();
    final boolean isSmile = SmileMediaTypes.APPLICATION_JACKSON_SMILE.equals(reqContentType)
                            || APPLICATION_SMILE.equals(reqContentType);
    final String contentType = isSmile ? SmileMediaTypes.APPLICATION_JACKSON_SMILE : MediaType.APPLICATION_JSON;

    ObjectMapper objectMapper = isSmile ? smileMapper : jsonMapper;
    final ObjectWriter jsonWriter = pretty != null
                                    ? objectMapper.writerWithDefaultPrettyPrinter()
                                    : objectMapper.writer();

    try {
      Query<?> query = objectMapper.readValue(in, Query.class);
      TimelineLookup<String, ServerSelector> timeline = brokerServerView.getTimeline(query.getDataSource());
      if (timeline == null) {
        return Response.ok(Sequences.empty()).build();
      }
      List<LocatedSegmentDescriptor> located = Lists.newArrayList();
      for (Interval interval : query.getIntervals()) {
        for (TimelineObjectHolder<String, ServerSelector> holder : timeline.lookup(interval)) {
          for (PartitionChunk<ServerSelector> chunk : holder.getObject()) {
            ServerSelector selector = chunk.getObject();
            final SegmentDescriptor descriptor = new SegmentDescriptor(
                holder.getInterval(), holder.getVersion(), chunk.getChunkNumber()
            );
            List<DruidServerMetadata> candidates = selector.getCandidates();
            located.add(new LocatedSegmentDescriptor(descriptor, candidates));
          }
        }
      }
      return Response.ok(jsonWriter.writeValueAsString(located), contentType).build();
    }
    catch (Exception e) {
      return Response.serverError().type(contentType).entity(
          jsonWriter.writeValueAsString(
              ImmutableMap.of(
                  "error", e.getMessage() == null ? "null exception" : e.getMessage()
              )
          )
      ).build();
    }
  }
}
