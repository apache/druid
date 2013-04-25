package com.metamx.druid.merger.worker.executor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.metamx.druid.merger.common.index.EventReceiver;
import com.metamx.druid.merger.common.index.EventReceiverProvider;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;
import java.util.List;
import java.util.Map;

@Path("/mmx/worker/v1")
public class EventReceiverResource
{
  private final ObjectMapper jsonMapper;
  private final EventReceiverProvider receivers;

  @Inject
  public EventReceiverResource(ObjectMapper jsonMapper, EventReceiverProvider receivers)
  {
    this.jsonMapper = jsonMapper;
    this.receivers = receivers;
  }

  @POST
  @Path("/firehose/{id}/push-events")
  @Produces("application/json")
  public Response doPush(
      @PathParam("id") String firehoseId,
      List<Map<String, Object>> events
  )
  {
    final Optional<EventReceiver> receiver = receivers.get(firehoseId);

    if (receiver.isPresent()) {
      receiver.get().addAll(events);
      return Response.ok(ImmutableMap.of("eventCount", events.size())).build();
    } else {
      return Response.status(Response.Status.NOT_FOUND).build();
    }
  }
}
