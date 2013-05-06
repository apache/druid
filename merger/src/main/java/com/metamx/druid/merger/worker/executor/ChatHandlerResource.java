package com.metamx.druid.merger.worker.executor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.inject.Inject;
import com.metamx.druid.merger.common.index.ChatHandler;
import com.metamx.druid.merger.common.index.ChatHandlerProvider;

import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Response;

@Path("/mmx/worker/v1")
public class ChatHandlerResource
{
  private final ObjectMapper jsonMapper;
  private final ChatHandlerProvider handlers;

  @Inject
  public ChatHandlerResource(ObjectMapper jsonMapper, ChatHandlerProvider handlers)
  {
    this.jsonMapper = jsonMapper;
    this.handlers = handlers;
  }

  @Path("/chat/{id}")
  public Object doTaskChat(
      @PathParam("id") String handlerId
  )
  {
    final Optional<ChatHandler> handler = handlers.get(handlerId);

    if (handler.isPresent()) {
      return handler.get();
    } else {
      return Response.status(Response.Status.NOT_FOUND).build();
    }
  }
}
