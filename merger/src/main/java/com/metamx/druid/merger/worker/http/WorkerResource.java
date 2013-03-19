package com.metamx.druid.merger.worker.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.metamx.common.logger.Logger;
import com.metamx.druid.merger.common.control.TaskControl;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;

/**
 */
@Path("/mmx/worker/v1")
public class WorkerResource
{
  private static final Logger log = new Logger(WorkerResource.class);

  private final ObjectMapper jsonMapper;

  @Inject
  public WorkerResource(
      ObjectMapper jsonMapper
  ) throws Exception
  {
    this.jsonMapper = jsonMapper;
  }

  @POST
  @Path("/control")
  @Consumes("application/json")
  @Produces("application/json")
  public Response doControl(final TaskControl control)
  {
    // TODO
    return Response.ok().build();
  }
}
