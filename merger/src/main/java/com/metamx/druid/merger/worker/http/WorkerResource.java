package com.metamx.druid.merger.worker.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.metamx.common.logger.Logger;
import com.metamx.druid.merger.coordinator.TaskRunner;

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
  private final TaskRunner taskRunner;

  @Inject
  public WorkerResource(
      ObjectMapper jsonMapper,
      TaskRunner taskRunner

  ) throws Exception
  {
    this.jsonMapper = jsonMapper;
    this.taskRunner = taskRunner;
  }

  @POST
  @Path("/shutdown")
  @Consumes("application/json")
  @Produces("application/json")
  public Response doShutdown(final String taskId)
  {
    try {
      taskRunner.shutdown(taskId);
    }
    catch (Exception e) {
      return Response.serverError().build();
    }
    return Response.ok().build();
  }
}
