package com.metamx.druid.merger.coordinator.http;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.metamx.common.logger.Logger;
import com.metamx.druid.merger.common.TaskStatus;
import com.metamx.druid.merger.common.task.MergeTask;
import com.metamx.druid.merger.common.task.Task;
import com.metamx.druid.merger.coordinator.TaskQueue;
import com.metamx.druid.merger.coordinator.config.IndexerCoordinatorConfig;
import com.metamx.emitter.service.ServiceEmitter;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;

/**
 */
@Path("/mmx/merger/v1")
public class IndexerCoordinatorResource
{
  private static final Logger log = new Logger(IndexerCoordinatorResource.class);

  private final IndexerCoordinatorConfig config;
  private final ServiceEmitter emitter;
  private final TaskQueue tasks;

  @Inject
  public IndexerCoordinatorResource(
      IndexerCoordinatorConfig config,
      ServiceEmitter emitter,
      TaskQueue tasks

  ) throws Exception
  {
    this.config = config;
    this.emitter = emitter;
    this.tasks = tasks;
  }

  @POST
  @Path("/merge")
  @Consumes("application/json")
  @Produces("application/json")
  public Response doMerge(
      final MergeTask task
  )
  {
    // legacy endpoint
    return doIndex(task);
  }

  @POST
  @Path("/index")
  @Consumes("application/json")
  @Produces("application/json")
  public Response doIndex(
      final Task task
  )
  {
    // verify against whitelist
    if (config.isWhitelistEnabled() && !config.getWhitelistDatasources().contains(task.getDataSource())) {
      return Response.status(Response.Status.BAD_REQUEST)
                     .entity(
                         ImmutableMap.of(
                             "error",
                             String.format("dataSource[%s] is not whitelisted", task.getDataSource())
                         )
                     )
                     .build();
    }

    tasks.add(task);
    return okResponse(task.getId());
  }

  @GET
  @Path("/status/{taskid}")
  @Produces("application/json")
  public Response doStatus(@PathParam("taskid") String taskid)
  {
    final Optional<TaskStatus> status = tasks.getStatus(taskid);
    if (!status.isPresent()) {
      return Response.status(Response.Status.NOT_FOUND).build();
    } else {
      return Response.ok().entity(status.get()).build();
    }
  }

  private Response okResponse(final String taskid)
  {
    return Response.ok(ImmutableMap.of("task", taskid)).build();
  }
}
