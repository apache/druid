/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
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
 */

package io.druid.indexing.worker.http;

import com.google.api.client.util.Lists;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteSource;
import com.google.common.io.InputSupplier;
import com.google.inject.Inject;
import com.metamx.common.logger.Logger;
import io.druid.indexing.overlord.ForkingTaskRunner;
import io.druid.indexing.overlord.TaskRunnerWorkItem;
import io.druid.indexing.worker.Worker;
import io.druid.indexing.worker.WorkerCuratorCoordinator;

import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Response;
import java.io.InputStream;

/**
 */
@Path("/druid/worker/v1")
public class WorkerResource
{
  private static final Logger log = new Logger(WorkerResource.class);
  private static String DISABLED_VERSION = "";

  private final Worker enabledWorker;
  private final Worker disabledWorker;
  private final WorkerCuratorCoordinator curatorCoordinator;
  private final ForkingTaskRunner taskRunner;

  @Inject
  public WorkerResource(
      Worker worker,
      WorkerCuratorCoordinator curatorCoordinator,
      ForkingTaskRunner taskRunner

  ) throws Exception
  {
    this.enabledWorker = worker;
    this.disabledWorker = new Worker(worker.getHost(), worker.getIp(), worker.getCapacity(), DISABLED_VERSION);
    this.curatorCoordinator = curatorCoordinator;
    this.taskRunner = taskRunner;
  }


  @POST
  @Path("/disable")
  @Produces("application/json")
  public Response doDisable()
  {
    try {
      curatorCoordinator.updateWorkerAnnouncement(disabledWorker);
      return Response.ok(ImmutableMap.of(disabledWorker.getHost(), "disabled")).build();
    }
    catch (Exception e) {
      return Response.serverError().build();
    }
  }

  @POST
  @Path("/enable")
  @Produces("application/json")
  public Response doEnable()
  {
    try {
      curatorCoordinator.updateWorkerAnnouncement(enabledWorker);
      return Response.ok(ImmutableMap.of(enabledWorker.getHost(), "enabled")).build();
    }
    catch (Exception e) {
      return Response.serverError().build();
    }
  }

  @GET
  @Path("/enabled")
  @Produces("application/json")
  public Response isEnabled()
  {
    try {
      final Worker theWorker = curatorCoordinator.getWorker();
      final boolean enabled = !theWorker.getVersion().equalsIgnoreCase(DISABLED_VERSION);
      return Response.ok(ImmutableMap.of(theWorker.getHost(), enabled)).build();
    }
    catch (Exception e) {
      return Response.serverError().build();
    }
  }

  @GET
  @Path("/tasks")
  @Produces("application/json")
  public Response getTasks()
  {
    try {
      return Response.ok(
          Lists.newArrayList(
              Collections2.transform(
                  taskRunner.getKnownTasks(),
                  new Function<TaskRunnerWorkItem, String>()
                  {
                    @Override
                    public String apply(TaskRunnerWorkItem input)
                    {
                      return input.getTaskId();
                    }
                  }
              )
          )
      ).build();
    }
    catch (Exception e) {
      return Response.serverError().build();
    }
  }

  @POST
  @Path("/task/{taskid}/shutdown")
  @Produces("application/json")
  public Response doShutdown(@PathParam("taskid") String taskid)
  {
    try {
      taskRunner.shutdown(taskid);
    }
    catch (Exception e) {
      log.error(e, "Failed to issue shutdown for task: %s", taskid);
      return Response.serverError().build();
    }
    return Response.ok(ImmutableMap.of("task", taskid)).build();
  }

  @GET
  @Path("/task/{taskid}/log")
  @Produces("text/plain")
  public Response doGetLog(
      @PathParam("taskid") String taskid,
      @QueryParam("offset") @DefaultValue("0") long offset
  )
  {
    final Optional<ByteSource> stream = taskRunner.streamTaskLog(taskid, offset);

    if (stream.isPresent()) {
      try (InputStream logStream = stream.get().openStream()) {
        return Response.ok(logStream).build();
      }
      catch (Exception e) {
        log.warn(e, "Failed to read log for task: %s", taskid);
        return Response.serverError().build();
      }
    } else {
      return Response.status(Response.Status.NOT_FOUND).build();
    }
  }
}
