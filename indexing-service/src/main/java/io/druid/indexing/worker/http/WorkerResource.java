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

package io.druid.indexing.worker.http;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.io.ByteSource;
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
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;

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
  @Produces(MediaType.APPLICATION_JSON)
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
  @Produces(MediaType.APPLICATION_JSON)
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
  @Produces(MediaType.APPLICATION_JSON)
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
  @Produces(MediaType.APPLICATION_JSON)
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
  @Produces(MediaType.APPLICATION_JSON)
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
      try {
        return Response.ok(stream.get().openStream()).build();
      }
      catch (IOException e) {
        log.warn(e, "Failed to read log for task: %s", taskid);
        return Response.serverError().build();
      }
    } else {
      return Response.status(Response.Status.NOT_FOUND).build();
    }
  }
}
