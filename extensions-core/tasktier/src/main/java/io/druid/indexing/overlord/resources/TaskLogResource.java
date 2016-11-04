/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.indexing.overlord.resources;

import com.google.common.base.Preconditions;
import io.druid.indexing.common.tasklogs.LogUtils;
import io.druid.indexing.overlord.TierLocalTaskRunner;
import io.druid.java.util.common.logger.Logger;
import io.druid.server.DruidNode;

import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;

@Path(TaskLogResource.PATH)
public class TaskLogResource
{
  private static final Logger log = new Logger(TaskLogResource.class);
  public static final String PATH = "/druid/worker/v1/task/log";
  public static final String OFFSET_PARAM = "offset";

  @GET
  @Produces(MediaType.TEXT_PLAIN)
  public Response getLog(
      @QueryParam(OFFSET_PARAM) @DefaultValue("0") long offset
  )
  {
    final File logFile = new File(TierLocalTaskRunner.LOG_FILE_NAME);

    if (!logFile.exists()) {
      return Response.status(Response.Status.NOT_FOUND).build();
    }

    if (!logFile.canRead()) {
      return Response.status(Response.Status.FORBIDDEN).build();
    }

    try {
      return Response.ok(LogUtils.streamFile(logFile, offset)).build();
    }
    catch (FileNotFoundException e) {
      log.wtf(e, "File [%s] not found, but was found!?", logFile);
      return Response.status(Response.Status.GONE).build();
    }
    catch (IOException e) {
      log.error(e, "Error fetching log file");
      return Response.serverError().build();
    }
  }

  public static URL buildURL(DruidNode targetNode, long offset) throws MalformedURLException
  {
    Preconditions.checkArgument(offset >= 0, "offset must be >= 0");
    return new URL(
        "http",
        targetNode.getHost(),
        targetNode.getPort(),
        String.format("%s?%s=%d", PATH, OFFSET_PARAM, offset)
    );
  }
}
