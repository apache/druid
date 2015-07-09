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

package io.druid.server.http;

import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Inject;
import com.metamx.common.lifecycle.Lifecycle;
import com.metamx.common.logger.Logger;

import javax.ws.rs.DELETE;
import javax.ws.rs.Path;
import javax.ws.rs.core.Response;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Path("/shutdown")
public class ShutdownResource
{
  private final Lifecycle lifecycle;
  @Inject
  public ShutdownResource(
      Lifecycle lifecycle
  ){
    this.lifecycle = lifecycle;
  }
  private static final Logger log = new Logger(ShutdownResource.class);
  private final ListeningScheduledExecutorService executorService = MoreExecutors.listeningDecorator(Executors.newScheduledThreadPool(1));

  @DELETE
  public Response shutDown()
  {
    log.info("Received shutdown request");
    executorService.schedule(
        new Runnable()
        {
          @Override
          public void run()
          {
            System.exit(0);
          }
        },
        1,
        TimeUnit.SECONDS
    );
    executorService.shutdown();
    return Response.status(Response.Status.ACCEPTED).build();
  }
}
