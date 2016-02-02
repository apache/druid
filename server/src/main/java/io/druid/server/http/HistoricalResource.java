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

package io.druid.server.http;

import com.amazonaws.services.dynamodbv2.model.ReturnConsumedCapacity;
import com.google.api.client.util.Lists;
import com.google.common.collect.ImmutableMap;
import io.druid.concurrent.Execs;
import io.druid.segment.loading.SegmentLoaderConfig;
import io.druid.server.coordination.DataSegmentChangeCallback;
import io.druid.server.coordination.DataSegmentChangeRequest;
import io.druid.server.coordination.SegmentManager;
import io.druid.server.coordination.ZkCoordinator;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;

@Path("/druid/historical/v1")
public class HistoricalResource
{
  private final SegmentManager segmentManager;
  private final ExecutorService exec;
  private final List<DataSegmentChangeRequest> pendingRequests = new CopyOnWriteArrayList<>();

  @Inject
  public HistoricalResource(
      SegmentManager segmentManager,
      SegmentLoaderConfig config
  )
  {
    this.segmentManager = segmentManager;
    exec = Execs.multiThreaded(
        config.getNumLoadingThreads(),
        "SegmentChangeRequestProcessing-%s"
    );

  }

  @GET
  @Path("/loadstatus")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getLoadStatus()
  {
    return Response.ok(ImmutableMap.of("cacheInitialized", segmentManager.isStarted())).build();
  }

  @POST
  @Path("/processRequest")
  public Response processSegmentChangeRequest(final DataSegmentChangeRequest request)
  {
    try {
      boolean newRequest = pendingRequests.add(request);
      if (newRequest) {
        exec.submit(new Runnable()
        {
          @Override
          public void run()
          {
            request.go(segmentManager, new DataSegmentChangeCallback()
            {
              @Override
              public void execute()
              {
                pendingRequests.remove(request);
              }
            });
          }
        });
      }
      return Response.ok().build();
    }
    catch (Exception e) {
      return Response.serverError().build();
    }
  }

  @GET
  @Path("/pendingRequests")
  public Response pendingRequests()
  {
    return Response.ok(pendingRequests).build();
  }

}
