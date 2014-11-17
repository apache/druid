package io.druid.server.http;

import com.google.common.collect.ImmutableMap;
import io.druid.server.coordination.ZkCoordinator;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;

@Path("/druid/historical/v1")
public class HistoricalResource
{
  private final ZkCoordinator coordinator;

  @Inject
  public HistoricalResource(
      ZkCoordinator coordinator
  )
  {
    this.coordinator = coordinator;
  }

  @GET
  @Path("/loadstatus")
  @Produces("application/json")
  public Response getLoadStatus()
  {
    return Response.ok(ImmutableMap.of("cacheInitialized", coordinator.isStarted())).build();
  }
}
