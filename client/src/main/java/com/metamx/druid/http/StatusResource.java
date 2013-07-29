package com.metamx.druid.http;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;

/**
 */
@Path("/{a:status|health}")
public class StatusResource
{
  @GET
  @Produces("text/plain")
  public Response doGet()
  {
    StringBuffer buf = new StringBuffer();

    Runtime runtime = Runtime.getRuntime();
    long maxMemory = runtime.maxMemory();
    long totalMemory = runtime.totalMemory();
    long freeMemory = runtime.freeMemory();

    buf.append(String.format("Max Memory:\t%,18d\t%1$d%n", maxMemory));
    buf.append(String.format("Total Memory:\t%,18d\t%1$d%n", totalMemory));
    buf.append(String.format("Free Memory:\t%,18d\t%1$d%n", freeMemory));
    buf.append(String.format("Used Memory:\t%,18d\t%1$d%n", totalMemory - freeMemory));

    return Response.ok(buf.toString()).build();
  }
}
