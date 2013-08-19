package com.metamx.druid.http;


import com.fasterxml.jackson.annotation.JsonProperty;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;


@Path("/status")
public class StatusResource
{
  @Inject
  public StatusResource() {}


  @GET
  @Path("/")
  @Produces("application/json")
  public Status getLoadStatus()
  {
    return new Status(
        StatusResource.class.getPackage().getImplementationVersion(),
        new Memory(Runtime.getRuntime())
    );
  }

  public static class Status {
    final String version;
    final Memory memory;

    public Status(String version, Memory memory)
    {
      this.version = version;
      this.memory = memory;
    }

    @JsonProperty
    public String getVersion()
    {
      return version;
    }

    @JsonProperty
    public Memory getMemory()
    {
      return memory;
    }
  }

  public static class Memory {
    final long maxMemory;
    final long totalMemory;
    final long freeMemory;
    final long usedMemory;

    public Memory(Runtime runtime) {
      maxMemory = runtime.maxMemory();
      totalMemory = runtime.totalMemory();
      freeMemory = runtime.freeMemory();
      usedMemory = totalMemory - freeMemory;
    }

    @JsonProperty
    public long getMaxMemory()
    {
      return maxMemory;
    }

    @JsonProperty
    public long getTotalMemory()
    {
      return totalMemory;
    }

    @JsonProperty
    public long getFreeMemory()
    {
      return freeMemory;
    }

    @JsonProperty
    public long getUsedMemory()
    {
      return usedMemory;
    }
  }

}
