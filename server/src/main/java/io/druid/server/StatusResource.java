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

package io.druid.server;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.druid.initialization.Initialization;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;

/**
 */
@Path("/{a:status|health}")
public class StatusResource
{
  @GET
  @Produces("application/json")
  public Status doGet()
  {
    return getStatus();
  }

  public static Status getStatus()
  {
    return new Status(
        Initialization.class.getPackage().getImplementationVersion(),
        new Memory(Runtime.getRuntime())
    );
  }

  public static class Status
  {
    final String version;
    final Memory memory;

    public Status(
        String version, Memory memory
    )
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

    @Override
    public String toString()
    {
      final String NL = System.getProperty("line.separator");
      return String.format("Druid version - %s", version) + NL;
    }
  }

  public static class ModuleVersion
  {
    final String name;
    final String artifact;
    final String version;

    public ModuleVersion(String name)
    {
      this(name, "", "");
    }

    public ModuleVersion(String name, String artifact, String version)
    {
      this.name = name;
      this.artifact = artifact;
      this.version = version;
    }

    @JsonProperty
    public String getName()
    {
      return name;
    }

    @JsonProperty
    public String getArtifact()
    {
      return artifact;
    }

    @JsonProperty
    public String getVersion()
    {
      return version;
    }

    @Override
    public String toString()
    {
      if (artifact.isEmpty()) {
        return String.format("  - %s ", name);
      } else {
        return String.format("  - %s (%s-%s)", name, artifact, version);
      }
    }
  }

  public static class Memory
  {
    final long maxMemory;
    final long totalMemory;
    final long freeMemory;
    final long usedMemory;

    public Memory(Runtime runtime)
    {
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
