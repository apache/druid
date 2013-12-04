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
import com.google.inject.Injector;
import io.druid.initialization.DruidModule;
import io.druid.initialization.Initialization;
import io.druid.server.initialization.ExtensionsConfig;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 */
@Path("/{a:status|health}")
public class StatusResource
{
  @GET
  @Produces("application/json")
  public Status doGet()
  {
    return new Status(
        Initialization.class.getPackage().getImplementationVersion(),
        getVersion("/druid-api.version"),
        getExtensionVersions(),
        new Memory(Runtime.getRuntime())
    );
  }

  /**
   * Load the extensions list and return the implementation-versions
   *
   * @return map of extensions loaded with their respective implementation versions.
   */
  private Map<String, String> getExtensionVersions()
  {
    final Injector injector = Initialization.makeStartupInjector();
    final ExtensionsConfig config = injector.getInstance(ExtensionsConfig.class);
    final List<DruidModule> druidModules = Initialization.getFromExtensions(config, DruidModule.class);
    Map<String, String> moduleVersions = new HashMap<>();
    for (DruidModule module : druidModules) {
      Package pkg = module.getClass().getPackage();
      moduleVersions.put(pkg.getImplementationTitle(), pkg.getImplementationVersion());
    }
    return moduleVersions;
  }

  /**
   * Load properties files from the classpath and return version number
   *
   * @param versionFile
   *
   * @return version number
   */
  private String getVersion(String versionFile)
  {

    Properties properties = new Properties();
    try {
      InputStream is = StatusResource.class.getResourceAsStream(versionFile);
      if (is == null) {
        return null;
      }
      properties.load(is);
    }
    catch (IOException e) {
//      e.printStackTrace();
    }
    return properties.getProperty("version");
  }

  public static class Status
  {
    final String serverVersion;
    final String apiVersion;
    final Map<String, String> extensionsVersion;
    final Memory memory;

    public Status(
        String serverVersion,
        String apiVersion,
        Map<String, String> extensionsVersion,
        Memory memory
    )
    {
      this.serverVersion = serverVersion;
      this.apiVersion = apiVersion;
      this.extensionsVersion = extensionsVersion;
      this.memory = memory;
    }

    @JsonProperty
    public String getServerVersion()
    {
      return serverVersion;
    }

    @JsonProperty
    public String getApiVersion()
    {
      return apiVersion;
    }

    @JsonProperty
    public Map<String, String> getExtensionsVersion()
    {
      return extensionsVersion;
    }

    @JsonProperty
    public Memory getMemory()
    {
      return memory;
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
