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

package io.druid.testing;


import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.api.client.repackaged.com.google.common.base.Throwables;
import org.apache.commons.io.IOUtils;
import java.util.List;

public class DockerConfigProvider  implements IntegrationTestingConfigProvider
{
  private static String lookupIp(){
    try {
      final Process process = Runtime.getRuntime().exec("boot2docker ip");
      process.waitFor();
      final List<String> output = IOUtils.readLines(process.getInputStream());
      return output.get(0);
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @JsonProperty
  private String dockerIp = lookupIp();

  @Override
  public IntegrationTestingConfig get()
  {
    return new IntegrationTestingConfig()
    {
      @Override
      public String getCoordinatorHost()
      {
        return dockerIp+":8081";
      }

      @Override
      public String getIndexerHost()
      {
        return dockerIp+":8090";
      }

      @Override
      public String getRouterHost()
      {
        return dockerIp+ ":8888";
      }

      @Override
      public String getMiddleManagerHost()
      {
        return dockerIp;
      }
    };
  }
}
