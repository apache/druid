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

package io.druid.indexing.overlord.setup;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.client.repackaged.com.google.common.base.Throwables;
import io.druid.guice.annotations.Json;
import org.apache.commons.codec.binary.Base64;

/**
 */
public class GalaxyEC2UserData implements EC2UserData<GalaxyEC2UserData>
{
  private final ObjectMapper jsonMapper;
  private final String env;
  private final String version;
  private final String type;

  @JsonCreator
  public GalaxyEC2UserData(
      @JacksonInject @Json ObjectMapper jsonMapper,
      @JsonProperty("env") String env,
      @JsonProperty("version") String version,
      @JsonProperty("type") String type
  )
  {
    this.jsonMapper = jsonMapper;
    this.env = env;
    this.version = version;
    this.type = type;
  }

  @JsonProperty
  public String getEnv()
  {
    return env;
  }

  @JsonProperty
  public String getVersion()
  {
    return version;
  }

  @JsonProperty
  public String getType()
  {
    return type;
  }

  @Override
  public GalaxyEC2UserData withVersion(String ver)
  {
    return new GalaxyEC2UserData(jsonMapper, env, ver, type);
  }

  @Override
  public String getUserDataBase64()
  {
    try {
      return Base64.encodeBase64String(jsonMapper.writeValueAsBytes(this));
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public String toString()
  {
    return "GalaxyUserData{" +
           "env='" + env + '\'' +
           ", version='" + version + '\'' +
           ", type='" + type + '\'' +
           '}';
  }
}
