/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
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

package org.apache.druid.indexing.overlord.autoscaling.ec2;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.java.util.common.StringUtils;

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
      return StringUtils.encodeBase64String(jsonMapper.writeValueAsBytes(this));
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    GalaxyEC2UserData that = (GalaxyEC2UserData) o;

    if (env != null ? !env.equals(that.env) : that.env != null) {
      return false;
    }
    if (jsonMapper != null ? !jsonMapper.equals(that.jsonMapper) : that.jsonMapper != null) {
      return false;
    }
    if (type != null ? !type.equals(that.type) : that.type != null) {
      return false;
    }
    if (version != null ? !version.equals(that.version) : that.version != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    int result = jsonMapper != null ? jsonMapper.hashCode() : 0;
    result = 31 * result + (env != null ? env.hashCode() : 0);
    result = 31 * result + (version != null ? version.hashCode() : 0);
    result = 31 * result + (type != null ? type.hashCode() : 0);
    return result;
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
