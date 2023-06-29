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

package org.apache.druid.testsEx.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.util.List;

public class ServiceConfig
{
  protected List<ServiceInstance> instances;

  public ServiceConfig(
      List<ServiceInstance> instances
  )
  {
    this.instances = instances;
  }

  @JsonProperty("instances")
  @JsonInclude(Include.NON_NULL)
  public List<ServiceInstance> instances()
  {
    return instances;
  }

  @Override
  public String toString()
  {
    return TestConfigs.toYaml(this);
  }

  /**
   * YAML description of a ZK cluster. Converted to
   * {@link org.apache.druid.curator.CuratorConfig}
   */
  public static class ZKConfig extends ServiceConfig
  {
    /**
     * Amount of time to wait for ZK to become ready.
     * Defaults to 5 seconds.
     */
    private final int startTimeoutSecs;

    @JsonCreator
    public ZKConfig(
        @JsonProperty("startTimeoutSecs") int startTimeoutSecs,
        @JsonProperty("instances") List<ServiceInstance> instances
    )
    {
      super(instances);
      this.startTimeoutSecs = startTimeoutSecs;
    }

    @JsonProperty("startTimeoutSecs")
    public int startTimeoutSecs()
    {
      return startTimeoutSecs;
    }
  }

  /**
   * Represents a Druid service (of one or more instances) running
   * in the test cluster. The service name comes from the key used
   * in the {@code druid} map: <code><pre>
   * druid:
   *   broker:  # <-- key (service name)
   *     if: config-tag
   *     instances:
   *       ...
   * </pre></code>
   *
   * Where {@code config-tag} is a string that indicates a config
   * option. At present there are two:
   * <ul>
   * <li>{@code middleManager}: cluster users the Middle Manager.</li>
   * <li>{@code indexer}: cluster uses the Indexer.</li>
   * <li>
   *
   * A service is included in the resolved config only if the corresponding
   * config tag is set.
   */
  public static class DruidConfig extends ServiceConfig
  {
    private final String ifTag;

    @JsonCreator
    public DruidConfig(
        @JsonProperty("if") String ifTag,
        @JsonProperty("instances") List<ServiceInstance> instances
    )
    {
      super(instances);
      this.ifTag = ifTag;
    }

    @Nullable
    @JsonProperty("if")
    @JsonInclude(Include.NON_NULL)
    public String ifTag()
    {
      return ifTag;
    }
  }
}
