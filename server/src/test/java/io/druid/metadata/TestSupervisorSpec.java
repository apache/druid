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

package io.druid.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.druid.indexing.overlord.supervisor.Supervisor;
import io.druid.indexing.overlord.supervisor.SupervisorSpec;

import java.util.List;

public class TestSupervisorSpec implements SupervisorSpec
{
  private String id;
  private Object data;

  @JsonCreator
  public TestSupervisorSpec(@JsonProperty("id") String id, @JsonProperty("data") Object data)
  {
    this.id = id;
    this.data = data;
  }

  @Override
  @JsonProperty
  public String getId()
  {
    return id;
  }

  @Override
  public Supervisor createSupervisor()
  {
    return null;
  }

  @Override
  public List<String> getDataSources()
  {
    return null;
  }

  @JsonProperty
  public Object getData()
  {
    return data;
  }
}
