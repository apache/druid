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

package org.apache.druid.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.indexing.overlord.supervisor.Supervisor;
import org.apache.druid.indexing.overlord.supervisor.SupervisorSpec;

import java.util.List;
import java.util.Objects;

public class TestSupervisorSpec implements SupervisorSpec
{
  private final String id;
  private final Object data;

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

  @Override
  public String getType()
  {
    return null;
  }

  @Override
  public String getSource()
  {
    return null;
  }

  @JsonProperty
  public Object getData()
  {
    return data;
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
    TestSupervisorSpec that = (TestSupervisorSpec) o;
    return Objects.equals(id, that.id) &&
           Objects.equals(data, that.data);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(id, data);
  }

  @Override
  public String toString()
  {
    return "TestSupervisorSpec{" +
           "id='" + id + '\'' +
           ", data=" + data +
           '}';
  }
}
