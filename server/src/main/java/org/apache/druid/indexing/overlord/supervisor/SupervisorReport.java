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

package org.apache.druid.indexing.overlord.supervisor;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.joda.time.DateTime;

import java.util.Objects;

public class SupervisorReport<T>
{
  private final String id;
  private final DateTime generationTime;
  private final T payload;

  public SupervisorReport(String id, DateTime generationTime, T payload)
  {
    this.id = id;
    this.generationTime = generationTime;
    this.payload = payload;
  }

  @JsonProperty
  public String getId()
  {
    return id;
  }

  @JsonProperty
  public DateTime getGenerationTime()
  {
    return generationTime;
  }

  @JsonProperty
  public T getPayload()
  {
    return payload;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(id, generationTime, payload);
  }

  @Override
  public boolean equals(Object o)
  {
    if (o == this) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final SupervisorReport that = (SupervisorReport) o;
    if (!id.equals(that.id)) {
      return false;
    }
    if (!generationTime.equals(that.generationTime)) {
      return false;
    }
    return payload.equals(that.payload);
  }

  @Override
  public String toString()
  {
    return "{" +
           "id='" + getId() + '\'' +
           ", generationTime=" + getGenerationTime() +
           ", payload=" + payload +
           '}';
  }
}
