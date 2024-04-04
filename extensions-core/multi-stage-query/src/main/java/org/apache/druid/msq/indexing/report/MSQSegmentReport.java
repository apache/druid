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

package org.apache.druid.msq.indexing.report;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

public class MSQSegmentReport
{
  private final String shardSpec;
  private final String details;

  @JsonCreator
  public MSQSegmentReport(@JsonProperty("shardSpec") String shardSpec, @JsonProperty("details") String reason)
  {
    this.shardSpec = shardSpec;
    this.details = reason;
  }

  @JsonProperty
  public String getShardSpec()
  {
    return shardSpec;
  }

  @JsonProperty
  public String getDetails()
  {
    return details;
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
    MSQSegmentReport that = (MSQSegmentReport) o;
    return Objects.equals(shardSpec, that.shardSpec) && Objects.equals(details, that.details);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(shardSpec, details);
  }

  @Override
  public String toString()
  {
    return "MSQSegmentReport{" +
           "shardSpec='" + shardSpec + '\'' +
           ", details='" + details + '\'' +
           '}';
  }
}
