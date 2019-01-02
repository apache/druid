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

package org.apache.druid.extensions.watermarking.storage.google;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.java.util.common.StringUtils;

import java.util.Objects;

public class PubsubWatermarkSinkConfig
{
  @JsonProperty
  private String projectId;
  @JsonProperty
  private String topic;

  public String getProjectId()
  {
    return projectId;
  }

  public String getTopic()
  {
    return topic;
  }

  public String getFormattedTopic()
  {
    return StringUtils.format("projects/%s/topics/%s", getProjectId(), getTopic());
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
    PubsubWatermarkSinkConfig that = (PubsubWatermarkSinkConfig) o;
    return Objects.equals(projectId, that.projectId) &&
           Objects.equals(topic, that.topic);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(projectId, topic);
  }

  @Override
  public String toString()
  {
    return "PubsubWatermarkSinkConfig{" +
           "projectId='" + projectId + '\'' +
           ", topic='" + topic + '\'' +
           '}';
  }
}
