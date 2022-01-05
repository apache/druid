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

package org.apache.druid.server.coordinator;

import org.apache.druid.query.aggregation.AggregatorFactory;

import java.util.Arrays;

/**
 * Spec containing Metrics configs for Auto Compaction.
 * This class mimics JSON field names for fields supported in auto compaction with
 * the corresponding fields in ingestion task.
 * This is done for end-user ease of use. Basically, end-user will use the same syntax / JSON structure to set
 * Metrics configs for Auto Compaction as they would for any other ingestion task.
 */
public class UserCompactionTaskMetricsConfig
{
  private final AggregatorFactory[] metricsSpec;

  public UserCompactionTaskMetricsConfig(
      AggregatorFactory[] metricsSpec
  )
  {
    this.metricsSpec = metricsSpec;
  }

  public AggregatorFactory[] getMetricsSpec()
  {
    return metricsSpec;
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
    UserCompactionTaskMetricsConfig that = (UserCompactionTaskMetricsConfig) o;
    return Arrays.equals(metricsSpec, that.metricsSpec);
  }

  @Override
  public int hashCode()
  {
    return Arrays.hashCode(metricsSpec);
  }

  @Override
  public String toString()
  {
    return "UserCompactionTaskMetricsConfig{" +
           "metricsSpec=" + Arrays.toString(metricsSpec) +
           '}';
  }
}
