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

package org.apache.druid.indexing.kafka;

import java.util.Set;

public class KafkaConsumerMetric
{
  private final String kafkaMetricName;
  private final String druidMetricName;
  private final Set<String> dimensions;
  private final MetricType metricType;

  public KafkaConsumerMetric(
      String kafkaMetricName,
      String druidMetricName,
      Set<String> dimensions,
      MetricType metricType
  )
  {
    this.kafkaMetricName = kafkaMetricName;
    this.druidMetricName = druidMetricName;
    this.dimensions = dimensions;
    this.metricType = metricType;
  }

  public String getKafkaMetricName()
  {
    return kafkaMetricName;
  }

  public String getDruidMetricName()
  {
    return druidMetricName;
  }

  public Set<String> getDimensions()
  {
    return dimensions;
  }

  public MetricType getMetricType()
  {
    return metricType;
  }

  @Override
  public String toString()
  {
    return "KafkaConsumerMetric{" +
           "kafkaMetricName='" + kafkaMetricName + '\'' +
           ", druidMetricName='" + druidMetricName + '\'' +
           ", dimensions=" + dimensions +
           ", metricType=" + metricType +
           '}';
  }

  public enum MetricType
  {
    GAUGE,
    COUNTER
  }
}
