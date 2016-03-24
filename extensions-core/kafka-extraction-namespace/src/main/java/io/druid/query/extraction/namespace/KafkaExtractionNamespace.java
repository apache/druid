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

package io.druid.query.extraction.namespace;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;

import javax.validation.constraints.NotNull;
import java.util.Properties;

/**
 *
 */
@JsonTypeName("kafka")
public class KafkaExtractionNamespace implements ExtractionNamespace
{
  @JsonProperty
  private final String kafkaTopic;

  @JsonProperty
  private final Properties kafkaProperties;

  @JsonCreator
  public KafkaExtractionNamespace(
      @NotNull @JsonProperty(value = "kafkaTopic", required = true) final String kafkaTopic,
      @NotNull @JsonProperty(value = "kafkaProperites", required = true) final Properties kafkaProperties
  )
  {
    Preconditions.checkNotNull(kafkaTopic, "kafkaTopic required");
    this.kafkaTopic = kafkaTopic;
    this.kafkaProperties = kafkaProperties;
  }

  public String getKafkaTopic()
  {
    return kafkaTopic;
  }

  public Properties getKafkaProperties()
  {
    return kafkaProperties;
  }

  @Override
  public long getPollMs()
  {
    return 0L;
  }

  @Override
  public String toString()
  {
    return String.format("KafkaExtractionNamespace = { kafkaTopic = '%s' }", kafkaTopic);
  }
}
