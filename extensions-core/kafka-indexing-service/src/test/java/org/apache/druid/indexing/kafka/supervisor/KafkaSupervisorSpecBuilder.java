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

package org.apache.druid.indexing.kafka.supervisor;

import org.apache.druid.segment.indexing.DataSchema;

import java.util.function.Consumer;

/**
 * Builder for a {@link KafkaSupervisorSpec}, which can be sent over a POST call
 * to the Overlord.
 */
public class KafkaSupervisorSpecBuilder
{
  private String id;
  private final DataSchema.Builder dataSchema = new DataSchema.Builder();
  private final KafkaIOConfigBuilder ioConfig = new KafkaIOConfigBuilder();
  private final KafkaTuningConfigBuilder tuningConfig = new KafkaTuningConfigBuilder();

  public KafkaSupervisorSpecBuilder withDataSchema(Consumer<DataSchema.Builder> updateDataSchema)
  {
    updateDataSchema.accept(this.dataSchema);
    return this;
  }

  public KafkaSupervisorSpecBuilder withTuningConfig(Consumer<KafkaTuningConfigBuilder> updateTuningConfig)
  {
    updateTuningConfig.accept(this.tuningConfig);
    return this;
  }

  public KafkaSupervisorSpecBuilder withIoConfig(Consumer<KafkaIOConfigBuilder> updateIOConfig)
  {
    updateIOConfig.accept(this.ioConfig);
    return this;
  }

  public KafkaSupervisorSpecBuilder withId(String id)
  {
    this.id = id;
    return this;
  }

  /**
   * Builds a new {@link KafkaSupervisorSpec} with the specified parameters.
   */
  public KafkaSupervisorSpec build(String dataSource, String topic)
  {
    dataSchema.withDataSource(dataSource);
    ioConfig.withTopic(topic).withTopicPattern(null);
    return build();
  }

  /**
   * Builds a new {@link KafkaSupervisorSpec} which reads from multiple toppics
   * that satisfy the given {@code topicPattern}.
   */
  public KafkaSupervisorSpec buildWithTopicPattern(String dataSource, String topicPattern)
  {
    dataSchema.withDataSource(dataSource);
    ioConfig.withTopic(null).withTopicPattern(topicPattern);
    return build();
  }

  private KafkaSupervisorSpec build()
  {
    return new KafkaSupervisorSpec(
        id,
        null,
        dataSchema.build(),
        tuningConfig.build(),
        ioConfig.build(),
        null,
        false,
        // Jackson injected params, not needed while posting a supervisor to the Overlord
        null, null, null, null, null, null, null, null, null
    );
  }
}
