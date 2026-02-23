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

package org.apache.druid.testing.embedded.indexing;

import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.indexing.kafka.simulate.KafkaResource;
import org.apache.druid.indexing.overlord.supervisor.SupervisorSpec;
import org.apache.druid.testing.embedded.StreamIngestResource;
import org.joda.time.Period;

import java.util.Map;

public class KafkaIndexDataFormatsTest extends StreamIndexDataFormatsTestBase
{
  private final KafkaResource kafka = new KafkaResource();

  @Override
  protected StreamIngestResource<?> getStreamResource()
  {
    return kafka;
  }

  @Override
  public SupervisorSpec createSupervisorWithParser(String dataSource, String topic, Map<String, Object> parserMap)
  {
    return MoreResources.Supervisor.KAFKA_JSON
        .get()
        .withDataSchema(
            schema -> schema
                .withTimestamp(new TimestampSpec("timestamp", null, null))
                .withParserMap(parserMap)
        )
        .withIoConfig(
            ioConfig -> ioConfig
                .withInputFormat(null)
                .withConsumerProperties(kafka.consumerProperties())
                .withSupervisorRunPeriod(Period.millis(10))
        )
        .withTuningConfig(tuningConfig -> tuningConfig.withMaxRowsPerSegment(1))
        .build(dataSource, topic);
  }

  @Override
  public SupervisorSpec createSupervisor(String dataSource, String topic, InputFormat inputFormat)
  {
    return MoreResources.Supervisor.KAFKA_JSON
        .get()
        .withDataSchema(schema -> schema.withTimestamp(new TimestampSpec("timestamp", null, null)))
        .withIoConfig(
            ioConfig -> ioConfig
                .withInputFormat(inputFormat)
                .withConsumerProperties(kafka.consumerProperties())
                .withSupervisorRunPeriod(Period.millis(10))
        )
        .withTuningConfig(tuningConfig -> tuningConfig.withMaxRowsPerSegment(1))
        .build(dataSource, topic);
  }
}
