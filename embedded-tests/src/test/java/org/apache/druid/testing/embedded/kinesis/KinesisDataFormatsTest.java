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

package org.apache.druid.testing.embedded.kinesis;

import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.indexer.granularity.UniformGranularitySpec;
import org.apache.druid.indexing.kinesis.KinesisRegion;
import org.apache.druid.indexing.kinesis.supervisor.KinesisSupervisorIOConfig;
import org.apache.druid.indexing.kinesis.supervisor.KinesisSupervisorSpec;
import org.apache.druid.indexing.overlord.supervisor.SupervisorSpec;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.StreamIngestResource;
import org.apache.druid.testing.embedded.indexing.StreamIndexDataFormatsTestBase;
import org.joda.time.Period;

import java.util.Map;

/**
 * Verifies Kinesis ingestion using various input data formats.
 */
public class KinesisDataFormatsTest extends StreamIndexDataFormatsTestBase
{
  private final KinesisResource kinesis = new KinesisResource();

  @Override
  protected StreamIngestResource<?> getStreamResource()
  {
    return kinesis;
  }

  @Override
  public EmbeddedDruidCluster createCluster()
  {
    return super.createCluster().useDefaultTimeoutForLatchableEmitter(120);
  }

  @Override
  protected SupervisorSpec createSupervisor(String dataSource, String topic, InputFormat inputFormat)
  {
    return createKinesisSupervisorSpec(dataSource, topic, inputFormat);
  }

  @Override
  protected SupervisorSpec createSupervisorWithParser(String dataSource, String topic, Map<String, Object> parserMap)
  {
    final KinesisSupervisorSpec baseSpec = createKinesisSupervisorSpec(dataSource, topic, null);
    return new KinesisSupervisorSpec(
        dataSource,
        null,
        DataSchema.builder(baseSpec.getSpec().getDataSchema())
                  .withParserMap(parserMap)
                  .build(),
        baseSpec.getSpec().getTuningConfig(),
        baseSpec.getSpec().getIOConfig(),
        Map.of(),
        false,
        null, null, null, null, null, null, null, null, null, null
    );
  }

  private KinesisSupervisorSpec createKinesisSupervisorSpec(String dataSource, String topic, InputFormat inputFormat)
  {
    return new KinesisSupervisorSpec(
        dataSource,
        null,
        DataSchema.builder()
                  .withDataSource(dataSource)
                  .withTimestamp(new TimestampSpec("timestamp", null, null))
                  .withGranularity(new UniformGranularitySpec(Granularities.HOUR, null, null))
                  .withDimensions(DimensionsSpec.EMPTY)
                  .build(),
        null,
        new KinesisSupervisorIOConfig(
            topic,
            inputFormat,
            kinesis.getEndpoint(),
            KinesisRegion.fromString(kinesis.getRegion()),
            1,
            1,
            Period.millis(500),
            Period.millis(10),
            Period.millis(10),
            true,
            Period.seconds(5),
            null, null, null, null, null, null, null, null,
            false
        ),
        Map.of(),
        false,
        null, null, null, null, null, null, null, null, null, null
    );
  }
}
