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

package org.apache.druid.indexing.common.task.batch.parallel;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.InjectableValues.Std;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.indexing.stats.NoopIngestionMetricsSnapshot;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.timeline.partition.HashBucketShardSpec;
import org.junit.Assert;
import org.junit.Test;

public class GeneratedPartitionsMetadataReportTest
{
  @Test
  public void testSerde()
  {
    final ObjectMapper mapper = new DefaultObjectMapper();
    mapper.setInjectableValues(new Std().addValue(ObjectMapper.class, mapper));
    final GeneratedPartitionsMetadataReport report = new GeneratedPartitionsMetadataReport(
        "taskId",
        ImmutableList.of(
            new GenericPartitionStat(
                "host",
                8000,
                true,
                Intervals.of("2020-01-01/P1D"),
                new HashBucketShardSpec(1, 2, ImmutableList.of("dim"), mapper),
                10,
                100L
            )
        ),
        NoopIngestionMetricsSnapshot.INSTANCE
    );
    TestHelper.testSerializesDeserializes(mapper, report);
  }

  @Test
  public void testSerdeWithMissingCreatedTimeNsAndMetrics() throws JsonProcessingException
  {
    final String json = "{\n"
                        + "  \"type\" : \"generated_partitions_metadata\",\n"
                        + "  \"taskId\" : \"taskId\",\n"
                        + "  \"partitionStats\" : [ {\n"
                        + "    \"taskExecutorHost\" : \"host\",\n"
                        + "    \"taskExecutorPort\" : 8000,\n"
                        + "    \"useHttps\" : true,\n"
                        + "    \"interval\" : \"2020-01-01T00:00:00.000Z/2020-01-02T00:00:00.000Z\",\n"
                        + "    \"shardSpec\" : {\n"
                        + "      \"type\" : \"bucket_hash\",\n"
                        + "      \"bucketId\" : 1,\n"
                        + "      \"numBuckets\" : 2,\n"
                        + "      \"partitionDimensions\" : [ \"dim\" ]\n"
                        + "    },\n"
                        + "    \"numRows\" : 10,\n"
                        + "    \"sizeBytes\" : 100\n"
                        + "  } ]\n"
                        + "}";
    final ObjectMapper mapper = new DefaultObjectMapper();
    mapper.setInjectableValues(new Std().addValue(ObjectMapper.class, mapper));

    final GeneratedPartitionsMetadataReport expected = new GeneratedPartitionsMetadataReport(
        0,
        "taskId",
        ImmutableList.of(
            new GenericPartitionStat(
                "host",
                8000,
                true,
                Intervals.of("2020-01-01/P1D"),
                new HashBucketShardSpec(1, 2, ImmutableList.of("dim"), mapper),
                10,
                100L
            )
        ),
        NoopIngestionMetricsSnapshot.INSTANCE
    );

    Assert.assertEquals(expected, mapper.readValue(json, SubTaskReport.class));
  }

  @Test
  public void testEquals()
  {
    EqualsVerifier.forClass(GeneratedPartitionsMetadataReport.class).usingGetClass().verify();
  }
}
