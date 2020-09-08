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
import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.indexing.stats.NoopIngestionMetricsSnapshot;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.DataSegment.PruneSpecsHolder;
import org.junit.Assert;
import org.junit.Test;

public class PushedSegmentsReportTest
{
  @Test
  public void testJsonSerde()
  {
    final PushedSegmentsReport report = new PushedSegmentsReport(
        "taskId",
        ImmutableSet.of(
            new DataSegment(
                "datasource",
                Intervals.of("2020-01-01/P1D"),
                "version",
                null,
                null,
                null,
                null,
                9,
                10L
            )
        ),
        ImmutableSet.of(
            new DataSegment(
                "datasource",
                Intervals.of("2020-01-01/P1D"),
                "version2",
                null,
                null,
                null,
                null,
                9,
                20L
            )
        ),
        NoopIngestionMetricsSnapshot.INSTANCE
    );
    final ObjectMapper mapper = new DefaultObjectMapper();
    InjectableValues.Std injectableValues = new InjectableValues.Std();
    injectableValues.addValue(PruneSpecsHolder.class, PruneSpecsHolder.DEFAULT);
    mapper.setInjectableValues(injectableValues);
    TestHelper.testSerializesDeserializes(mapper, report);
  }

  @Test
  public void testJsonSerdeWithMissingCreatedTimeNsAndMetrics() throws JsonProcessingException
  {
    final String json = "{\n"
                        + "  \"type\" : \"pushed_segments\",\n"
                        + "  \"taskId\" : \"taskId\",\n"
                        + "  \"oldSegments\" : [ {\n"
                        + "    \"dataSource\" : \"datasource\",\n"
                        + "    \"interval\" : \"2020-01-01T00:00:00.000Z/2020-01-02T00:00:00.000Z\",\n"
                        + "    \"version\" : \"version\",\n"
                        + "    \"shardSpec\" : {\n"
                        + "      \"type\" : \"numbered\",\n"
                        + "      \"partitionNum\" : 0,\n"
                        + "      \"partitions\" : 1\n"
                        + "    },\n"
                        + "    \"binaryVersion\" : 9,\n"
                        + "    \"size\" : 10,\n"
                        + "    \"identifier\" : \"datasource_2020-01-01T00:00:00.000Z_2020-01-02T00:00:00.000Z_version\"\n"
                        + "  } ],\n"
                        + "  \"segments\" : [ {\n"
                        + "    \"dataSource\" : \"datasource\",\n"
                        + "    \"interval\" : \"2020-01-01T00:00:00.000Z/2020-01-02T00:00:00.000Z\",\n"
                        + "    \"version\" : \"version2\",\n"
                        + "    \"shardSpec\" : {\n"
                        + "      \"type\" : \"numbered\",\n"
                        + "      \"partitionNum\" : 0,\n"
                        + "      \"partitions\" : 1\n"
                        + "    },\n"
                        + "    \"binaryVersion\" : 9,\n"
                        + "    \"size\" : 20,\n"
                        + "    \"identifier\" : \"datasource_2020-01-01T00:00:00.000Z_2020-01-02T00:00:00.000Z_version2\"\n"
                        + "  } ]\n"
                        + "}\n";
    final ObjectMapper mapper = new DefaultObjectMapper();
    InjectableValues.Std injectableValues = new InjectableValues.Std();
    injectableValues.addValue(PruneSpecsHolder.class, PruneSpecsHolder.DEFAULT);
    mapper.setInjectableValues(injectableValues);

    final PushedSegmentsReport expected = new PushedSegmentsReport(
        0L,
        "taskId",
        ImmutableSet.of(
            new DataSegment(
                "datasource",
                Intervals.of("2020-01-01/P1D"),
                "version",
                null,
                null,
                null,
                null,
                9,
                10L
            )
        ),
        ImmutableSet.of(
            new DataSegment(
                "datasource",
                Intervals.of("2020-01-01/P1D"),
                "version2",
                null,
                null,
                null,
                null,
                9,
                20L
            )
        ),
        NoopIngestionMetricsSnapshot.INSTANCE
    );
    Assert.assertEquals(expected, mapper.readValue(json, SubTaskReport.class));
  }

  @Test
  public void testEquals()
  {
    EqualsVerifier.forClass(PushedSegmentsReport.class).usingGetClass().verify();
  }
}
