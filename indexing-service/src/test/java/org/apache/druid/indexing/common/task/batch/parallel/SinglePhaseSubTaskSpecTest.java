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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.data.input.InputSplit;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.InputRowParser;
import org.apache.druid.data.input.impl.JSONParseSpec;
import org.apache.druid.data.input.impl.StringInputRowParser;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.indexing.common.TestUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.realtime.firehose.LocalFirehoseFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Map;

public class SinglePhaseSubTaskSpecTest
{
  private static final ObjectMapper MAPPER = new TestUtils().getTestObjectMapper();

  private static ParallelIndexIngestionSpec createParallelIndexIngestionSpec() throws IOException
  {
    final InputRowParser parser = new StringInputRowParser(
        new JSONParseSpec(
            new TimestampSpec(null, null, null),
            new DimensionsSpec(null),
            null,
            null
        ),
        StringUtils.UTF8_STRING
    );
    final Map<String, Object> parserMap = MAPPER.readValue(MAPPER.writeValueAsBytes(parser), Map.class);
    return new ParallelIndexIngestionSpec(
        new DataSchema(
            "dataSource",
            parserMap,
            new AggregatorFactory[0],
            null,
            null,
            MAPPER
        ),
        new ParallelIndexIOConfig(
            new LocalFirehoseFactory(new File("baseDir"), "filter", null),
            null
        ),
        null
    );
  }

  private SinglePhaseSubTaskSpec spec;

  @Before
  public void setup() throws IOException
  {
    spec = new SinglePhaseSubTaskSpec(
        "id",
        "groupId",
        "supervisorTaskId",
        createParallelIndexIngestionSpec(),
        null,
        new InputSplit<>("string split")
    );
  }

  @Test
  public void testNewSubTaskType() throws IOException
  {
    final SinglePhaseSubTask expected = spec.newSubTask(0);
    final byte[] json = MAPPER.writeValueAsBytes(expected);
    final Map<String, Object> actual = MAPPER.readValue(json, Map.class);
    Assert.assertEquals(SinglePhaseSubTask.TYPE, actual.get("type"));
  }

  @Test
  public void testNewSubTaskWithBackwardCompatibleType() throws IOException
  {
    final SinglePhaseSubTask expected = spec.newSubTaskWithBackwardCompatibleType(0);
    final byte[] json = MAPPER.writeValueAsBytes(expected);
    final Map<String, Object> actual = MAPPER.readValue(json, Map.class);
    Assert.assertEquals(SinglePhaseSubTask.OLD_TYPE_NAME, actual.get("type"));
  }
}
