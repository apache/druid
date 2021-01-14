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
import org.apache.druid.data.input.impl.JsonInputFormat;
import org.apache.druid.data.input.impl.LocalInputSource;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.indexing.common.TestUtils;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.segment.indexing.DataSchema;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Map;

public class SinglePhaseSubTaskSpecTest
{
  private static final SinglePhaseSubTaskSpec SPEC = new SinglePhaseSubTaskSpec(
      "id",
      "groupId",
      "supervisorTaskId",
      new ParallelIndexIngestionSpec(
          new DataSchema(
              "dataSource",
              new TimestampSpec(null, null, null),
              new DimensionsSpec(null),
              new AggregatorFactory[0],
              null,
              null
          ),
          new ParallelIndexIOConfig(
              null,
              new LocalInputSource(new File("baseDir"), "filter"),
              new JsonInputFormat(null, null, null),
              null
          ),
          null
      ),
      null,
      new InputSplit<>("string split")
  );

  private ObjectMapper mapper;

  @Before
  public void setup()
  {
    mapper = new TestUtils().getTestObjectMapper();
  }

  @Test
  public void testNewSubTaskType() throws IOException
  {
    final SinglePhaseSubTask expected = SPEC.newSubTask(0);
    final byte[] json = mapper.writeValueAsBytes(expected);
    final Map<String, Object> actual = mapper.readValue(json, Map.class);
    Assert.assertEquals(SinglePhaseSubTask.TYPE, actual.get("type"));
  }

  @Test
  public void testNewSubTaskWithBackwardCompatibleType() throws IOException
  {
    final SinglePhaseSubTask expected = SPEC.newSubTaskWithBackwardCompatibleType(0);
    final byte[] json = mapper.writeValueAsBytes(expected);
    final Map<String, Object> actual = mapper.readValue(json, Map.class);
    Assert.assertEquals(SinglePhaseSubTask.OLD_TYPE_NAME, actual.get("type"));
  }
}
