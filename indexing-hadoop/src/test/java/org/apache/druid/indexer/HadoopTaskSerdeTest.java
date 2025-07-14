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

package org.apache.druid.indexer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.indexer.granularity.UniformGranularitySpec;
import org.apache.druid.indexing.common.TestUtils;
import org.apache.druid.indexing.common.task.IndexTask;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.common.task.batch.parallel.ParallelIndexTuningConfig;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.server.security.AuthTestUtils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class HadoopTaskSerdeTest
{
  private final ObjectMapper jsonMapper;
  private final IndexSpec indexSpec = IndexSpec.DEFAULT;

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  public HadoopTaskSerdeTest()
  {
    TestUtils testUtils = new TestUtils();
    jsonMapper = testUtils.getTestObjectMapper();
    jsonMapper.registerSubtypes(
        new NamedType(ParallelIndexTuningConfig.class, "index_parallel"),
        new NamedType(IndexTask.IndexTuningConfig.class, "index")
    );
  }

  @Test
  public void testHadoopIndexTaskSerde() throws Exception
  {
    final HadoopIndexTask task = new HadoopIndexTask(
        null,
        new HadoopIngestionSpec(
            DataSchema.builder()
                      .withDataSource("foo")
                      .withGranularity(
                          new UniformGranularitySpec(
                              Granularities.DAY,
                              null,
                              ImmutableList.of(Intervals.of("2010-01-01/P1D"))
                          )
                      )
                      .withObjectMapper(jsonMapper)
                      .build(),
            new HadoopIOConfig(ImmutableMap.of("paths", "bar"), null, null),
            null
        ),
        null,
        null,
        "blah",
        jsonMapper,
        null,
        AuthTestUtils.TEST_AUTHORIZER_MAPPER,
        null,
        new HadoopTaskConfig(null, null)
    );

    final String json = jsonMapper.writeValueAsString(task);

    final HadoopIndexTask task2 = (HadoopIndexTask) jsonMapper.readValue(json, Task.class);

    Assert.assertEquals("foo", task.getDataSource());

    Assert.assertEquals(task.getId(), task2.getId());
    Assert.assertEquals(task.getGroupId(), task2.getGroupId());
    Assert.assertEquals(task.getDataSource(), task2.getDataSource());
    Assert.assertEquals(
        task.getSpec().getTuningConfig().getJobProperties(),
        task2.getSpec().getTuningConfig().getJobProperties()
    );
    Assert.assertEquals("blah", task.getClasspathPrefix());
    Assert.assertEquals("blah", task2.getClasspathPrefix());
  }

  @Test
  public void testHadoopIndexTaskWithContextSerde() throws Exception
  {
    final HadoopIndexTask task = new HadoopIndexTask(
        null,
        new HadoopIngestionSpec(
            DataSchema.builder()
                      .withDataSource("foo")
                      .withGranularity(
                          new UniformGranularitySpec(
                              Granularities.DAY,
                              null, ImmutableList.of(Intervals.of("2010-01-01/P1D"))
                          )
                      )
                      .withObjectMapper(jsonMapper)
                      .build(),
            new HadoopIOConfig(ImmutableMap.of("paths", "bar"), null, null),
            null
        ),
        null,
        null,
        "blah",
        jsonMapper,
        ImmutableMap.of("userid", 12345, "username", "bob"),
        AuthTestUtils.TEST_AUTHORIZER_MAPPER,
        null,
        new HadoopTaskConfig(null, null)
    );

    final String json = jsonMapper.writeValueAsString(task);

    final HadoopIndexTask task2 = (HadoopIndexTask) jsonMapper.readValue(json, Task.class);

    Assert.assertEquals("foo", task.getDataSource());

    Assert.assertEquals(task.getId(), task2.getId());
    Assert.assertEquals(task.getGroupId(), task2.getGroupId());
    Assert.assertEquals(task.getDataSource(), task2.getDataSource());
    Assert.assertEquals(
        task.getSpec().getTuningConfig().getJobProperties(),
        task2.getSpec().getTuningConfig().getJobProperties()
    );
    Assert.assertEquals("blah", task.getClasspathPrefix());
    Assert.assertEquals("blah", task2.getClasspathPrefix());
    Assert.assertEquals(ImmutableMap.of("userid", 12345, "username", "bob"), task2.getContext());
    Assert.assertEquals(ImmutableMap.of("userid", 12345, "username", "bob"), task2.getSpec().getContext());
  }
}
