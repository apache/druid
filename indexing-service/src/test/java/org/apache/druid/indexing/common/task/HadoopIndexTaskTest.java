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

package org.apache.druid.indexing.common.task;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.indexer.HadoopIOConfig;
import org.apache.druid.indexer.HadoopIngestionSpec;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.granularity.UniformGranularitySpec;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.AuthTestUtils;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.server.security.ResourceType;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;

public class HadoopIndexTaskTest
{
  private final ObjectMapper jsonMapper = new DefaultObjectMapper();

  @Test
  public void testCorrectInputSourceTypes()
  {
    final HadoopIndexTask task = new HadoopIndexTask(
        null,
        new HadoopIngestionSpec(
            new DataSchema(
                "foo", null, new AggregatorFactory[0], new UniformGranularitySpec(
                Granularities.DAY,
                null,
                ImmutableList.of(Intervals.of("2010-01-01/P1D"))
            ),
                null,
                jsonMapper
            ), new HadoopIOConfig(ImmutableMap.of("paths", "bar"), null, null), null
        ),
        null,
        null,
        "blah",
        jsonMapper,
        null,
        AuthTestUtils.TEST_AUTHORIZER_MAPPER,
        null
    );

    Assert.assertEquals(
        Collections.singleton(
            new ResourceAction(new Resource(
                ResourceType.EXTERNAL,
                "hadoop"
            ), Action.READ)),
        task.getInputSourceResources()
    );
  }
}
