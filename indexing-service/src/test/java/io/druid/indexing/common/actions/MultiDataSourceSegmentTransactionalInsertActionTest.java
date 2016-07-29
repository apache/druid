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

package io.druid.indexing.common.actions;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.druid.indexing.overlord.DataSourceMetadataAndSegments;
import io.druid.indexing.overlord.ObjectMetadata;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.NoneShardSpec;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

/**
 */
public class MultiDataSourceSegmentTransactionalInsertActionTest
{
  @Test
  public void testSerde() throws Exception
  {
    ObjectMapper mapper = new DefaultObjectMapper();

    MultiDataSourceSegmentTransactionalInsertAction expected = new MultiDataSourceSegmentTransactionalInsertAction(
        ImmutableList.of(
            new DataSourceMetadataAndSegments(
                ImmutableSet.of(
                    new DataSegment(
                        "testDataSource",
                        new Interval("2000/2001"),
                        "version",
                        ImmutableMap.<String, Object>of(),
                        ImmutableList.<String>of(),
                        ImmutableList.<String>of(),
                        NoneShardSpec.instance(),
                        9,
                        1024
                    )
                ),
                new ObjectMetadata("A"),
                new ObjectMetadata("B")

            )
        )
    );

    MultiDataSourceSegmentTransactionalInsertAction actual = (MultiDataSourceSegmentTransactionalInsertAction) mapper.readValue(
        mapper.writeValueAsString(expected),
        TaskAction.class
    );

    Assert.assertEquals(expected, actual);
  }
}
