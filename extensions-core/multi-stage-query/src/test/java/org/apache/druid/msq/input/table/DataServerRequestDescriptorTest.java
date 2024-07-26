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

package org.apache.druid.msq.input.table;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.msq.guice.MSQIndexingModule;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.server.coordination.DruidServerMetadata;
import org.apache.druid.server.coordination.ServerType;
import org.junit.Assert;
import org.junit.Test;

public class DataServerRequestDescriptorTest
{
  @Test
  public void testSerde() throws JsonProcessingException
  {
    DataServerRequestDescriptor segment = new DataServerRequestDescriptor(
        new DruidServerMetadata(
            "testServer",
            "localhost:8081",
            null,
            1,
            ServerType.INDEXER_EXECUTOR,
            "tier1", 2
        ),
        ImmutableList.of(new RichSegmentDescriptor(Intervals.ETERNITY, Intervals.ETERNITY, "v1", 2))
    );

    final ObjectMapper mapper = TestHelper.makeJsonMapper()
                                          .registerModules(new MSQIndexingModule().getJacksonModules());

    Assert.assertEquals(
        segment,
        mapper.readValue(mapper.writeValueAsString(segment), DataServerRequestDescriptor.class)
    );
  }
}
