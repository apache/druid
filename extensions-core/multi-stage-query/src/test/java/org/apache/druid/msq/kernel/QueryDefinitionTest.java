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

package org.apache.druid.msq.kernel;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.frame.key.ClusterBy;
import org.apache.druid.frame.key.KeyColumn;
import org.apache.druid.frame.key.KeyOrder;
import org.apache.druid.msq.guice.MSQIndexingModule;
import org.apache.druid.msq.querykit.common.OffsetLimitStageProcessor;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.junit.Assert;
import org.junit.Test;

import java.util.UUID;

public class QueryDefinitionTest
{
  @Test
  public void testSerde() throws Exception
  {
    final QueryDefinition queryDef =
        QueryDefinition
            .builder(UUID.randomUUID().toString())
            .add(
                StageDefinition
                    .builder(0)
                    .processor(new OffsetLimitStageProcessor(0, 1L))
                    .shuffleSpec(
                        new GlobalSortMaxCountShuffleSpec(
                            new ClusterBy(ImmutableList.of(new KeyColumn("s", KeyOrder.ASCENDING)), 0),
                            2,
                            false,
                            ShuffleSpec.UNLIMITED
                        )
                    )
                    .maxWorkerCount(3)
                    .signature(RowSignature.builder().add("s", ColumnType.STRING).build())
            )
            .build();

    final ObjectMapper mapper = TestHelper.makeJsonMapper()
                                          .registerModules(new MSQIndexingModule().getJacksonModules());

    Assert.assertEquals(
        queryDef,
        mapper.readValue(mapper.writeValueAsString(queryDef), QueryDefinition.class)
    );
  }

  @Test
  public void testEquals()
  {
    EqualsVerifier.forClass(QueryDefinition.class)
                  .withNonnullFields("stageDefinitions", "finalStage")
                  .usingGetClass()
                  .verify();
  }
}
