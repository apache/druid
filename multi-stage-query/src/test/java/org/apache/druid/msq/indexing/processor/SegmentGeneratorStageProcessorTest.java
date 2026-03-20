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

package org.apache.druid.msq.indexing.processor;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.indexer.granularity.UniformGranularitySpec;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.msq.exec.StageProcessor;
import org.apache.druid.msq.guice.MSQIndexingModule;
import org.apache.druid.msq.indexing.MSQTuningConfig;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.apache.druid.sql.calcite.planner.ColumnMapping;
import org.apache.druid.sql.calcite.planner.ColumnMappings;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

class SegmentGeneratorStageProcessorTest
{
  @Test
  public void testSerde() throws JsonProcessingException
  {
    ObjectMapper mapper = TestHelper.makeJsonMapper();
    mapper.registerModules(new MSQIndexingModule().getJacksonModules());
    SegmentGeneratorStageProcessor processor = new SegmentGeneratorStageProcessor(
        DataSchema.builder()
                  .withDataSource("test")
                  .withTimestamp(new TimestampSpec("timestamp", "auto", null))
                  .withGranularity(new UniformGranularitySpec(Granularities.HOUR, Granularities.MINUTE, null))
                  .withDimensions(new StringDimensionSchema("a"), new StringDimensionSchema("b"))
                  .withAggregators(new CountAggregatorFactory("cnt"))
                  .build(),
        new ColumnMappings(
            List.of(
                new ColumnMapping("d0", "__time"),
                new ColumnMapping("d1", "a"),
                new ColumnMapping("d2", "b"),
                new ColumnMapping("a0", "cnt")
            )
        ),
        Map.of(
            "v0",
            new ExpressionVirtualColumn("v0", "concat(\"a\",'foo')", ColumnType.STRING, TestExprMacroTable.INSTANCE)
        ),
        MSQTuningConfig.defaultConfig()
    );

    Assertions.assertEquals(processor, mapper.readValue(mapper.writeValueAsString(processor), StageProcessor.class));
  }

  @Test
  public void testEqualsAndHashCode()
  {
    EqualsVerifier.forClass(SegmentGeneratorStageProcessor.class).usingGetClass().verify();
  }
}
