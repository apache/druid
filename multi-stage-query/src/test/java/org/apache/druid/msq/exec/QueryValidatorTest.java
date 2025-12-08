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

package org.apache.druid.msq.exec;

import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.msq.indexing.error.MSQException;
import org.apache.druid.msq.kernel.QueryDefinition;
import org.apache.druid.msq.kernel.QueryDefinitionBuilder;
import org.apache.druid.msq.kernel.StageDefinition;
import org.apache.druid.msq.kernel.StageDefinitionBuilder;
import org.apache.druid.msq.querykit.common.OffsetLimitStageProcessor;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.UUID;
import java.util.stream.IntStream;

public class QueryValidatorTest
{
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testValidQueryDefination()
  {
    QueryValidator.validateQueryDef(createQueryDefinition(1, 1));
    QueryValidator.validateQueryDef(createQueryDefinition(
        Limits.MAX_FRAME_COLUMNS,
        Limits.MAX_WORKERS
    ));
  }

  @Test
  public void testNegativeWorkers()
  {
    expectedException.expect(ISE.class);
    expectedException.expectMessage("Number of workers must be greater than 0");
    QueryValidator.validateQueryDef(createQueryDefinition(1, -1));
  }

  @Test
  public void testZeroWorkers()
  {
    expectedException.expect(ISE.class);
    expectedException.expectMessage("Number of workers must be greater than 0");
    QueryValidator.validateQueryDef(createQueryDefinition(1, 0));
  }

  @Test
  public void testGreaterThanMaxWorkers()
  {
    expectedException.expect(MSQException.class);
    expectedException.expectMessage(
        StringUtils.format(
            "Too many workers (current = %d; max = %d)",
            Limits.MAX_WORKERS + 1,
            Limits.MAX_WORKERS
        ));
    QueryValidator.validateQueryDef(createQueryDefinition(1, Limits.MAX_WORKERS + 1));
  }

  @Test
  public void testGreaterThanMaxColumns()
  {
    expectedException.expect(MSQException.class);
    expectedException.expectMessage(StringUtils.format(
        "Too many output columns (requested = %d, max = %d)",
        Limits.MAX_FRAME_COLUMNS + 1,
        Limits.MAX_FRAME_COLUMNS
    ));
    QueryValidator.validateQueryDef(createQueryDefinition(Limits.MAX_FRAME_COLUMNS + 1, 1));
  }

  public static QueryDefinition createQueryDefinition(int numColumns, int numWorkers)
  {
    QueryDefinitionBuilder builder = QueryDefinition.builder(UUID.randomUUID().toString());

    StageDefinitionBuilder stageBuilder = StageDefinition.builder(0);
    builder.add(stageBuilder);
    stageBuilder.maxWorkerCount(numWorkers);

    // Need to have *some* processor.
    stageBuilder.processor(new OffsetLimitStageProcessor(1, 1L));

    RowSignature.Builder rowSignatureBuilder = RowSignature.builder();
    IntStream.range(0, numColumns).forEach(col -> rowSignatureBuilder.add("col_" + col, ColumnType.STRING));
    stageBuilder.signature(rowSignatureBuilder.build());

    return builder.build();
  }
}
