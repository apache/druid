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

package org.apache.druid.msq.indexing.error;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.msq.guice.MSQIndexingModule;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.sql.calcite.planner.JoinAlgorithm;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

public class MSQFaultSerdeTest
{
  private ObjectMapper objectMapper;

  @Before
  public void setUp()
  {
    objectMapper = TestHelper.makeJsonMapper();
    objectMapper.registerModules(new MSQIndexingModule().getJacksonModules());
    objectMapper.enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION);
  }

  @Test
  public void testFaultSerde() throws IOException
  {
    assertFaultSerde(new BroadcastTablesTooLargeFault(10, null));
    assertFaultSerde(new BroadcastTablesTooLargeFault(10, JoinAlgorithm.SORT_MERGE));
    assertFaultSerde(CanceledFault.INSTANCE);
    assertFaultSerde(new CannotParseExternalDataFault("the message"));
    assertFaultSerde(new ColumnTypeNotSupportedFault("the column", null));
    assertFaultSerde(new ColumnTypeNotSupportedFault("the column", ColumnType.STRING_ARRAY));
    assertFaultSerde(new ColumnNameRestrictedFault("the column"));
    assertFaultSerde(new InsertCannotAllocateSegmentFault("the datasource", Intervals.ETERNITY, null));
    assertFaultSerde(new InsertCannotAllocateSegmentFault(
        "the datasource",
        Intervals.of("2000-01-01/2002-01-01"),
        Intervals.ETERNITY
    ));
    assertFaultSerde(new InsertCannotBeEmptyFault("the datasource"));
    assertFaultSerde(InsertLockPreemptedFault.INSTANCE);
    assertFaultSerde(InsertTimeNullFault.INSTANCE);
    assertFaultSerde(new InsertTimeOutOfBoundsFault(
        Intervals.of("2001/2002"),
        Collections.singletonList(Intervals.of("2000/2001"))
    ));
    assertFaultSerde(new InvalidNullByteFault("the source", 1, "the column", "the value", 2));
    assertFaultSerde(new NotEnoughMemoryFault(1000, 1000, 900, 1, 2));
    assertFaultSerde(QueryNotSupportedFault.INSTANCE);
    assertFaultSerde(new QueryRuntimeFault("new error", "base error"));
    assertFaultSerde(new QueryRuntimeFault("new error", null));
    assertFaultSerde(new RowTooLargeFault(1000));
    assertFaultSerde(new TaskStartTimeoutFault(1, 10, 11));
    assertFaultSerde(new TooManyBucketsFault(10));
    assertFaultSerde(new TooManyColumnsFault(10, 8));
    assertFaultSerde(new TooManyClusteredByColumnsFault(10, 8, 1));
    assertFaultSerde(new TooManyInputFilesFault(15, 10, 5));
    assertFaultSerde(new TooManyPartitionsFault(10));
    assertFaultSerde(new TooManyRowsWithSameKeyFault(Arrays.asList("foo", 123), 1, 2));
    assertFaultSerde(new TooManyWarningsFault(10, "the error"));
    assertFaultSerde(new TooManyWorkersFault(10, 5));
    assertFaultSerde(new TooManyAttemptsForWorker(2, "taskId", 1, "rootError"));
    assertFaultSerde(new TooManyAttemptsForJob(2, 2, "taskId", "rootError"));
    assertFaultSerde(UnknownFault.forMessage(null));
    assertFaultSerde(UnknownFault.forMessage("the message"));
    assertFaultSerde(new WorkerFailedFault("the worker task", "the error msg"));
    assertFaultSerde(new WorkerRpcFailedFault("the worker task"));
    assertFaultSerde(new NotEnoughTemporaryStorageFault(250, 2));
  }

  @Test
  public void testFaultEqualsAndHashCode()
  {
    for (Class<? extends MSQFault> faultClass : MSQIndexingModule.FAULT_CLASSES) {
      EqualsVerifier.forClass(faultClass).usingGetClass().verify();
    }
  }

  private void assertFaultSerde(final MSQFault fault) throws IOException
  {
    final String json = objectMapper.writeValueAsString(fault);
    final MSQFault fault2 = objectMapper.readValue(json, MSQFault.class);
    Assert.assertEquals(json, fault, fault2);
  }
}
