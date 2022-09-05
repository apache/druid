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
import org.apache.druid.timeline.SegmentId;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

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
    assertFaultSerde(new BroadcastTablesTooLargeFault(10));
    assertFaultSerde(CanceledFault.INSTANCE);
    assertFaultSerde(new CannotParseExternalDataFault("the message"));
    assertFaultSerde(new ColumnTypeNotSupportedFault("the column", null));
    assertFaultSerde(new ColumnTypeNotSupportedFault("the column", ColumnType.STRING_ARRAY));
    assertFaultSerde(new ColumnNameRestrictedFault("the column"));
    assertFaultSerde(new InsertCannotAllocateSegmentFault("the datasource", Intervals.ETERNITY));
    assertFaultSerde(new InsertCannotBeEmptyFault("the datasource"));
    assertFaultSerde(new InsertCannotOrderByDescendingFault("the column"));
    assertFaultSerde(
        new InsertCannotReplaceExistingSegmentFault(SegmentId.of("the datasource", Intervals.ETERNITY, "v1", 1))
    );
    assertFaultSerde(InsertLockPreemptedFault.INSTANCE);
    assertFaultSerde(InsertTimeNullFault.INSTANCE);
    assertFaultSerde(new InsertTimeOutOfBoundsFault(Intervals.ETERNITY));
    assertFaultSerde(new InvalidNullByteFault("the column"));
    assertFaultSerde(new NotEnoughMemoryFault(1000, 1, 2));
    assertFaultSerde(QueryNotSupportedFault.INSTANCE);
    assertFaultSerde(new RowTooLargeFault(1000));
    assertFaultSerde(new TaskStartTimeoutFault(10));
    assertFaultSerde(new TooManyBucketsFault(10));
    assertFaultSerde(new TooManyColumnsFault(10, 8));
    assertFaultSerde(new TooManyInputFilesFault(15, 10, 5));
    assertFaultSerde(new TooManyPartitionsFault(10));
    assertFaultSerde(new TooManyWarningsFault(10, "the error"));
    assertFaultSerde(new TooManyWorkersFault(10, 5));
    assertFaultSerde(UnknownFault.forMessage(null));
    assertFaultSerde(UnknownFault.forMessage("the message"));
    assertFaultSerde(new WorkerFailedFault("the worker task", "the error msg"));
    assertFaultSerde(new WorkerRpcFailedFault("the worker task"));
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
