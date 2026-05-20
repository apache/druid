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

import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.JoinAlgorithm;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

public class MSQFaultToDruidExceptionTest
{
  @Test
  public void testBroadcastTablesTooLargeFault()
  {
    final DruidException e = new BroadcastTablesTooLargeFault(10, JoinAlgorithm.SORT_MERGE).toDruidException();
    Assert.assertEquals("BroadcastTablesTooLarge", e.getErrorCode());
    Assert.assertEquals(DruidException.Category.CAPACITY_EXCEEDED, e.getCategory());
    Assert.assertEquals(DruidException.Persona.USER, e.getTargetPersona());
  }

  @Test
  public void testCanceledFaultShutdown()
  {
    final DruidException e = CanceledFault.shutdown().toDruidException();
    Assert.assertEquals("Canceled", e.getErrorCode());
    Assert.assertEquals(DruidException.Category.CANCELED, e.getCategory());
    Assert.assertEquals(DruidException.Persona.USER, e.getTargetPersona());
  }

  @Test
  public void testCanceledFaultTimeout()
  {
    final DruidException e = CanceledFault.timeout().toDruidException();
    Assert.assertEquals("Canceled", e.getErrorCode());
    Assert.assertEquals(DruidException.Category.TIMEOUT, e.getCategory());
    Assert.assertEquals(DruidException.Persona.USER, e.getTargetPersona());
  }

  @Test
  public void testCanceledFaultUserRequest()
  {
    final DruidException e = CanceledFault.userRequest().toDruidException();
    Assert.assertEquals("Canceled", e.getErrorCode());
    Assert.assertEquals(DruidException.Category.CANCELED, e.getCategory());
    Assert.assertEquals(DruidException.Persona.USER, e.getTargetPersona());
  }

  @Test
  public void testCanceledFaultUnknown()
  {
    final DruidException e = CanceledFault.unknown().toDruidException();
    Assert.assertEquals("Canceled", e.getErrorCode());
    Assert.assertEquals(DruidException.Category.CANCELED, e.getCategory());
    Assert.assertEquals(DruidException.Persona.USER, e.getTargetPersona());
  }

  @Test
  public void testCannotParseExternalDataFault()
  {
    final DruidException e = new CannotParseExternalDataFault("the message").toDruidException();
    Assert.assertEquals("CannotParseExternalData", e.getErrorCode());
    Assert.assertEquals(DruidException.Category.INVALID_INPUT, e.getCategory());
    Assert.assertEquals(DruidException.Persona.USER, e.getTargetPersona());
  }

  @Test
  public void testDruidExceptionFault()
  {
    final DruidException e = new DruidExceptionFault(
        "custom",
        "USER",
        "RUNTIME_FAILURE",
        "test message",
        Collections.singletonMap("key", "value")
    ).toDruidException();
    Assert.assertEquals("custom", e.getErrorCode());
    Assert.assertEquals(DruidException.Category.RUNTIME_FAILURE, e.getCategory());
    Assert.assertEquals(DruidException.Persona.USER, e.getTargetPersona());
    Assert.assertEquals(Collections.singletonMap("key", "value"), e.getContext());
  }

  @Test
  public void testDruidExceptionFaultUnknownEnums()
  {
    final DruidException e = new DruidExceptionFault(
        "custom",
        "NONEXISTENT_PERSONA",
        "NONEXISTENT_CATEGORY",
        "test message",
        Collections.emptyMap()
    ).toDruidException();
    Assert.assertEquals("custom", e.getErrorCode());
    Assert.assertEquals(DruidException.Category.UNCATEGORIZED, e.getCategory());
    Assert.assertEquals(DruidException.Persona.DEVELOPER, e.getTargetPersona());
  }

  @Test
  public void testDurableStorageConfigurationFault()
  {
    final DruidException e = new DurableStorageConfigurationFault("some error").toDruidException();
    Assert.assertEquals("DurableStorageConfiguration", e.getErrorCode());
    Assert.assertEquals(DruidException.Category.INVALID_INPUT, e.getCategory());
    Assert.assertEquals(DruidException.Persona.OPERATOR, e.getTargetPersona());
  }

  @Test
  public void testInsertCannotAllocateSegmentFault()
  {
    final DruidException e = new InsertCannotAllocateSegmentFault(
        "the datasource",
        Intervals.ETERNITY,
        null
    ).toDruidException();
    Assert.assertEquals("InsertCannotAllocateSegment", e.getErrorCode());
    Assert.assertEquals(DruidException.Category.RUNTIME_FAILURE, e.getCategory());
    Assert.assertEquals(DruidException.Persona.USER, e.getTargetPersona());
  }

  @Test
  public void testInsertCannotBeEmptyFault()
  {
    final DruidException e = new InsertCannotBeEmptyFault("the datasource").toDruidException();
    Assert.assertEquals("InsertCannotBeEmpty", e.getErrorCode());
    Assert.assertEquals(DruidException.Category.INVALID_INPUT, e.getCategory());
    Assert.assertEquals(DruidException.Persona.USER, e.getTargetPersona());
  }

  @Test
  public void testInsertLockPreemptedFault()
  {
    final DruidException e = InsertLockPreemptedFault.INSTANCE.toDruidException();
    Assert.assertEquals("InsertLockPreempted", e.getErrorCode());
    Assert.assertEquals(DruidException.Category.CONFLICT, e.getCategory());
    Assert.assertEquals(DruidException.Persona.USER, e.getTargetPersona());
  }

  @Test
  public void testInsertTimeNullFault()
  {
    final DruidException e = InsertTimeNullFault.INSTANCE.toDruidException();
    Assert.assertEquals("InsertTimeNull", e.getErrorCode());
    Assert.assertEquals(DruidException.Category.INVALID_INPUT, e.getCategory());
    Assert.assertEquals(DruidException.Persona.USER, e.getTargetPersona());
  }

  @Test
  public void testInsertTimeOutOfBoundsFault()
  {
    final DruidException e = new InsertTimeOutOfBoundsFault(
        Intervals.of("2001/2002"),
        Collections.singletonList(Intervals.of("2000/2001"))
    ).toDruidException();
    Assert.assertEquals("InsertTimeOutOfBounds", e.getErrorCode());
    Assert.assertEquals(DruidException.Category.INVALID_INPUT, e.getCategory());
    Assert.assertEquals(DruidException.Persona.USER, e.getTargetPersona());
  }

  @Test
  public void testInvalidFieldFault()
  {
    final DruidException e = new InvalidFieldFault(
        "the source",
        "the column",
        1,
        "the error",
        "the log msg"
    ).toDruidException();
    Assert.assertEquals("InvalidField", e.getErrorCode());
    Assert.assertEquals(DruidException.Category.INVALID_INPUT, e.getCategory());
    Assert.assertEquals(DruidException.Persona.DEVELOPER, e.getTargetPersona());
  }

  @Test
  public void testInvalidNullByteFault()
  {
    final DruidException e = new InvalidNullByteFault(
        "the source",
        1,
        "the column",
        "the value",
        2
    ).toDruidException();
    Assert.assertEquals("InvalidNullByte", e.getErrorCode());
    Assert.assertEquals(DruidException.Category.INVALID_INPUT, e.getCategory());
    Assert.assertEquals(DruidException.Persona.USER, e.getTargetPersona());
  }

  @Test
  public void testNotEnoughMemoryFault()
  {
    final DruidException e = new NotEnoughMemoryFault(
        1234,
        1000,
        1000,
        900,
        1,
        2,
        2
    ).toDruidException();
    Assert.assertEquals("NotEnoughMemory", e.getErrorCode());
    Assert.assertEquals(DruidException.Category.CAPACITY_EXCEEDED, e.getCategory());
    Assert.assertEquals(DruidException.Persona.OPERATOR, e.getTargetPersona());
  }

  @Test
  public void testNotEnoughTemporaryStorageFault()
  {
    final DruidException e = new NotEnoughTemporaryStorageFault(250, 2).toDruidException();
    Assert.assertEquals("NotEnoughTemporaryStorageFault", e.getErrorCode());
    Assert.assertEquals(DruidException.Category.CAPACITY_EXCEEDED, e.getCategory());
    Assert.assertEquals(DruidException.Persona.OPERATOR, e.getTargetPersona());
  }

  @Test
  public void testQueryNotSupportedFault()
  {
    final DruidException e = QueryNotSupportedFault.INSTANCE.toDruidException();
    Assert.assertEquals("QueryNotSupported", e.getErrorCode());
    Assert.assertEquals(DruidException.Category.UNSUPPORTED, e.getCategory());
    Assert.assertEquals(DruidException.Persona.DEVELOPER, e.getTargetPersona());
  }

  @Test
  public void testQueryRuntimeFault()
  {
    final DruidException e = new QueryRuntimeFault("new error", "base error").toDruidException();
    Assert.assertEquals("QueryRuntimeError", e.getErrorCode());
    Assert.assertEquals(DruidException.Category.RUNTIME_FAILURE, e.getCategory());
    Assert.assertEquals(DruidException.Persona.DEVELOPER, e.getTargetPersona());
  }

  @Test
  public void testRowTooLargeFault()
  {
    final DruidException e = new RowTooLargeFault(1000).toDruidException();
    Assert.assertEquals("RowTooLarge", e.getErrorCode());
    Assert.assertEquals(DruidException.Category.CAPACITY_EXCEEDED, e.getCategory());
    Assert.assertEquals(DruidException.Persona.USER, e.getTargetPersona());
  }

  @Test
  public void testTaskStartTimeoutFault()
  {
    final DruidException e = new TaskStartTimeoutFault(1, 10, 11).toDruidException();
    Assert.assertEquals("TaskStartTimeout", e.getErrorCode());
    Assert.assertEquals(DruidException.Category.CAPACITY_EXCEEDED, e.getCategory());
    Assert.assertEquals(DruidException.Persona.OPERATOR, e.getTargetPersona());
  }

  @Test
  public void testTooManyAttemptsForJob()
  {
    final DruidException e = new TooManyAttemptsForJob(2, 2, "taskId", "rootError").toDruidException();
    Assert.assertEquals("TooManyAttemptsForJob", e.getErrorCode());
    Assert.assertEquals(DruidException.Category.RUNTIME_FAILURE, e.getCategory());
    Assert.assertEquals(DruidException.Persona.OPERATOR, e.getTargetPersona());
  }

  @Test
  public void testTooManyAttemptsForWorker()
  {
    final DruidException e = new TooManyAttemptsForWorker(2, "taskId", 1, "rootError").toDruidException();
    Assert.assertEquals("TooManyAttemptsForWorker", e.getErrorCode());
    Assert.assertEquals(DruidException.Category.RUNTIME_FAILURE, e.getCategory());
    Assert.assertEquals(DruidException.Persona.OPERATOR, e.getTargetPersona());
  }

  @Test
  public void testTooManyBucketsFault()
  {
    final DruidException e = new TooManyBucketsFault(10).toDruidException();
    Assert.assertEquals("TooManyBuckets", e.getErrorCode());
    Assert.assertEquals(DruidException.Category.CAPACITY_EXCEEDED, e.getCategory());
    Assert.assertEquals(DruidException.Persona.USER, e.getTargetPersona());
  }

  @Test
  public void testTooManyClusteredByColumnsFault()
  {
    final DruidException e = new TooManyClusteredByColumnsFault(10, 8, 1).toDruidException();
    Assert.assertEquals("TooManyClusteredByColumns", e.getErrorCode());
    Assert.assertEquals(DruidException.Category.CAPACITY_EXCEEDED, e.getCategory());
    Assert.assertEquals(DruidException.Persona.USER, e.getTargetPersona());
  }

  @Test
  public void testTooManyColumnsFault()
  {
    final DruidException e = new TooManyColumnsFault(10, 8).toDruidException();
    Assert.assertEquals("TooManyColumns", e.getErrorCode());
    Assert.assertEquals(DruidException.Category.CAPACITY_EXCEEDED, e.getCategory());
    Assert.assertEquals(DruidException.Persona.USER, e.getTargetPersona());
  }

  @Test
  public void testTooManyInputFilesFault()
  {
    final DruidException e = new TooManyInputFilesFault(15, 10, 5).toDruidException();
    Assert.assertEquals("TooManyInputFiles", e.getErrorCode());
    Assert.assertEquals(DruidException.Category.CAPACITY_EXCEEDED, e.getCategory());
    Assert.assertEquals(DruidException.Persona.USER, e.getTargetPersona());
  }

  @Test
  public void testTooManyPartitionsFault()
  {
    final DruidException e = new TooManyPartitionsFault(10).toDruidException();
    Assert.assertEquals("TooManyPartitions", e.getErrorCode());
    Assert.assertEquals(DruidException.Category.CAPACITY_EXCEEDED, e.getCategory());
    Assert.assertEquals(DruidException.Persona.USER, e.getTargetPersona());
  }

  @Test
  public void testTooManyRowsInAWindowFault()
  {
    final DruidException e = new TooManyRowsInAWindowFault(10, 20).toDruidException();
    Assert.assertEquals("TooManyRowsInAWindow", e.getErrorCode());
    Assert.assertEquals(DruidException.Category.CAPACITY_EXCEEDED, e.getCategory());
    Assert.assertEquals(DruidException.Persona.USER, e.getTargetPersona());
  }

  @Test
  public void testTooManyRowsWithSameKeyFault()
  {
    final DruidException e = new TooManyRowsWithSameKeyFault(
        Arrays.asList("foo", 123), 1, 2
    ).toDruidException();
    Assert.assertEquals("TooManyRowsWithSameKey", e.getErrorCode());
    Assert.assertEquals(DruidException.Category.CAPACITY_EXCEEDED, e.getCategory());
    Assert.assertEquals(DruidException.Persona.USER, e.getTargetPersona());
  }

  @Test
  public void testTooManySegmentsInTimeChunkFault()
  {
    final DruidException e = new TooManySegmentsInTimeChunkFault(
        DateTimes.nowUtc(), 10, 1, Granularities.ALL
    ).toDruidException();
    Assert.assertEquals("TooManySegmentsInTimeChunk", e.getErrorCode());
    Assert.assertEquals(DruidException.Category.CAPACITY_EXCEEDED, e.getCategory());
    Assert.assertEquals(DruidException.Persona.USER, e.getTargetPersona());
  }

  @Test
  public void testTooManyWarningsFault()
  {
    final DruidException e = new TooManyWarningsFault(10, "the error").toDruidException();
    Assert.assertEquals("TooManyWarnings", e.getErrorCode());
    Assert.assertEquals(DruidException.Category.RUNTIME_FAILURE, e.getCategory());
    Assert.assertEquals(DruidException.Persona.USER, e.getTargetPersona());
  }

  @Test
  public void testTooManyWorkersFault()
  {
    final DruidException e = new TooManyWorkersFault(10, 5).toDruidException();
    Assert.assertEquals("TooManyWorkers", e.getErrorCode());
    Assert.assertEquals(DruidException.Category.CAPACITY_EXCEEDED, e.getCategory());
    Assert.assertEquals(DruidException.Persona.USER, e.getTargetPersona());
  }

  @Test
  public void testUnknownFault()
  {
    final DruidException e = UnknownFault.forMessage("the message").toDruidException();
    Assert.assertEquals("UnknownError", e.getErrorCode());
    Assert.assertEquals(DruidException.Category.UNCATEGORIZED, e.getCategory());
    Assert.assertEquals(DruidException.Persona.DEVELOPER, e.getTargetPersona());
  }

  @Test
  public void testWorkerFailedFault()
  {
    final DruidException e = new WorkerFailedFault("the worker task", "the error msg").toDruidException();
    Assert.assertEquals("WorkerFailed", e.getErrorCode());
    Assert.assertEquals(DruidException.Category.RUNTIME_FAILURE, e.getCategory());
    Assert.assertEquals(DruidException.Persona.OPERATOR, e.getTargetPersona());
  }

  @Test
  public void testWorkerRpcFailedFault()
  {
    final DruidException e = new WorkerRpcFailedFault("the worker task", "the error msg").toDruidException();
    Assert.assertEquals("WorkerRpcFailed", e.getErrorCode());
    Assert.assertEquals(DruidException.Category.RUNTIME_FAILURE, e.getCategory());
    Assert.assertEquals(DruidException.Persona.OPERATOR, e.getTargetPersona());
  }
}
