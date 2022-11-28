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

import com.google.common.collect.ImmutableSet;
import org.apache.druid.indexing.common.actions.RetrieveUsedSegmentsAction;
import org.apache.druid.indexing.common.actions.SegmentAllocateAction;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.msq.indexing.error.InsertCannotAllocateSegmentFault;
import org.apache.druid.msq.indexing.error.InsertCannotBeEmptyFault;
import org.apache.druid.msq.indexing.error.InsertCannotOrderByDescendingFault;
import org.apache.druid.msq.indexing.error.InsertCannotReplaceExistingSegmentFault;
import org.apache.druid.msq.indexing.error.InsertTimeOutOfBoundsFault;
import org.apache.druid.msq.test.MSQTestBase;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.timeline.DataSegment;
import org.junit.Test;
import org.mockito.Mockito;

import static org.mockito.ArgumentMatchers.isA;

public class MSQFaultsTest extends MSQTestBase
{
  @Test
  public void testInsertCannotAllocateSegmentFault()
  {
    RowSignature rowSignature = RowSignature.builder()
                                            .add("__time", ColumnType.LONG)
                                            .add("dim1", ColumnType.STRING)
                                            .add("cnt", ColumnType.LONG).build();

    // If there is some problem allocating the segment,task action client will return a null value.
    Mockito.doReturn(null).when(testTaskActionClient).submit(isA(SegmentAllocateAction.class));

    testIngestQuery().setSql(
                         "insert into foo1 select  __time, dim1 , count(*) as cnt from foo where dim1 is not null and __time >= TIMESTAMP '2000-01-02 00:00:00' and __time < TIMESTAMP '2000-01-03 00:00:00' group by 1, 2 PARTITIONED by day clustered by dim1")
                     .setExpectedDataSource("foo1")
                     .setExpectedRowSignature(rowSignature)
                     .setExpectedMSQFault(
                         new InsertCannotAllocateSegmentFault(
                             "foo1",
                             Intervals.of("2000-01-02T00:00:00.000Z/2000-01-03T00:00:00.000Z")
                         )
                     )
                     .verifyResults();
  }

  @Test
  public void testInsertCannotBeEmptyFault()
  {
    RowSignature rowSignature = RowSignature.builder()
                                            .add("__time", ColumnType.LONG)
                                            .add("dim1", ColumnType.STRING)
                                            .add("cnt", ColumnType.LONG).build();

    //Insert with a condition which results in 0 rows being inserted
    testIngestQuery().setSql(
                         "insert into foo1 select  __time, dim1 , count(*) as cnt from foo where dim1 is not null and __time < TIMESTAMP '1971-01-01 00:00:00' group by 1, 2 PARTITIONED by day clustered by dim1")
                     .setExpectedDataSource("foo1")
                     .setExpectedRowSignature(rowSignature)
                     .setExpectedMSQFault(new InsertCannotBeEmptyFault("foo1"))
                     .verifyResults();
  }

  @Test
  public void testInsertCannotOrderByDescendingFault()
  {
    RowSignature rowSignature = RowSignature.builder()
                                            .add("__time", ColumnType.LONG)
                                            .add("dim1", ColumnType.STRING)
                                            .add("cnt", ColumnType.LONG).build();

    // Add an DESC clustered by column, which should not be allowed
    testIngestQuery().setSql(
                         "insert into foo1 select  __time, dim1 , count(*) as cnt from foo where dim1 is not null and __time < TIMESTAMP '2000-01-02 00:00:00' group by 1, 2 PARTITIONED by day clustered by dim1 DESC")
                     .setExpectedDataSource("foo1")
                     .setExpectedRowSignature(rowSignature)
                     .setExpectedMSQFault(new InsertCannotOrderByDescendingFault("d1"))
                     .verifyResults();
  }

  @Test
  public void testInsertCannotReplaceExistingSegmentFault()
  {
    RowSignature rowSignature = RowSignature.builder()
                                            .add("__time", ColumnType.LONG)
                                            .add("dim1", ColumnType.STRING)
                                            .add("cnt", ColumnType.LONG).build();

    // Create a datasegment which lies partially outside the generated segment
    DataSegment existingDataSegment = DataSegment.builder()
                                   .interval(Intervals.of("2001-01-01T/2003-01-04T"))
                                   .size(50)
                                   .version("1").dataSource("foo1")
                                   .build();
    Mockito.doReturn(ImmutableSet.of(existingDataSegment)).when(testTaskActionClient).submit(isA(RetrieveUsedSegmentsAction.class));

    testIngestQuery().setSql(
                         "replace into foo1 overwrite where __time >= TIMESTAMP '2000-01-01 00:00:00' and __time < TIMESTAMP '2002-01-03 00:00:00' select  __time, dim1 , count(*) as cnt from foo where dim1 is not null group by 1, 2 PARTITIONED by day clustered by dim1")
                     .setExpectedDataSource("foo1")
                     .setExpectedRowSignature(rowSignature)
                     .setExpectedMSQFault(new InsertCannotReplaceExistingSegmentFault(existingDataSegment.getId()))
                     .verifyResults();
  }

  @Test
  public void testInsertTimeOutOfBoundsFault()
  {
    RowSignature rowSignature = RowSignature.builder()
                                            .add("__time", ColumnType.LONG)
                                            .add("dim1", ColumnType.STRING)
                                            .add("cnt", ColumnType.LONG).build();

    // Add a REPLACE statement which replaces a different partition than the ones which rows are present for. The generated
    // partition will be outside the replace interval which should throw an InsertTimeOutOfBoundsFault.
    testIngestQuery().setSql(
                         "replace into foo1 overwrite where __time >= TIMESTAMP '2002-01-02 00:00:00' and __time < TIMESTAMP '2002-01-03 00:00:00' select  __time, dim1 , count(*) as cnt from foo where dim1 is not null group by 1, 2 PARTITIONED by day clustered by dim1")
                     .setExpectedDataSource("foo1")
                     .setExpectedRowSignature(rowSignature)
                     .setExpectedMSQFault(new InsertTimeOutOfBoundsFault(Intervals.of("2000-01-02T00:00:00.000Z/2000-01-03T00:00:00.000Z")))
                     .verifyResults();
  }
}
