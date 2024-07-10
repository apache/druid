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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.msq.test.MSQTestBase;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.hamcrest.CoreMatchers;
import org.junit.internal.matchers.ThrowableMessageMatcher;
import org.junit.jupiter.api.Test;

public class MSQSegmentMorphTest extends MSQTestBase
{
  @Test
  public void testSegmentMorphFactory()
  {
    testSegmentMorphFactoryCreator.setFrameProcessorFactory(new TestSegmentMorpherFrameProcessorFactory());

    RowSignature rowSignature = RowSignature.builder()
                                            .add("__time", ColumnType.LONG)
                                            .add("m1", ColumnType.FLOAT)
                                            .build();

    testIngestQuery().setSql(" REPLACE INTO foo "
                             + "OVERWRITE WHERE __time >= TIMESTAMP '2000-01-01' AND __time < TIMESTAMP '2000-01-04'"
                             + "SELECT __time, m1 "
                             + "FROM foo "
                             + "PARTITIONED BY DAY ")
                     .setExpectedDataSource("foo")
                     .setExpectedRowSignature(rowSignature)
                     .setQueryContext(DEFAULT_MSQ_CONTEXT)
                     .setExpectedTombstoneIntervals(ImmutableSet.of(Intervals.of("1970-01-01T00:00:00.000Z/2001-01-03T00:00:00.001Z")))
                     .setExpectedDestinationIntervals(Intervals.ONLY_ETERNITY)
                     .setExpectedResultRows(ImmutableList.of())
                     .setExpectedDestinationIntervals(ImmutableList.of(Intervals.of("2000-01-01T/2000-01-04T")))
                     .verifyResults();
  }

  @Test
  public void testSegmentMorphFactoryWithMultipleReplaceTimeChunks()
  {
    testSegmentMorphFactoryCreator.setFrameProcessorFactory(new TestSegmentMorpherFrameProcessorFactory());

    RowSignature rowSignature = RowSignature.builder()
                                            .add("__time", ColumnType.LONG)
                                            .add("m1", ColumnType.FLOAT)
                                            .build();

    testIngestQuery().setSql(" REPLACE INTO foo "
                             + "OVERWRITE WHERE __time >= TIMESTAMP '2000-01-01' AND __time < TIMESTAMP '2000-01-04' OR __time >= TIMESTAMP '2001-01-01' AND __time < TIMESTAMP '2001-01-04'"
                             + "SELECT __time, m1 "
                             + "FROM foo "
                             + "PARTITIONED BY DAY ")
                     .setExpectedDataSource("foo")
                     .setExpectedRowSignature(rowSignature)
                     .setQueryContext(DEFAULT_MSQ_CONTEXT)
                     .setExpectedExecutionErrorMatcher(
                         CoreMatchers.allOf(
                             CoreMatchers.instanceOf(ISE.class),
                             ThrowableMessageMatcher.hasMessage(
                                 CoreMatchers.containsString("Must have single interval in replaceTimeChunks, "
                                                             + "but got [[2000-01-01T00:00:00.000Z/2000-01-04T00:00:00.000Z, "
                                                             + "2001-01-01T00:00:00.000Z/2001-01-04T00:00:00.000Z]]")
                             )
                         )
                     )
                     .verifyExecutionError();
  }
}
