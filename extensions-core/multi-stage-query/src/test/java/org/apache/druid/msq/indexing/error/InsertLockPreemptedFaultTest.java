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

import org.apache.druid.java.util.common.ISE;
import org.apache.druid.msq.test.MSQTestBase;
import org.apache.druid.msq.test.MSQTestTaskActionClient;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.junit.Test;

public class InsertLockPreemptedFaultTest extends MSQTestBase
{

  @Test
  public void testThrowsInsertLockPreemptedFault()
  {
    LockPreemptedHelper.preempt(true);

    RowSignature rowSignature = RowSignature.builder()
                                            .add("__time", ColumnType.LONG)
                                            .add("dim1", ColumnType.STRING)
                                            .add("cnt", ColumnType.LONG).build();

    testIngestQuery().setSql(
                         "insert into foo1 select  __time, dim1 , count(*) as cnt from foo where dim1 is not null "
                         + "group by 1, 2 PARTITIONED by day clustered by dim1")
                     .setExpectedDataSource("foo1")
                     .setExpectedRowSignature(rowSignature)
                     .setExpectedMSQFault(InsertLockPreemptedFault.instance())
                     .verifyResults();

  }

  /**
   * Dummy class for {@link MSQTestTaskActionClient} to determine whether
   * to grant or preempt the lock. This should be ideally done via injectors
   */
  public static class LockPreemptedHelper
  {
    private static boolean preempted = false;

    public static void preempt(final boolean preempted)
    {
      LockPreemptedHelper.preempted = preempted;
    }

    public static void throwIfPreempted()
    {
      if (preempted) {
        throw new ISE(
            "Segments[dummySegment] are not covered by locks[dummyLock] for task[dummyTask]"
        );
      }
    }
  }
}
