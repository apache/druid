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

package org.apache.druid.msq.indexing;

import com.google.common.collect.ImmutableList;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.Druids;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.sql.calcite.planner.ColumnMapping;
import org.apache.druid.sql.calcite.planner.ColumnMappings;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;

public class MSQControllerTaskTest
{
  MSQSpec MSQ_SPEC = MSQSpec
      .builder()
      .destination(new DataSourceMSQDestination(
          "target",
          Granularities.DAY,
          null,
          null
      ))
      .query(new Druids.ScanQueryBuilder()
                 .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                 .legacy(false)
                 .intervals(new MultipleIntervalSegmentSpec(
                     Collections.singletonList(Intervals.of(
                         "2011-04-01T00:00:00.000Z/2011-04-03T00:00:00.000Z"))))
                 .dataSource("target")
                 .build()
      )
      .columnMappings(new ColumnMappings(ImmutableList.of(new ColumnMapping("a0", "cnt"))))
      .tuningConfig(MSQTuningConfig.defaultConfig())
      .build();

  @Test
  public void testGetInputSourceResources()
  {
    MSQControllerTask msqWorkerTask = new MSQControllerTask(
        null,
        MSQ_SPEC,
        null,
        null,
        null,
        null,
        null);
    Assert.assertTrue(msqWorkerTask.getInputSourceResources().isEmpty());
  }
}
