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

package org.apache.druid.indexing.common.task.batch.parallel;

import com.fasterxml.jackson.databind.ObjectMapper;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.indexing.common.task.batch.parallel.distribution.StringDistribution;
import org.apache.druid.indexing.common.task.batch.parallel.distribution.StringSketch;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.segment.TestHelper;
import org.joda.time.Interval;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;

public class DimensionDistributionReportTest
{
  private static final ObjectMapper OBJECT_MAPPER = ParallelIndexTestingFactory.createObjectMapper();

  private DimensionDistributionReport target;

  @Before
  public void setup()
  {
    Interval interval = Intervals.ETERNITY;
    StringSketch sketch = new StringSketch();
    Map<Interval, StringDistribution> intervalToDistribution = Collections.singletonMap(interval, sketch);
    String taskId = "abc";
    target = new DimensionDistributionReport(taskId, intervalToDistribution);
  }

  @Test
  public void serializesDeserializes()
  {
    TestHelper.testSerializesDeserializes(OBJECT_MAPPER, target);
  }

  @Test
  public void abidesEqualsContract()
  {
    EqualsVerifier.forClass(DimensionDistributionReport.class)
                  .usingGetClass()
                  .verify();
  }
}
