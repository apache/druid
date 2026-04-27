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

package org.apache.druid.java.util.common.granularity;

import org.joda.time.DateTimeZone;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Test;

public class PeriodGranularityBugTest
{
  @Test(timeout = 5000)
  public void testCompoundPeriodWithTimeZone()
  {
    // America/New_York has DST, so days are imprecise
    PeriodGranularity pg = new PeriodGranularity(new Period("PT1M1S"), null, DateTimeZone.forID("America/New_York"));
    long time = 1704067200000L; // 2024-01-01T00:00:00Z
    long start = System.currentTimeMillis();
    long bucket = pg.bucketStart(time);
    long end = System.currentTimeMillis();
    
    Assert.assertTrue("Should run quickly", (end - start) < 1000);
    
    // Let's also check if it returns the same as a non-compound period of same duration (PT61S)
    PeriodGranularity pg2 = new PeriodGranularity(new Period("PT61S"), null, DateTimeZone.forID("America/New_York"));
    long bucket2 = pg2.bucketStart(time);
    Assert.assertEquals(bucket2, bucket);
  }
}

