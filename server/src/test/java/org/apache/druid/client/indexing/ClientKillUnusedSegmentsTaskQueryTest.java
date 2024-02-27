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

package org.apache.druid.client.indexing;

import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.java.util.common.DateTimes;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ClientKillUnusedSegmentsTaskQueryTest
{
  private static final String DATA_SOURCE = "data_source";
  public static final DateTime START = DateTimes.nowUtc();
  private static final Interval INTERVAL = new Interval(START, START.plus(1));
  private static final Boolean MARK_UNUSED = true;
  private static final Integer BATCH_SIZE = 999;
  private static final Integer LIMIT = 1000;

  ClientKillUnusedSegmentsTaskQuery clientKillUnusedSegmentsQuery;

  @Before
  public void setUp()
  {
    clientKillUnusedSegmentsQuery = new ClientKillUnusedSegmentsTaskQuery(
        "killTaskId",
        DATA_SOURCE,
        INTERVAL,
        true,
        BATCH_SIZE,
        LIMIT,
        null
    );
  }

  @After
  public void tearDown()
  {
    clientKillUnusedSegmentsQuery = null;
  }

  @Test
  public void testGetType()
  {
    Assert.assertEquals("kill", clientKillUnusedSegmentsQuery.getType());
  }

  @Test
  public void testGetDataSource()
  {
    Assert.assertEquals(DATA_SOURCE, clientKillUnusedSegmentsQuery.getDataSource());
  }

  @Test
  public void testGetInterval()
  {
    Assert.assertEquals(INTERVAL, clientKillUnusedSegmentsQuery.getInterval());
  }

  @Test
  public void testGetMarkUnused()
  {
    Assert.assertEquals(MARK_UNUSED, clientKillUnusedSegmentsQuery.getMarkAsUnused());
  }

  @Test
  public void testGetBatchSize()
  {
    Assert.assertEquals(BATCH_SIZE, clientKillUnusedSegmentsQuery.getBatchSize());
  }

  @Test
  public void testGetLimit()
  {
    Assert.assertEquals(LIMIT, clientKillUnusedSegmentsQuery.getLimit());
  }

  @Test
  public void testEquals()
  {
    EqualsVerifier.forClass(ClientKillUnusedSegmentsTaskQuery.class)
                  .usingGetClass()
                  .withNonnullFields("id", "dataSource", "interval", "batchSize", "limit")
                  .verify();
  }
}
