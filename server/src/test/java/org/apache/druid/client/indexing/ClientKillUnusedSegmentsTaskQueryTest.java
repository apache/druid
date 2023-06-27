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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ClientKillUnusedSegmentsTaskQueryTest
{
  private static final String DATA_SOURCE = "data_source";
  private static final DateTime START = DateTimes.nowUtc();
  private static final Interval INTERVAL = new Interval(START, START.plus(1));
  private static final Boolean MARK_UNUSED = true;

  ClientKillUnusedSegmentsTaskQuery clientKillUnusedSegmentsQuery;

  @BeforeEach
  void setUp()
  {
    clientKillUnusedSegmentsQuery = new ClientKillUnusedSegmentsTaskQuery("killTaskId", DATA_SOURCE, INTERVAL, true);
  }

  @AfterEach
  void tearDown()
  {
    clientKillUnusedSegmentsQuery = null;
  }

  @Test
  void testGetType()
  {
    assertEquals("kill", clientKillUnusedSegmentsQuery.getType());
  }

  @Test
  void testGetDataSource()
  {
    assertEquals(DATA_SOURCE, clientKillUnusedSegmentsQuery.getDataSource());
  }

  @Test
  void testGetInterval()
  {
    assertEquals(INTERVAL, clientKillUnusedSegmentsQuery.getInterval());
  }

  @Test
  void testGetMarkUnused()
  {
    assertEquals(MARK_UNUSED, clientKillUnusedSegmentsQuery.getMarkAsUnused());
  }

  @Test
  void testEquals()
  {
    EqualsVerifier.forClass(ClientKillUnusedSegmentsTaskQuery.class)
                  .usingGetClass()
                  .withNonnullFields("id", "dataSource", "interval")
                  .verify();
  }
}
