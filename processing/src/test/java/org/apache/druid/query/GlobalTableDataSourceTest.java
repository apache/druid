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

package org.apache.druid.query;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.segment.TestHelper;
import org.junit.Assert;
import org.junit.Test;

public class GlobalTableDataSourceTest
{
  private static final GlobalTableDataSource GLOBAL_TABLE_DATA_SOURCE = new GlobalTableDataSource("foo");

  @Test
  public void testEquals()
  {
    EqualsVerifier.forClass(GlobalTableDataSource.class)
                  .usingGetClass()
                  .withNonnullFields("name")
                  .verify();
  }

  @Test
  public void testGlobalTableIsNotEqualsTable()
  {
    TableDataSource tbl = new TableDataSource(GLOBAL_TABLE_DATA_SOURCE.getName());
    Assert.assertNotEquals(GLOBAL_TABLE_DATA_SOURCE, tbl);
    Assert.assertNotEquals(tbl, GLOBAL_TABLE_DATA_SOURCE);
  }

  @Test
  public void testIsGlobal()
  {
    Assert.assertTrue(GLOBAL_TABLE_DATA_SOURCE.isGlobal());
  }

  @Test
  public void testSerde() throws JsonProcessingException
  {
    final ObjectMapper jsonMapper = TestHelper.makeJsonMapper();
    final GlobalTableDataSource deserialized = (GlobalTableDataSource) jsonMapper.readValue(
        jsonMapper.writeValueAsString(GLOBAL_TABLE_DATA_SOURCE),
        DataSource.class
    );

    Assert.assertEquals(GLOBAL_TABLE_DATA_SOURCE, deserialized);
  }
}
