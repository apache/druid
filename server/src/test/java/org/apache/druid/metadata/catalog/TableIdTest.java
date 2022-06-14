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

package org.apache.druid.metadata.catalog;

import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.catalog.TableId;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Tests the various catalog table record objects. These are mostly
 * just "data objects" that do nothing other than hold data.
 */
public class TableIdTest
{
  @Test
  public void testId()
  {
    TableId id1 = new TableId("schema", "table");
    assertEquals(id1, id1);
    assertEquals("schema", id1.schema());
    assertEquals("table", id1.name());
    assertEquals("\"schema\".\"table\"", id1.sqlName());
    assertEquals(id1.sqlName(), id1.toString());

    TableId id2 = TableId.datasource("ds");
    assertEquals(TableId.DRUID_SCHEMA, id2.schema());
    assertEquals("ds", id2.name());
  }

  @Test
  public void testEquals()
  {
    EqualsVerifier.forClass(TableId.class)
                  .usingGetClass()
                  .verify();
  }
}
