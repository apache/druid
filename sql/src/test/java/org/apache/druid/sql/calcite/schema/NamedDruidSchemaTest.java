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

package org.apache.druid.sql.calcite.schema;

import org.apache.druid.sql.calcite.util.CalciteTestBase;
import org.easymock.EasyMockRunner;
import org.easymock.Mock;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;

import static org.junit.jupiter.api.Assertions.assertEquals;

@RunWith(EasyMockRunner.class)
class NamedDruidSchemaTest extends CalciteTestBase
{
  private static final String SCHEMA_NAME = "SCHEMA_NAME";

  @Mock
  private DruidSchema druidSchema;

  private NamedDruidSchema target;

  @BeforeEach
  void setUp()
  {
    target = new NamedDruidSchema(druidSchema, SCHEMA_NAME);
  }

  @Test
  void getSchemaNameShouldReturnName()
  {
    assertEquals(SCHEMA_NAME, target.getSchemaName());
  }

  @Test
  void getSchemaShouldReturnSchema()
  {
    assertEquals(druidSchema, target.getSchema());
  }
}
