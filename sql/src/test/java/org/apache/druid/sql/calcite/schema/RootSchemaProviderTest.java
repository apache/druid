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

import com.google.common.collect.ImmutableSet;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.sql.calcite.util.CalciteTestBase;
import org.easymock.EasyMock;
import org.easymock.EasyMockRunner;
import org.easymock.Mock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Set;

@RunWith(EasyMockRunner.class)
public class RootSchemaProviderTest extends CalciteTestBase
{
  private static final String SCHEMA_1 = "SCHEMA_1";
  private static final String SCHEMA_2 = "SCHEMA_2";
  @Mock
  private NamedSchema druidSchema1;
  @Mock
  private NamedSchema druidSchema2;
  @Mock
  private NamedSchema duplicateSchema1;
  @Mock
  private Schema schema1;
  @Mock
  private Schema schema2;
  @Mock
  private Schema schema3;
  private Set<NamedSchema> druidSchemas;

  private RootSchemaProvider target;

  @Before
  public void setUp()
  {
    EasyMock.expect(druidSchema1.getSchema()).andStubReturn(schema1);
    EasyMock.expect(druidSchema2.getSchema()).andStubReturn(schema2);
    EasyMock.expect(duplicateSchema1.getSchema()).andStubReturn(schema3);
    EasyMock.expect(druidSchema1.getSchemaName()).andStubReturn(SCHEMA_1);
    EasyMock.expect(druidSchema2.getSchemaName()).andStubReturn(SCHEMA_2);
    EasyMock.expect(duplicateSchema1.getSchemaName()).andStubReturn(SCHEMA_1);
    EasyMock.replay(druidSchema1, druidSchema2, duplicateSchema1);

    druidSchemas = ImmutableSet.of(druidSchema1, druidSchema2);
    target = new RootSchemaProvider(druidSchemas);
  }
  @Test
  public void testGetShouldReturnRootSchemaWithProvidedSchemasRegistered()
  {
    SchemaPlus rootSchema = target.get();
    Assert.assertEquals("", rootSchema.getName());
    Assert.assertFalse(rootSchema.isCacheEnabled());
    // metadata schema should not be added
    Assert.assertEquals(druidSchemas.size(), rootSchema.getSubSchemaNames().size());

    Assert.assertEquals(schema1, rootSchema.getSubSchema(SCHEMA_1).unwrap(schema1.getClass()));
    Assert.assertEquals(schema2, rootSchema.getSubSchema(SCHEMA_2).unwrap(schema2.getClass()));
  }

  @Test(expected = ISE.class)
  public void testGetWithDuplicateSchemasShouldThrowISE()
  {
    target = new RootSchemaProvider(ImmutableSet.of(druidSchema1, druidSchema2, duplicateSchema1));
    target.get();
  }
}
