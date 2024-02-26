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

package org.apache.druid.sql.calcite.planner;

import org.apache.druid.sql.calcite.table.DatasourceTable.PhysicalDatasourceMetadata;
import org.apache.druid.sql.calcite.table.DruidTable;

import java.util.Collections;
import java.util.Set;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Test class to reference items that are otherwise unused in this package, and
 * only used in extensions. Prevents GitHub CodeQL warnings.
 */
public class TrivialTest
{
  private static class DummyCatalogResolver implements CatalogResolver
  {
    @Override
    public boolean ingestRequiresExistingTable()
    {
      return true;
    }

    @Override
    public DruidTable resolveDatasource(String tableName, PhysicalDatasourceMetadata dsMetadata)
    {
      assertTrue("foo".equals(tableName));
      return null;
    }

    @Override
    public Set<String> getTableNames(Set<String> datasourceNames)
    {
      return Collections.emptySet();
    }
  }

  /**
   * This is a silly test. It exists only to create references to otherwise-unused
   * method parameters.
   */
  public void testResolver()
  {
    CatalogResolver resolver = new DummyCatalogResolver();
    assertTrue(resolver.ingestRequiresExistingTable());
    assertNull(resolver.resolveDatasource("foo", null));
    assertTrue(resolver.getTableNames(Collections.emptySet()).isEmpty());
  }
}
