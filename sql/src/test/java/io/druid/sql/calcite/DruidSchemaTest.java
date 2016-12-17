/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.sql.calcite;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import io.druid.sql.calcite.planner.PlannerConfig;
import io.druid.sql.calcite.table.DruidTable;
import io.druid.sql.calcite.util.CalciteTests;
import io.druid.sql.calcite.util.SpecificSegmentsQuerySegmentWalker;
import io.druid.sql.calcite.util.TestServerInventoryView;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class DruidSchemaTest
{
  private static final PlannerConfig PLANNER_CONFIG_DEFAULT = new PlannerConfig();
  private static final PlannerConfig PLANNER_CONFIG_NO_TOPN = new PlannerConfig()
  {
    @Override
    public int getMaxTopNLimit()
    {
      return 0;
    }

    @Override
    public boolean isUseApproximateTopN()
    {
      return false;
    }
  };

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private SpecificSegmentsQuerySegmentWalker walker = null;
  private DruidSchema schema = null;
  private Connection connection = null;

  @Before
  public void setUp() throws Exception
  {
    walker = CalciteTests.createWalker(temporaryFolder.newFolder());

    Properties props = new Properties();
    props.setProperty("caseSensitive", "true");
    props.setProperty("unquotedCasing", "UNCHANGED");
    connection = DriverManager.getConnection("jdbc:calcite:", props);
    CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);

    schema = new DruidSchema(
        walker,
        new TestServerInventoryView(walker.getSegments()),
        PLANNER_CONFIG_DEFAULT
    );

    calciteConnection.getRootSchema().add("s", schema);
    schema.start();
    schema.awaitInitialization();
  }

  @After
  public void tearDown() throws Exception
  {
    schema.stop();
    walker.close();
    connection.close();
  }

  @Test
  public void testGetTableMap()
  {
    Assert.assertEquals(ImmutableSet.of("foo"), schema.getTableNames());

    final Map<String, Table> tableMap = schema.getTableMap();
    Assert.assertEquals(1, tableMap.size());
    Assert.assertEquals("foo", Iterables.getOnlyElement(tableMap.keySet()));

    final DruidTable druidTable = (DruidTable) Iterables.getOnlyElement(tableMap.values());
    final RelDataType rowType = druidTable.getRowType(new JavaTypeFactoryImpl());
    final List<RelDataTypeField> fields = rowType.getFieldList();

    Assert.assertEquals(5, fields.size());

    Assert.assertEquals("__time", fields.get(0).getName());
    Assert.assertEquals(SqlTypeName.TIMESTAMP, fields.get(0).getType().getSqlTypeName());

    Assert.assertEquals("cnt", fields.get(1).getName());
    Assert.assertEquals(SqlTypeName.BIGINT, fields.get(1).getType().getSqlTypeName());

    Assert.assertEquals("dim1", fields.get(2).getName());
    Assert.assertEquals(SqlTypeName.VARCHAR, fields.get(2).getType().getSqlTypeName());

    Assert.assertEquals("dim2", fields.get(3).getName());
    Assert.assertEquals(SqlTypeName.VARCHAR, fields.get(3).getType().getSqlTypeName());

    Assert.assertEquals("m1", fields.get(4).getName());
    Assert.assertEquals(SqlTypeName.FLOAT, fields.get(4).getType().getSqlTypeName());
  }
}
