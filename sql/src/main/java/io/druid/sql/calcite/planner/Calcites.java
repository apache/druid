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

package io.druid.sql.calcite.planner;

import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.jdbc.CalciteJdbc41Factory;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.jdbc.Driver;
import org.apache.calcite.linq4j.function.Function0;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;

import java.sql.SQLException;
import java.util.Properties;

/**
 * Entry points for Calcite.
 */
public class Calcites
{
  private static final String DRUID_SCHEMA_NAME = "druid";

  private Calcites()
  {
    // No instantiation.
  }

  /**
   * Create a Calcite JDBC driver.
   *
   * @param druidSchema "druid" schema
   *
   * @return JDBC driver
   */
  public static CalciteConnection jdbc(
      final Schema druidSchema,
      final PlannerConfig plannerConfig
  ) throws SQLException
  {
    final Properties props = new Properties();
    props.setProperty("caseSensitive", "true");
    props.setProperty("unquotedCasing", "UNCHANGED");

    final CalciteJdbc41Factory jdbcFactory = new CalciteJdbc41Factory();
    final Function0<CalcitePrepare> prepareFactory = new Function0<CalcitePrepare>()
    {
      @Override
      public CalcitePrepare apply()
      {
        return new DruidPlannerImpl(plannerConfig);
      }
    };
    final Driver driver = new Driver()
    {
      @Override
      protected Function0<CalcitePrepare> createPrepareFactory()
      {
        return prepareFactory;
      }
    };
    final CalciteConnection calciteConnection = (CalciteConnection) jdbcFactory.newConnection(
        driver,
        jdbcFactory,
        "jdbc:calcite:",
        props
    );

    final SchemaPlus druidSchemaPlus = calciteConnection.getRootSchema().add(DRUID_SCHEMA_NAME, druidSchema);
    druidSchemaPlus.setCacheEnabled(false);
    return calciteConnection;
  }
}
