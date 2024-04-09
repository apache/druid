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

package org.apache.druid.quidem;

import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class DruidAvaticaDriverTest
{
  // create a new driver instance; this will load the class and register it
  DruidAvaticaTestDriver driver = new DruidAvaticaTestDriver();

  @Test
  public void testSelect() throws SQLException
  {
    try (Connection con = DriverManager.getConnection("druidtest:///");
        Statement stmt = con.createStatement();
        ResultSet rs = stmt.executeQuery("select 42");) {
      assertTrue(rs.next());
      assertEquals(42, rs.getInt(1));
      assertFalse(rs.next());
    }
  }

  @Test
  public void testURIParse() throws SQLException
  {
    DruidAvaticaTestDriver.buildConfigfromURIParams("druidtest:///");
  }
}
