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

package org.apache.druid.jdbc;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.jdbc.http.DruidHttpClient;
import org.apache.druid.jdbc.http.SqlRequest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DruidJdbcDriverTest
{
  private final DruidJdbcDriver driver = new DruidJdbcDriver();

  @Test
  public void testAcceptsValidUrls()
  {
    Assertions.assertTrue(driver.acceptsURL("jdbc:druid:http://localhost:8888/druid/v2/sql/"));
    Assertions.assertTrue(driver.acceptsURL("jdbc:druid:http://localhost:8888/druid/v2/sql/druid"));
    Assertions.assertTrue(driver.acceptsURL("jdbc:druid:http://localhost:8888/druid/v2/sql/path/to/broker"));
    Assertions.assertTrue(driver.acceptsURL("jdbc:druid:http://example.com:9999/"));
    Assertions.assertTrue(driver.acceptsURL("jdbc:druid:http://192.168.1.100:8888/"));
  }

  @Test
  public void testRejectsInvalidUrls()
  {
    Assertions.assertFalse(driver.acceptsURL(null));
    Assertions.assertFalse(driver.acceptsURL(""));
    Assertions.assertFalse(driver.acceptsURL("jdbc:mysql://localhost:3306/test"));
    Assertions.assertFalse(driver.acceptsURL("http://localhost:8888/"));
    Assertions.assertFalse(driver.acceptsURL("druid://localhost:8888/"));
    Assertions.assertFalse(driver.acceptsURL("jdbc:postgresql://localhost:5432/test"));
  }

  @Test
  public void testConnectClosesHttpClientWhenValidationFails() throws SQLException
  {
    final DruidConnectionUrl connectionUrl =
        DruidConnectionUrl.parse("jdbc:druid:http://localhost:8888/druid/v2/sql/", null);

    final DruidHttpClient httpClient = mock(DruidHttpClient.class);
    when(httpClient.runQuery(any(SqlRequest.class))).thenThrow(new DruidJdbcException("validation failed"));

    final DruidConnection connection = new DruidConnection(connectionUrl, httpClient, new ObjectMapper());
    Assertions.assertThrows(SQLException.class, () -> DruidJdbcDriver.validateOrClose(connection));

    verify(httpClient).close();
    Assertions.assertTrue(connection.isClosed());
  }

  @Test
  public void testParseVersion()
  {
    Assertions.assertEquals(38, DruidJdbcDriver.parseMajorVersion("38.0.0-SNAPSHOT"));
    Assertions.assertEquals(0, DruidJdbcDriver.parseMinorVersion("38.0.0-SNAPSHOT"));
    Assertions.assertEquals(37, DruidJdbcDriver.parseMajorVersion("37.1.0"));
    Assertions.assertEquals(1, DruidJdbcDriver.parseMinorVersion("37.1.0"));
    Assertions.assertEquals(36, DruidJdbcDriver.parseMajorVersion("36.0"));
    Assertions.assertEquals(0, DruidJdbcDriver.parseMinorVersion("36.0"));
    Assertions.assertEquals(5, DruidJdbcDriver.parseMajorVersion("5"));
    Assertions.assertEquals(0, DruidJdbcDriver.parseMinorVersion("5"));
    Assertions.assertEquals(42, DruidJdbcDriver.parseMajorVersion("42-beta"));
    Assertions.assertEquals(0, DruidJdbcDriver.parseMinorVersion("42-beta"));
    Assertions.assertEquals(1, DruidJdbcDriver.parseMajorVersion("1.3-rc1"));
    Assertions.assertEquals(3, DruidJdbcDriver.parseMinorVersion("1.3-rc1"));
    // An unparseable component reads as 0 rather than failing.
    Assertions.assertEquals(0, DruidJdbcDriver.parseMajorVersion("abc"));
    Assertions.assertEquals(0, DruidJdbcDriver.parseMinorVersion("abc"));
    Assertions.assertEquals(0, DruidJdbcDriver.parseMajorVersion(""));
    Assertions.assertEquals(0, DruidJdbcDriver.parseMinorVersion(""));
  }
}
