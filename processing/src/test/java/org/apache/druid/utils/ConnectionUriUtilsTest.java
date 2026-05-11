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

package org.apache.druid.utils;

import com.google.common.collect.ImmutableSet;
import org.apache.druid.java.util.common.IAE;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Set;

public class ConnectionUriUtilsTest
{
  public static class ThrowIfURLHasNotAllowedPropertiesTest
  {
    private static final String MYSQL_URI = "jdbc:mysql://localhost:3306/test?user=druid&password=diurd&keyonly&otherOptions=wat";
    private static final String MARIA_URI = "jdbc:mariadb://localhost:3306/test?user=druid&password=diurd&keyonly&otherOptions=wat";
    private static final String POSTGRES_URI = "jdbc:postgresql://localhost:3306/test?user=druid&password=diurd&keyonly&otherOptions=wat";
    private static final String UNKNOWN_URI = "jdbc:druid://localhost:8888/query/v2/sql/avatica?user=druid&password=diurd&keyonly&otherOptions=wat";

    @Test
    public void testEmptyActualProperties()
    {
      ConnectionUriUtils.throwIfPropertiesAreNotAllowed(
          ImmutableSet.of(),
          ImmutableSet.of("valid_key1", "valid_key2"),
          ImmutableSet.of("system_key1", "system_key2")
      );
    }

    @Test
    public void testThrowForNonAllowedProperties()
    {
      IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class, () ->
          ConnectionUriUtils.throwIfPropertiesAreNotAllowed(
              ImmutableSet.of("valid_key1", "invalid_key"),
              ImmutableSet.of("system_key1", "system_key2"),
              ImmutableSet.of("valid_key1", "valid_key2")
          )
      );
      Assertions.assertTrue(e.getMessage().contains("The property [invalid_key] is not in the allowed list [valid_key1, valid_key2]"));
    }

    @Test
    public void testAllowedProperties()
    {
      ConnectionUriUtils.throwIfPropertiesAreNotAllowed(
          ImmutableSet.of("valid_key2"),
          ImmutableSet.of("system_key1", "system_key2"),
          ImmutableSet.of("valid_key1", "valid_key2")
      );
    }

    @Test
    public void testAllowSystemProperties()
    {
      ConnectionUriUtils.throwIfPropertiesAreNotAllowed(
          ImmutableSet.of("system_key1", "valid_key2"),
          ImmutableSet.of("system_key1", "system_key2"),
          ImmutableSet.of("valid_key1", "valid_key2")
      );
    }

    @Test
    public void testMatchSystemProperties()
    {
      ConnectionUriUtils.throwIfPropertiesAreNotAllowed(
          ImmutableSet.of("system_key1.1", "system_key1.5", "system_key11.11", "valid_key2"),
          ImmutableSet.of("system_key1", "system_key2"),
          ImmutableSet.of("valid_key1", "valid_key2")
      );
    }

    @Test
    public void testTryParses()
    {
      Set<String> props = ConnectionUriUtils.tryParseJdbcUriParameters(POSTGRES_URI, false);
      Assertions.assertEquals(7, props.size());

      props = ConnectionUriUtils.tryParseJdbcUriParameters(MYSQL_URI, false);
      // though this would be 4 if mysql wasn't loaded in classpath because it would fall back to mariadb
      Assertions.assertEquals(6, props.size());

      props = ConnectionUriUtils.tryParseJdbcUriParameters(MARIA_URI, false);
      Assertions.assertEquals(4, props.size());
    }

    @Test
    public void testTryParseUnknown()
    {
      Set<String> props = ConnectionUriUtils.tryParseJdbcUriParameters(UNKNOWN_URI, true);
      Assertions.assertEquals(0, props.size());

      Assertions.assertThrows(IAE.class, () -> ConnectionUriUtils.tryParseJdbcUriParameters(UNKNOWN_URI, false));
    }

    @Test
    public void tryParseInvalidPostgres()
    {
      Assertions.assertThrows(IAE.class, () -> ConnectionUriUtils.tryParseJdbcUriParameters("jdbc:postgresql://bad:1234&param", true));
    }

    @Test
    public void testMySqlFallbackMySqlMaria2x()
    {
      MockedStatic<ConnectionUriUtils> utils = Mockito.mockStatic(ConnectionUriUtils.class);
      utils.when(() -> ConnectionUriUtils.tryParseJdbcUriParameters(MYSQL_URI, false)).thenCallRealMethod();
      utils.when(() -> ConnectionUriUtils.tryParseMySqlConnectionUri(MYSQL_URI)).thenThrow(ClassNotFoundException.class);
      utils.when(() -> ConnectionUriUtils.tryParseMariaDb2xConnectionUri(MYSQL_URI)).thenCallRealMethod();

      Set<String> props = ConnectionUriUtils.tryParseJdbcUriParameters(MYSQL_URI, false);
      // this would be 9 if didn't fall back to mariadb
      Assertions.assertEquals(4, props.size());
      utils.close();
    }

    @Test
    public void testMariaFallbackMaria3x()
    {
      MockedStatic<ConnectionUriUtils> utils = Mockito.mockStatic(ConnectionUriUtils.class);
      utils.when(() -> ConnectionUriUtils.tryParseJdbcUriParameters(MARIA_URI, false)).thenCallRealMethod();
      utils.when(() -> ConnectionUriUtils.tryParseMariaDb2xConnectionUri(MARIA_URI)).thenThrow(ClassNotFoundException.class);
      utils.when(() -> ConnectionUriUtils.tryParseMariaDb3xConnectionUri(MARIA_URI)).thenCallRealMethod();

      try {
        Set<String> props = ConnectionUriUtils.tryParseJdbcUriParameters(MARIA_URI, false);
        // this would be 4 if didn't fall back to mariadb 3x
        Assertions.assertEquals(8, props.size());
      }
      catch (RuntimeException e) {

        Assertions.assertTrue(e.getMessage().contains("Failed to find MariaDB driver class"));
      }
      utils.close();
    }

    @Test
    public void testMySqlFallbackMySqlNoDrivers()
    {
      MockedStatic<ConnectionUriUtils> utils = Mockito.mockStatic(ConnectionUriUtils.class);
      utils.when(() -> ConnectionUriUtils.tryParseJdbcUriParameters(MYSQL_URI, false)).thenCallRealMethod();
      utils.when(() -> ConnectionUriUtils.tryParseMySqlConnectionUri(MYSQL_URI)).thenThrow(ClassNotFoundException.class);
      utils.when(() -> ConnectionUriUtils.tryParseMariaDb2xConnectionUri(MYSQL_URI)).thenThrow(ClassNotFoundException.class);

      try {
        ConnectionUriUtils.tryParseJdbcUriParameters(MYSQL_URI, false);
      }
      catch (RuntimeException e) {
        Assertions.assertTrue(e.getMessage().contains("Failed to find MySQL driver class"));
      }
      utils.close();
    }

    @Test
    public void testPosgresDriver() throws Exception
    {
      Set<String> props = ConnectionUriUtils.tryParsePostgresConnectionUri(POSTGRES_URI);
      Assertions.assertEquals(7, props.size());
      // postgres adds a few extra system properties, PGDBNAME, PGHOST, PGPORT
      Assertions.assertTrue(props.contains("user"));
      Assertions.assertTrue(props.contains("password"));
      Assertions.assertTrue(props.contains("otherOptions"));
      Assertions.assertTrue(props.contains("keyonly"));
    }

    @Test
    public void testMySQLDriver() throws Exception
    {
      Set<String> props = ConnectionUriUtils.tryParseMySqlConnectionUri(MYSQL_URI);
      // mysql actually misses 'keyonly', but spits out several keys that are not actually uri parameters
      // DBNAME, HOST, PORT, HOST.1, PORT.1, NUM_HOSTS
      Assertions.assertEquals(6, props.size());
      Assertions.assertTrue(props.contains("user"));
      Assertions.assertTrue(props.contains("password"));
      Assertions.assertTrue(props.contains("otherOptions"));
      Assertions.assertFalse(props.contains("keyonly"));
    }

    @Test
    public void testMariaDb2xDriver() throws Throwable
    {
      Set<String> props = ConnectionUriUtils.tryParseMariaDb2xConnectionUri(MYSQL_URI);
      // mariadb doesn't spit out any extras other than what the user specified
      Assertions.assertEquals(4, props.size());
      Assertions.assertTrue(props.contains("user"));
      Assertions.assertTrue(props.contains("password"));
      Assertions.assertTrue(props.contains("otherOptions"));
      Assertions.assertTrue(props.contains("keyonly"));
      props = ConnectionUriUtils.tryParseMariaDb2xConnectionUri(MARIA_URI);
      Assertions.assertEquals(4, props.size());
      Assertions.assertTrue(props.contains("user"));
      Assertions.assertTrue(props.contains("password"));
      Assertions.assertTrue(props.contains("otherOptions"));
      Assertions.assertTrue(props.contains("keyonly"));
    }

    @Test
    public void testMariaDb3xDriver() throws Exception
    {
      // at the time of adding this test, mariadb connector/j 3.x does not actually parse jdbc:mysql uris
      // so this would throw an IAE.class instead of ClassNotFoundException.class if the connector is swapped out
      // in maven dependencies
      Assertions.assertThrows(ClassNotFoundException.class, () ->
          ConnectionUriUtils.tryParseMariaDb3xConnectionUri(MYSQL_URI)
      );
    }

    @Test
    public void testMariaDb3xDriverMariaUri() throws Exception
    {
      // mariadb 3.x driver cannot be loaded alongside 2.x, so this will fail with class not found
      // however, if we swap out version in pom then we end up with 8 keys where
      // "database", "addresses", "codecs", and "initialUrl" are added as extras
      // we should perhaps consider adding them to built-in allowed lists in the future when this driver is no longer
      // an alpha release
      Assertions.assertThrows(ClassNotFoundException.class, () -> {
        Set<String> props = ConnectionUriUtils.tryParseMariaDb3xConnectionUri(MARIA_URI);
        Assertions.assertEquals(8, props.size());
        Assertions.assertTrue(props.contains("user"));
        Assertions.assertTrue(props.contains("password"));
        Assertions.assertTrue(props.contains("otherOptions"));
        Assertions.assertTrue(props.contains("keyonly"));
      });
    }

    @Test
    public void testPostgresInvalidArgs() throws Exception
    {
      Assertions.assertThrows(IAE.class, () -> ConnectionUriUtils.tryParsePostgresConnectionUri(MYSQL_URI));
    }

    @Test
    public void testMySqlInvalidArgs() throws Exception
    {
      Assertions.assertThrows(IAE.class, () -> ConnectionUriUtils.tryParseMySqlConnectionUri(POSTGRES_URI));
    }

    @Test
    public void testMariaDbInvalidArgs() throws Exception
    {
      Assertions.assertThrows(IAE.class, () -> ConnectionUriUtils.tryParseMariaDb2xConnectionUri(POSTGRES_URI));
    }
  }
}
