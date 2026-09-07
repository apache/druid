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

package org.apache.druid.server.lookup.jdbc;

import com.google.common.collect.ImmutableSet;
import org.apache.druid.metadata.MetadataStorageConnectorConfig;
import org.apache.druid.server.initialization.JdbcAccessSecurityConfig;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class JdbcDataFetcherUrlCheckTest
{
  private static final String TABLE_NAME = "tableName";
  private static final String KEY_COLUMN = "keyColumn";
  private static final String VALUE_COLUMN = "valueColumn";

  @Nested
  public class MySqlTest
  {

    @Test
    public void testCreateInstanceWhenUrlHasOnlyAllowedProperties()
    {
      new JdbcDataFetcher(
          new MetadataStorageConnectorConfig()
          {
            @Override
            public String getConnectURI()
            {
              return "jdbc:mysql://localhost:3306/db?valid_key1=val1&valid_key2=val2";
            }
          },
          TABLE_NAME,
          KEY_COLUMN,
          VALUE_COLUMN,
          100,
          new JdbcAccessSecurityConfig()
          {
            @Override
            public Set<String> getAllowedProperties()
            {
              return ImmutableSet.of("valid_key1", "valid_key2");
            }

            @Override
            public boolean isEnforceAllowedProperties()
            {
              return true;
            }
          }
      );
    }

    @Test
    public void testThrowWhenUrlHasDisallowedPropertiesWhenEnforcingAllowedProperties()
    {
      Throwable exception = assertThrows(IllegalArgumentException.class, () -> {
        new JdbcDataFetcher(
            new MetadataStorageConnectorConfig()
            {
              @Override
              public String getConnectURI()
              {
                return "jdbc:mysql://localhost:3306/db?invalid_key1=val1&valid_key2=val2";
              }
            },
            TABLE_NAME,
            KEY_COLUMN,
            VALUE_COLUMN,
            100,
            new JdbcAccessSecurityConfig()
            {
              @Override
              public Set<String> getAllowedProperties()
              {
                return ImmutableSet.of("valid_key1", "valid_key2");
              }

              @Override
              public boolean isEnforceAllowedProperties()
              {
                return true;
              }
            }
        );
      });
      assertTrue(exception.getMessage().contains("The property [invalid_key1] is not in the allowed list [valid_key1, valid_key2]"));
    }

    @Test
    public void testWhenUrlHasDisallowedPropertiesWhenNotEnforcingAllowedProperties()
    {
      new JdbcDataFetcher(
          new MetadataStorageConnectorConfig()
          {
            @Override
            public String getConnectURI()
            {
              return "jdbc:mysql://localhost:3306/db?invalid_key1=val1&valid_key2=val2";
            }
          },
          TABLE_NAME,
          KEY_COLUMN,
          VALUE_COLUMN,
          100,
          new JdbcAccessSecurityConfig()
          {
            @Override
            public Set<String> getAllowedProperties()
            {
              return ImmutableSet.of("valid_key1", "valid_key2");
            }

            @Override
            public boolean isEnforceAllowedProperties()
            {
              return false;
            }
          }
      );
    }
  }

  @Nested
  public class PostgreSqlTest
  {

    @Test
    public void testCreateInstanceWhenUrlHasOnlyAllowedProperties()
    {
      new JdbcDataFetcher(
          new MetadataStorageConnectorConfig()
          {
            @Override
            public String getConnectURI()
            {
              return "jdbc:postgresql://localhost:5432/db?valid_key1=val1&valid_key2=val2";
            }
          },
          TABLE_NAME,
          KEY_COLUMN,
          VALUE_COLUMN,
          100,
          new JdbcAccessSecurityConfig()
          {
            @Override
            public Set<String> getAllowedProperties()
            {
              return ImmutableSet.of("valid_key1", "valid_key2");
            }

            @Override
            public boolean isEnforceAllowedProperties()
            {
              return true;
            }
          }
      );
    }

    @Test
    public void testThrowWhenUrlHasDisallowedPropertiesWhenEnforcingAllowedProperties()
    {
      Throwable exception = assertThrows(IllegalArgumentException.class, () -> {
        new JdbcDataFetcher(
            new MetadataStorageConnectorConfig()
            {
              @Override
              public String getConnectURI()
              {
                return "jdbc:postgresql://localhost:5432/db?invalid_key1=val1&valid_key2=val2";
              }
            },
            TABLE_NAME,
            KEY_COLUMN,
            VALUE_COLUMN,
            100,
            new JdbcAccessSecurityConfig()
            {
              @Override
              public Set<String> getAllowedProperties()
              {
                return ImmutableSet.of("valid_key1", "valid_key2");
              }

              @Override
              public boolean isEnforceAllowedProperties()
              {
                return true;
              }
            }
        );
      });
      assertTrue(exception.getMessage().contains("The property [invalid_key1] is not in the allowed list [valid_key1, valid_key2]"));
    }

    @Test
    public void testWhenUrlHasDisallowedPropertiesWhenNotEnforcingAllowedProperties()
    {
      new JdbcDataFetcher(
          new MetadataStorageConnectorConfig()
          {
            @Override
            public String getConnectURI()
            {
              return "jdbc:postgresql://localhost:5432/db?invalid_key1=val1&valid_key2=val2";
            }
          },
          TABLE_NAME,
          KEY_COLUMN,
          VALUE_COLUMN,
          100,
          new JdbcAccessSecurityConfig()
          {
            @Override
            public Set<String> getAllowedProperties()
            {
              return ImmutableSet.of("valid_key1", "valid_key2");
            }

            @Override
            public boolean isEnforceAllowedProperties()
            {
              return false;
            }
          }
      );
    }

    @Test
    public void testWhenInvalidUrlFormat()
    {
      Throwable exception = assertThrows(IllegalArgumentException.class, () -> {
        new JdbcDataFetcher(
            new MetadataStorageConnectorConfig()
            {
              @Override
              public String getConnectURI()
              {
                return "jdbc:postgresql://invalid-url::3006";
              }
            },
            TABLE_NAME,
            KEY_COLUMN,
            VALUE_COLUMN,
            100,
            new JdbcAccessSecurityConfig()
            {
              @Override
              public Set<String> getAllowedProperties()
              {
                return ImmutableSet.of("valid_key1", "valid_key2");
              }

              @Override
              public boolean isEnforceAllowedProperties()
              {
                return true;
              }
            }
        );
      });
      assertTrue(exception.getMessage().contains("Invalid URL format for PostgreSQL: [jdbc:postgresql://invalid-url::3006]"));
    }
  }

  @Nested
  public class UnknownSchemeTest
  {

    @Test
    public void testThrowWhenUnknownFormatIsNotAllowed()
    {
      Throwable exception = assertThrows(IllegalArgumentException.class, () -> {
        new JdbcDataFetcher(
            new MetadataStorageConnectorConfig()
            {
              @Override
              public String getConnectURI()
              {
                return "jdbc:mydb://localhost:5432/db?valid_key1=val1&valid_key2=val2";
              }
            },
            TABLE_NAME,
            KEY_COLUMN,
            VALUE_COLUMN,
            100,
            new JdbcAccessSecurityConfig()
            {
              @Override
              public Set<String> getAllowedProperties()
              {
                return ImmutableSet.of("valid_key1", "valid_key2");
              }

              @Override
              public boolean isAllowUnknownJdbcUrlFormat()
              {
                return false;
              }

              @Override
              public boolean isEnforceAllowedProperties()
              {
                return true;
              }
            }
        );
      });
      assertTrue(exception.getMessage().contains("Unknown JDBC connection scheme: mydb"));
    }

    @Test
    public void testSkipUrlParsingWhenUnknownFormatIsAllowed()
    {
      new JdbcDataFetcher(
          new MetadataStorageConnectorConfig()
          {
            @Override
            public String getConnectURI()
            {
              return "jdbc:mydb://localhost:5432/db?valid_key1=val1&valid_key2=val2";
            }
          },
          TABLE_NAME,
          KEY_COLUMN,
          VALUE_COLUMN,
          100,
          new JdbcAccessSecurityConfig()
          {
            @Override
            public Set<String> getAllowedProperties()
            {
              return ImmutableSet.of("valid_key1", "valid_key2");
            }

            @Override
            public boolean isAllowUnknownJdbcUrlFormat()
            {
              return true;
            }

            @Override
            public boolean isEnforceAllowedProperties()
            {
              return true;
            }
          }
      );
    }
  }
}
