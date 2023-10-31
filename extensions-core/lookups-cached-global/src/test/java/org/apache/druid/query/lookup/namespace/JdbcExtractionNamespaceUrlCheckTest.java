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

package org.apache.druid.query.lookup.namespace;

import com.google.common.collect.ImmutableSet;
import org.apache.druid.metadata.MetadataStorageConnectorConfig;
import org.apache.druid.server.initialization.JdbcAccessSecurityConfig;
import org.joda.time.Period;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.Set;

@RunWith(Enclosed.class)
public class JdbcExtractionNamespaceUrlCheckTest
{
  private static final String TABLE_NAME = "abstractDbRenameTest";
  private static final String KEY_NAME = "keyName";
  private static final String VAL_NAME = "valName";
  private static final String TS_COLUMN = "tsColumn";

  public static class MySqlTest
  {
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testCreateInstanceWhenUrlHasOnlyAllowedProperties()
    {
      new JdbcExtractionNamespace(
          new MetadataStorageConnectorConfig()
          {
            @Override
            public String getConnectURI()
            {
              return "jdbc:mysql://localhost:3306/db?valid_key1=val1&valid_key2=val2";
            }
          },
          TABLE_NAME,
          KEY_NAME,
          VAL_NAME,
          TS_COLUMN,
          "some filter",
          new Period(10),
          null,
          0,
          1000L,
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
    public void testThrowWhenUrlHasNonAllowedPropertiesWhenEnforcingAllowedProperties()
    {
      expectedException.expect(IllegalArgumentException.class);
      expectedException.expectMessage("The property [invalid_key1] is not in the allowed list [valid_key1, valid_key2]");
      new JdbcExtractionNamespace(
          new MetadataStorageConnectorConfig()
          {
            @Override
            public String getConnectURI()
            {
              return "jdbc:mysql://localhost:3306/db?invalid_key1=val1&valid_key2=val2";
            }
          },
          TABLE_NAME,
          KEY_NAME,
          VAL_NAME,
          TS_COLUMN,
          "some filter",
          new Period(10),
          null,
          0,
          null,
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
    public void testWhenUrlHasNonAllowedPropertiesWhenNotEnforcingAllowedProperties()
    {
      new JdbcExtractionNamespace(
          new MetadataStorageConnectorConfig()
          {
            @Override
            public String getConnectURI()
            {
              return "jdbc:mysql://localhost:3306/db?invalid_key1=val1&valid_key2=val2";
            }
          },
          TABLE_NAME,
          KEY_NAME,
          VAL_NAME,
          TS_COLUMN,
          "some filter",
          new Period(10),
          null,
          0,
          null,
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
      expectedException.expect(IllegalArgumentException.class);
      expectedException.expectMessage("Invalid URL format for MySQL: [jdbc:mysql:/invalid-url::3006]");
      new JdbcExtractionNamespace(
          new MetadataStorageConnectorConfig()
          {
            @Override
            public String getConnectURI()
            {
              return "jdbc:mysql:/invalid-url::3006";
            }
          },
          TABLE_NAME,
          KEY_NAME,
          VAL_NAME,
          TS_COLUMN,
          "some filter",
          new Period(10),
          null,
          0,
          null,
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
  }

  public static class PostgreSqlTest
  {
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testCreateInstanceWhenUrlHasOnlyAllowedProperties()
    {
      new JdbcExtractionNamespace(
          new MetadataStorageConnectorConfig()
          {
            @Override
            public String getConnectURI()
            {
              return "jdbc:postgresql://localhost:5432/db?valid_key1=val1&valid_key2=val2";
            }
          },
          TABLE_NAME,
          KEY_NAME,
          VAL_NAME,
          TS_COLUMN,
          "some filter",
          new Period(10),
          null,
          0,
          null,
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
    public void testThrowWhenUrlHasNonAllowedPropertiesWhenEnforcingAllowedProperties()
    {
      expectedException.expect(IllegalArgumentException.class);
      expectedException.expectMessage("The property [invalid_key1] is not in the allowed list [valid_key1, valid_key2]");
      new JdbcExtractionNamespace(
          new MetadataStorageConnectorConfig()
          {
            @Override
            public String getConnectURI()
            {
              return "jdbc:postgresql://localhost:5432/db?invalid_key1=val1&valid_key2=val2";
            }
          },
          TABLE_NAME,
          KEY_NAME,
          VAL_NAME,
          TS_COLUMN,
          "some filter",
          new Period(10),
          10L,
          0,
          null,
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
    public void testWhenUrlHasNonAllowedPropertiesWhenNotEnforcingAllowedProperties()
    {
      new JdbcExtractionNamespace(
          new MetadataStorageConnectorConfig()
          {
            @Override
            public String getConnectURI()
            {
              return "jdbc:postgresql://localhost:5432/db?invalid_key1=val1&valid_key2=val2";
            }
          },
          TABLE_NAME,
          KEY_NAME,
          VAL_NAME,
          TS_COLUMN,
          "some filter",
          new Period(10),
          null,
          0,
          null,
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
      expectedException.expect(IllegalArgumentException.class);
      expectedException.expectMessage("Invalid URL format for PostgreSQL: [jdbc:postgresql://invalid-url::3006]");
      new JdbcExtractionNamespace(
          new MetadataStorageConnectorConfig()
          {
            @Override
            public String getConnectURI()
            {
              return "jdbc:postgresql://invalid-url::3006";
            }
          },
          TABLE_NAME,
          KEY_NAME,
          VAL_NAME,
          TS_COLUMN,
          "some filter",
          new Period(10),
          null,
          0,
          null,
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
  }

  public static class UnknownSchemeTest
  {
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testThrowWhenUnknownFormatIsNotAllowed()
    {
      expectedException.expect(IllegalArgumentException.class);
      expectedException.expectMessage("Unknown JDBC connection scheme: mydb");
      new JdbcExtractionNamespace(
          new MetadataStorageConnectorConfig()
          {
            @Override
            public String getConnectURI()
            {
              return "jdbc:mydb://localhost:5432/db?valid_key1=val1&valid_key2=val2";
            }
          },
          TABLE_NAME,
          KEY_NAME,
          VAL_NAME,
          TS_COLUMN,
          "some filter",
          new Period(10),
          null,
          0,
          null,
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
    }

    @Test
    public void testSkipUrlParsingWhenUnknownFormatIsAllowed()
    {
      new JdbcExtractionNamespace(
          new MetadataStorageConnectorConfig()
          {
            @Override
            public String getConnectURI()
            {
              return "jdbc:mydb://localhost:5432/db?valid_key1=val1&valid_key2=val2";
            }
          },
          TABLE_NAME,
          KEY_NAME,
          VAL_NAME,
          TS_COLUMN,
          "some filter",
          new Period(10),
          null,
          0,
          null,
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
