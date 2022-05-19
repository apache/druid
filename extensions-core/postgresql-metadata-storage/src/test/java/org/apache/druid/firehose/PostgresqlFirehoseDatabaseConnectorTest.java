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

package org.apache.druid.firehose;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.metadata.MetadataStorageConnectorConfig;
import org.apache.druid.metadata.storage.postgresql.PostgreSQLMetadataStorageModule;
import org.apache.druid.server.initialization.JdbcAccessSecurityConfig;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Set;

public class PostgresqlFirehoseDatabaseConnectorTest
{
  private static final ObjectMapper MAPPER = new DefaultObjectMapper();
  private static final JdbcAccessSecurityConfig INJECTED_CONF = newSecurityConfigEnforcingAllowList(ImmutableSet.of());

  static {
    MAPPER.registerModules(new PostgreSQLMetadataStorageModule().getJacksonModules());
    MAPPER.setInjectableValues(new InjectableValues.Std().addValue(JdbcAccessSecurityConfig.class, INJECTED_CONF));
  }

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();


  @Test
  public void testSerde() throws JsonProcessingException
  {
    MetadataStorageConnectorConfig connectorConfig = new MetadataStorageConnectorConfig()
    {
      @Override
      public String getConnectURI()
      {
        return "jdbc:postgresql://localhost:3306/test";
      }
    };
    PostgresqlFirehoseDatabaseConnector connector = new PostgresqlFirehoseDatabaseConnector(
        connectorConfig,
        INJECTED_CONF
    );
    PostgresqlFirehoseDatabaseConnector andBack = MAPPER.readValue(
        MAPPER.writeValueAsString(connector),
        PostgresqlFirehoseDatabaseConnector.class
    );
    Assert.assertEquals(connector, andBack);
  }

  @Test
  public void testEqualsAndHashcode()
  {
    EqualsVerifier.forClass(PostgresqlFirehoseDatabaseConnector.class)
                  .usingGetClass()
                  .withNonnullFields("connectorConfig")
                  .withIgnoredFields("dbi")
                  .verify();
  }

  @Test
  public void testSuccessWhenNoPropertyInUriAndNoAllowlist()
  {
    MetadataStorageConnectorConfig connectorConfig = new MetadataStorageConnectorConfig()
    {
      @Override
      public String getConnectURI()
      {
        return "jdbc:postgresql://localhost:3306/test";
      }
    };

    JdbcAccessSecurityConfig securityConfig = newSecurityConfigEnforcingAllowList(ImmutableSet.of());

    new PostgresqlFirehoseDatabaseConnector(
        connectorConfig,
        securityConfig
    );
  }

  @Test
  public void testSuccessWhenAllowlistAndNoProperty()
  {
    MetadataStorageConnectorConfig connectorConfig = new MetadataStorageConnectorConfig()
    {
      @Override
      public String getConnectURI()
      {
        return "jdbc:postgresql://localhost:3306/test";
      }
    };

    JdbcAccessSecurityConfig securityConfig = newSecurityConfigEnforcingAllowList(ImmutableSet.of("user"));

    new PostgresqlFirehoseDatabaseConnector(
        connectorConfig,
        securityConfig
    );
  }

  @Test
  public void testFailWhenNoAllowlistAndHaveProperty()
  {
    MetadataStorageConnectorConfig connectorConfig = new MetadataStorageConnectorConfig()
    {
      @Override
      public String getConnectURI()
      {
        return "jdbc:postgresql://localhost:3306/test?user=maytas&password=secret&keyonly";
      }
    };

    JdbcAccessSecurityConfig securityConfig = newSecurityConfigEnforcingAllowList(ImmutableSet.of(""));

    expectedException.expectMessage("is not in the allowed list");
    expectedException.expect(IllegalArgumentException.class);

    new PostgresqlFirehoseDatabaseConnector(
        connectorConfig,
        securityConfig
    );
  }

  @Test
  public void testSuccessOnlyValidProperty()
  {
    MetadataStorageConnectorConfig connectorConfig = new MetadataStorageConnectorConfig()
    {
      @Override
      public String getConnectURI()
      {
        return "jdbc:postgresql://localhost:3306/test?user=maytas&password=secret&keyonly";
      }
    };

    JdbcAccessSecurityConfig securityConfig = newSecurityConfigEnforcingAllowList(
        ImmutableSet.of("user", "password", "keyonly", "etc")
    );

    new PostgresqlFirehoseDatabaseConnector(
        connectorConfig,
        securityConfig
    );
  }

  @Test
  public void testFailOnlyInvalidProperty()
  {
    MetadataStorageConnectorConfig connectorConfig = new MetadataStorageConnectorConfig()
    {
      @Override
      public String getConnectURI()
      {
        return "jdbc:postgresql://localhost:3306/test?user=maytas&password=secret&keyonly";
      }
    };

    JdbcAccessSecurityConfig securityConfig = newSecurityConfigEnforcingAllowList(ImmutableSet.of("none", "nonenone"));

    expectedException.expectMessage("is not in the allowed list");
    expectedException.expect(IllegalArgumentException.class);

    new PostgresqlFirehoseDatabaseConnector(
        connectorConfig,
        securityConfig
    );
  }

  @Test
  public void testFailValidAndInvalidProperty()
  {
    MetadataStorageConnectorConfig connectorConfig = new MetadataStorageConnectorConfig()
    {
      @Override
      public String getConnectURI()
      {
        return "jdbc:postgresql://localhost:3306/test?user=maytas&password=secret&keyonly";
      }
    };

    JdbcAccessSecurityConfig securityConfig = newSecurityConfigEnforcingAllowList(ImmutableSet.of("user", "nonenone"));

    expectedException.expectMessage("is not in the allowed list");
    expectedException.expect(IllegalArgumentException.class);

    new PostgresqlFirehoseDatabaseConnector(
        connectorConfig,
        securityConfig
    );
  }

  @Test
  public void testIgnoreInvalidPropertyWhenNotEnforcingAllowList()
  {
    MetadataStorageConnectorConfig connectorConfig = new MetadataStorageConnectorConfig()
    {
      @Override
      public String getConnectURI()
      {
        return "jdbc:postgresql://localhost:3306/test?user=maytas&password=secret&keyonly";
      }
    };

    JdbcAccessSecurityConfig securityConfig = new JdbcAccessSecurityConfig()
    {
      @Override
      public Set<String> getAllowedProperties()
      {
        return ImmutableSet.of("user", "nonenone");
      }

      @Override
      public boolean isEnforceAllowedProperties()
      {
        return false;
      }
    };

    new PostgresqlFirehoseDatabaseConnector(
        connectorConfig,
        securityConfig
    );
  }

  private static JdbcAccessSecurityConfig newSecurityConfigEnforcingAllowList(Set<String> allowedProperties)
  {
    return new JdbcAccessSecurityConfig()
    {
      @Override
      public Set<String> getAllowedProperties()
      {
        return allowedProperties;
      }

      @Override
      public boolean isEnforceAllowedProperties()
      {
        return true;
      }
    };
  }
}
