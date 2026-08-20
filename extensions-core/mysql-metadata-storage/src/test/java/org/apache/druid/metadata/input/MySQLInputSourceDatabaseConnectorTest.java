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

package org.apache.druid.metadata.input;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.metadata.MetadataStorageConnectorConfig;
import org.apache.druid.metadata.storage.mysql.MySQLConnectorDriverConfig;
import org.apache.druid.metadata.storage.mysql.MySQLMetadataStorageModule;
import org.apache.druid.server.initialization.JdbcAccessSecurityConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.lenient;

@ExtendWith(MockitoExtension.class)
public class MySQLInputSourceDatabaseConnectorTest
{
  private static final JdbcAccessSecurityConfig INJECTED_CONF = newSecurityConfigEnforcingAllowList(ImmutableSet.of());

  @Mock
  private MySQLConnectorDriverConfig mySQLConnectorDriverConfig;

  @BeforeEach
  public void setup()
  {
    lenient().doReturn("com.mysql.jdbc.Driver").when(mySQLConnectorDriverConfig).getDriverClassName();
  }

  @Test
  public void testSerde() throws JsonProcessingException
  {
    ObjectMapper mapper = new DefaultObjectMapper();
    mapper.registerModules(new MySQLMetadataStorageModule().getJacksonModules());
    mapper.setInjectableValues(new InjectableValues.Std().addValue(JdbcAccessSecurityConfig.class, INJECTED_CONF)
                                                         .addValue(
                                                             MySQLConnectorDriverConfig.class,
                                                             mySQLConnectorDriverConfig
                                                         ));
    MetadataStorageConnectorConfig connectorConfig = new MetadataStorageConnectorConfig()
    {
      @Override
      public String getConnectURI()
      {
        return "jdbc:mysql://localhost:3306/test";
      }
    };
    MySQLInputSourceDatabaseConnector connector = new MySQLInputSourceDatabaseConnector(
        connectorConfig,
        null,
        INJECTED_CONF,
        mySQLConnectorDriverConfig
    );
    MySQLInputSourceDatabaseConnector andBack = mapper.readValue(
        mapper.writeValueAsString(connector),
        MySQLInputSourceDatabaseConnector.class
    );
    Assertions.assertEquals(connector, andBack);

    // test again with classname
    connector = new MySQLInputSourceDatabaseConnector(
        connectorConfig,
        "some.class.name.Driver",
        INJECTED_CONF,
        mySQLConnectorDriverConfig
    );
    andBack = mapper.readValue(mapper.writeValueAsString(connector), MySQLInputSourceDatabaseConnector.class);
    Assertions.assertEquals(connector, andBack);
  }

  @Test
  public void testEqualsAndHashcode()
  {
    EqualsVerifier.forClass(MySQLInputSourceDatabaseConnector.class)
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
        return "jdbc:mysql://localhost:3306/test";
      }
    };

    JdbcAccessSecurityConfig securityConfig = newSecurityConfigEnforcingAllowList(ImmutableSet.of());

    new MySQLInputSourceDatabaseConnector(
        connectorConfig,
        null,
        securityConfig,
        mySQLConnectorDriverConfig
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
        return "jdbc:mysql://localhost:3306/test";
      }
    };

    JdbcAccessSecurityConfig securityConfig = newSecurityConfigEnforcingAllowList(ImmutableSet.of("user"));

    new MySQLInputSourceDatabaseConnector(
        connectorConfig,
        null,
        securityConfig,
        mySQLConnectorDriverConfig
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
        return "jdbc:mysql://localhost:3306/test?user=maytas&password=secret&keyonly";
      }
    };

    JdbcAccessSecurityConfig securityConfig = newSecurityConfigEnforcingAllowList(ImmutableSet.of(""));

    Throwable exception = assertThrows(
        IllegalArgumentException.class,
        () -> new MySQLInputSourceDatabaseConnector(
            connectorConfig,
            null,
            securityConfig,
            mySQLConnectorDriverConfig
        )
    );
    assertTrue(
        exception.getMessage().contains("The property [password] is not in the allowed list")
            || exception.getMessage().contains("The property [user] is not in the allowed list")
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
        return "jdbc:mysql://localhost:3306/test?user=maytas&password=secret&keyonly";
      }
    };

    JdbcAccessSecurityConfig securityConfig = newSecurityConfigEnforcingAllowList(
        ImmutableSet.of("user", "password", "keyonly", "etc")
    );

    new MySQLInputSourceDatabaseConnector(
        connectorConfig,
        null,
        securityConfig,
        mySQLConnectorDriverConfig
    );
  }

  @Test
  public void testSuccessOnlyValidPropertyMariaDb()
  {
    MetadataStorageConnectorConfig connectorConfig = new MetadataStorageConnectorConfig()
    {
      @Override
      public String getConnectURI()
      {
        return "jdbc:mariadb://localhost:3306/test?user=maytas&password=secret&keyonly";
      }
    };

    JdbcAccessSecurityConfig securityConfig = newSecurityConfigEnforcingAllowList(
        ImmutableSet.of("user", "password", "keyonly", "etc")
    );

    new MySQLInputSourceDatabaseConnector(
        connectorConfig,
        null,
        securityConfig,
        mySQLConnectorDriverConfig
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
        return "jdbc:mysql://localhost:3306/test?user=maytas&password=secret&keyonly";
      }
    };

    JdbcAccessSecurityConfig securityConfig = newSecurityConfigEnforcingAllowList(ImmutableSet.of("none", "nonenone"));

    Throwable exception = assertThrows(
        IllegalArgumentException.class,
        () -> new MySQLInputSourceDatabaseConnector(
            connectorConfig,
            null,
            securityConfig,
            mySQLConnectorDriverConfig
        )
    );
    assertTrue(
        exception.getMessage().contains("The property [password] is not in the allowed list")
            || exception.getMessage().contains("The property [user] is not in the allowed list")
    );
  }

  @Test
  public void testFailValidAndInvalidProperty()
  {
    Throwable exception = assertThrows(IllegalArgumentException.class, () -> {
      MetadataStorageConnectorConfig connectorConfig = new MetadataStorageConnectorConfig()
      {
        @Override
        public String getConnectURI()
        {
          return "jdbc:mysql://localhost:3306/test?user=maytas&password=secret&keyonly";
        }
      };

      JdbcAccessSecurityConfig securityConfig = newSecurityConfigEnforcingAllowList(ImmutableSet.of("user", "nonenone"));

      new MySQLInputSourceDatabaseConnector(
          connectorConfig,
          null,
          securityConfig,
          mySQLConnectorDriverConfig
      );
    });
    assertTrue(exception.getMessage().contains("The property [password] is not in the allowed list"));
  }

  @Test
  public void testFailValidAndInvalidPropertyMariadb()
  {
    MetadataStorageConnectorConfig connectorConfig = new MetadataStorageConnectorConfig()
    {
      @Override
      public String getConnectURI()
      {
        return "jdbc:mariadb://localhost:3306/test?user=maytas&password=secret&keyonly";
      }
    };

    JdbcAccessSecurityConfig securityConfig = newSecurityConfigEnforcingAllowList(ImmutableSet.of("user", "nonenone"));

    Throwable exception = assertThrows(
        IllegalArgumentException.class,
        () -> new MySQLInputSourceDatabaseConnector(
            connectorConfig,
            null,
            securityConfig,
            mySQLConnectorDriverConfig
        )
    );
    assertTrue(
        exception.getMessage().contains("The property [password] is not in the allowed list")
            || exception.getMessage().contains("The property [keyonly] is not in the allowed list")
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
        return "jdbc:mysql://localhost:3306/test?user=maytas&password=secret&keyonly";
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

    new MySQLInputSourceDatabaseConnector(
        connectorConfig,
        null,
        securityConfig,
        mySQLConnectorDriverConfig
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
