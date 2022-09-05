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

package org.apache.druid.firehose.sql;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.metadata.MetadataStorageConnectorConfig;
import org.apache.druid.metadata.storage.mysql.MySQLConnectorDriverConfig;
import org.apache.druid.metadata.storage.mysql.MySQLMetadataStorageModule;
import org.apache.druid.server.initialization.JdbcAccessSecurityConfig;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Set;

@RunWith(MockitoJUnitRunner.class)
public class MySQLFirehoseDatabaseConnectorTest
{
  private static final JdbcAccessSecurityConfig INJECTED_CONF = newSecurityConfigEnforcingAllowList(ImmutableSet.of());

  @Mock
  private MySQLConnectorDriverConfig mySQLConnectorDriverConfig;

  @Before
  public void setup()
  {
    Mockito.doReturn("com.mysql.jdbc.Driver").when(mySQLConnectorDriverConfig).getDriverClassName();
  }

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

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
    MySQLFirehoseDatabaseConnector connector = new MySQLFirehoseDatabaseConnector(
        connectorConfig,
        null,
        INJECTED_CONF,
        mySQLConnectorDriverConfig
    );
    MySQLFirehoseDatabaseConnector andBack = mapper.readValue(
        mapper.writeValueAsString(connector),
        MySQLFirehoseDatabaseConnector.class
    );
    Assert.assertEquals(connector, andBack);

    // test again with classname
    connector = new MySQLFirehoseDatabaseConnector(
        connectorConfig,
        "some.class.name.Driver",
        INJECTED_CONF,
        mySQLConnectorDriverConfig
    );
    andBack = mapper.readValue(mapper.writeValueAsString(connector), MySQLFirehoseDatabaseConnector.class);
    Assert.assertEquals(connector, andBack);
  }

  @Test
  public void testEqualsAndHashcode()
  {
    EqualsVerifier.forClass(MySQLFirehoseDatabaseConnector.class)
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

    new MySQLFirehoseDatabaseConnector(
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

    new MySQLFirehoseDatabaseConnector(
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

    expectedException.expectMessage("The property [password] is not in the allowed list");
    expectedException.expect(IllegalArgumentException.class);

    new MySQLFirehoseDatabaseConnector(
        connectorConfig,
        null,
        securityConfig,
        mySQLConnectorDriverConfig
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

    new MySQLFirehoseDatabaseConnector(
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

    new MySQLFirehoseDatabaseConnector(
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

    expectedException.expectMessage("The property [password] is not in the allowed list");
    expectedException.expect(IllegalArgumentException.class);

    new MySQLFirehoseDatabaseConnector(
        connectorConfig,
        null,
        securityConfig,
        mySQLConnectorDriverConfig
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
        return "jdbc:mysql://localhost:3306/test?user=maytas&password=secret&keyonly";
      }
    };

    JdbcAccessSecurityConfig securityConfig = newSecurityConfigEnforcingAllowList(ImmutableSet.of("user", "nonenone"));

    expectedException.expectMessage("The property [password] is not in the allowed list");
    expectedException.expect(IllegalArgumentException.class);

    new MySQLFirehoseDatabaseConnector(
        connectorConfig,
        null,
        securityConfig,
        mySQLConnectorDriverConfig
    );
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

    expectedException.expectMessage("The property [password] is not in the allowed list");
    expectedException.expect(IllegalArgumentException.class);

    new MySQLFirehoseDatabaseConnector(
        connectorConfig,
        null,
        securityConfig,
        mySQLConnectorDriverConfig
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

    new MySQLFirehoseDatabaseConnector(
        connectorConfig,
        null,
        securityConfig,
        mySQLConnectorDriverConfig
    );
  }

  @Test
  public void testFindPropertyKeysFromInvalidConnectUrl()
  {
    final String url = "jdbc:mysql:/invalid-url::3006";
    MetadataStorageConnectorConfig connectorConfig = new MetadataStorageConnectorConfig()
    {
      @Override
      public String getConnectURI()
      {
        return url;
      }
    };

    expectedException.expect(RuntimeException.class);
    expectedException.expectMessage(StringUtils.format("Invalid URL format for MySQL: [%s]", url));
    new MySQLFirehoseDatabaseConnector(
        connectorConfig,
        null,
        new JdbcAccessSecurityConfig(),
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
