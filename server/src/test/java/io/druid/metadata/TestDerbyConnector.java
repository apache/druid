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

package io.druid.metadata;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import io.druid.java.util.common.StringUtils;
import io.druid.metadata.storage.derby.DerbyConnector;
import org.junit.Assert;
import org.junit.rules.ExternalResource;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.exceptions.UnableToObtainConnectionException;

import java.sql.SQLException;
import java.util.UUID;

public class TestDerbyConnector extends DerbyConnector
{
  private final String jdbcUri;

  public TestDerbyConnector(
      Supplier<MetadataStorageConnectorConfig> config,
      Supplier<MetadataStorageTablesConfig> dbTables
  )
  {
    this(config, dbTables, "jdbc:derby:memory:druidTest" + dbSafeUUID());
  }

  protected TestDerbyConnector(
      Supplier<MetadataStorageConnectorConfig> config,
      Supplier<MetadataStorageTablesConfig> dbTables,
      String jdbcUri
  )
  {
    super(new NoopMetadataStorageProvider().get(), config, dbTables, new DBI(jdbcUri + ";create=true"));
    this.jdbcUri = jdbcUri;
  }

  public void tearDown()
  {
    try {
      new DBI(jdbcUri + ";drop=true").open().close();
    }
    catch (UnableToObtainConnectionException e) {
      SQLException cause = (SQLException) e.getCause();
      // error code "08006" indicates proper shutdown
      Assert.assertEquals(StringUtils.format("Derby not shutdown: [%s]", cause.toString()), "08006", cause.getSQLState());
    }
  }

  public static String dbSafeUUID()
  {
    return UUID.randomUUID().toString().replace("-", "");
  }

  public String getJdbcUri()
  {
    return jdbcUri;
  }

  public static class DerbyConnectorRule extends ExternalResource
  {
    private TestDerbyConnector connector;
    private final Supplier<MetadataStorageTablesConfig> dbTables;
    private final MetadataStorageConnectorConfig connectorConfig;

    public DerbyConnectorRule()
    {
      this("druidTest" + dbSafeUUID());
    }

    private DerbyConnectorRule(
        final String defaultBase
    )
    {
      this(Suppliers.ofInstance(MetadataStorageTablesConfig.fromBase(defaultBase)));
    }

    public DerbyConnectorRule(
        Supplier<MetadataStorageTablesConfig> dbTables
    )
    {
      this.dbTables = dbTables;
      this.connectorConfig = new MetadataStorageConnectorConfig()
      {
        @Override
        public String getConnectURI()
        {
          return connector.getJdbcUri();
        }
      };
    }

    @Override
    protected void before() throws Throwable
    {
      connector = new TestDerbyConnector(Suppliers.ofInstance(connectorConfig), dbTables);
      connector.getDBI().open().close(); // create db
    }

    @Override
    protected void after()
    {
      connector.tearDown();
    }

    public TestDerbyConnector getConnector()
    {
      return connector;
    }

    public MetadataStorageConnectorConfig getMetadataConnectorConfig()
    {
      return connectorConfig;
    }

    public Supplier<MetadataStorageTablesConfig> metadataTablesConfigSupplier()
    {
      return dbTables;
    }
  }
}
