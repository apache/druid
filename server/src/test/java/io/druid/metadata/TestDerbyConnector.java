/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.metadata;

import com.google.common.base.Supplier;
import io.druid.metadata.storage.derby.DerbyConnector;
import org.junit.Assert;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.exceptions.UnableToObtainConnectionException;

import java.sql.SQLException;

public class TestDerbyConnector extends DerbyConnector
{
  public TestDerbyConnector(
      Supplier<MetadataStorageConnectorConfig> config,
      Supplier<MetadataStorageTablesConfig> dbTables
  )
  {
    super(config, dbTables, new DBI("jdbc:derby:memory:druidTest;create=true"));
  }

  public void tearDown()
  {
    try {
      new DBI("jdbc:derby:memory:druidTest;drop=true").open().close();
    } catch(UnableToObtainConnectionException e) {
      SQLException cause = (SQLException) e.getCause();
      // error code "08006" indicates proper shutdown
      Assert.assertEquals("08006", cause.getSQLState());
    }
  }
}
