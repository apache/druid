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

package org.apache.druid.metadata.storage.sqlserver;

import com.google.common.base.Suppliers;
import org.apache.druid.metadata.MetadataStorageConnectorConfig;
import org.apache.druid.metadata.MetadataStorageTablesConfig;
import org.apache.druid.segment.metadata.CentralizedDatasourceSchemaConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;

@SuppressWarnings("nls")
public class SQLServerConnectorTest
{

  @Test
  public void testIsTransientException()
  {
    SQLServerConnector connector = new SQLServerConnector(
        Suppliers.ofInstance(new MetadataStorageConnectorConfig()),
        Suppliers.ofInstance(
            MetadataStorageTablesConfig.fromBase(null)
        ),
        CentralizedDatasourceSchemaConfig.create()
    );

    Assertions.assertTrue(connector.isTransientException(new SQLException("Resource Failure!", "08DIE")));
    Assertions.assertTrue(connector.isTransientException(new SQLException("Resource Failure as well!", "53RES")));
    Assertions.assertTrue(connector.isTransientException(new SQLException("Transient Failures", "JW001")));
    Assertions.assertTrue(connector.isTransientException(new SQLException("Transient Rollback", "40001")));

    Assertions.assertFalse(connector.isTransientException(new SQLException("SQLException with reason only")));
    Assertions.assertFalse(connector.isTransientException(new SQLException()));
    Assertions.assertFalse(connector.isTransientException(new Exception("Exception with reason only")));
    Assertions.assertFalse(connector.isTransientException(new Throwable("Throwable with reason only")));
  }

  @Test
  public void testIsUniqueConstraintViolation()
  {
    SQLServerConnector connector = new SQLServerConnector(
        Suppliers.ofInstance(new MetadataStorageConnectorConfig()),
        Suppliers.ofInstance(
            MetadataStorageTablesConfig.fromBase(null)
        ),
        CentralizedDatasourceSchemaConfig.create()
    );

    // SQL Server integrity_constraint_violation SQL state (23000)
    Assertions.assertTrue(connector.isUniqueConstraintViolation(
        new SQLException("Violation of UNIQUE KEY constraint", "23000")
    ));

    // Different SQL state should return false
    Assertions.assertFalse(connector.isUniqueConstraintViolation(
        new SQLException("some other error", "42000")
    ));

    // SQLException wrapped in another exception (tests cause chain traversal)
    Assertions.assertTrue(connector.isUniqueConstraintViolation(
        new RuntimeException(new SQLException("Duplicate key", "23000"))
    ));

    // Non-SQLException exception
    Assertions.assertFalse(connector.isUniqueConstraintViolation(new Exception("not a SQLException")));
  }

  @Test
  public void testLimitClause()
  {
    SQLServerConnector connector = new SQLServerConnector(
        Suppliers.ofInstance(new MetadataStorageConnectorConfig()),
        Suppliers.ofInstance(
            MetadataStorageTablesConfig.fromBase(null)
        ),
        CentralizedDatasourceSchemaConfig.create()
    );
    Assertions.assertEquals("FETCH NEXT 100 ROWS ONLY", connector.limitClause(100));
  }
}
