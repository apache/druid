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

package org.apache.druid.metadata.storage.mysql;

import com.mysql.jdbc.exceptions.MySQLTransientException;
import org.junit.Assert;
import org.junit.Test;

import java.sql.SQLException;
import java.sql.SQLTransientConnectionException;

public class MySQLConnectorTest
{
  @Test
  public void testIsExceptionTransient()
  {
    final String driver = "com.mysql.jdbc.Driver";
    final ClassLoader loader = getClass().getClassLoader();
    Assert.assertTrue(MySQLConnector.isTransientException(loader, driver, new MySQLTransientException()));
    Assert.assertTrue(
        MySQLConnector.isTransientException(loader, driver, new SQLException("some transient failure", "wtf", 1317))
    );
    Assert.assertFalse(
        MySQLConnector.isTransientException(loader, driver, new SQLException("totally realistic test data", "wtf", 1337))
    );
    // this method does not specially handle normal transient exceptions either, since it is not vendor specific
    Assert.assertFalse(
        MySQLConnector.isTransientException(loader, driver, new SQLTransientConnectionException("transient"))
    );
  }
}
