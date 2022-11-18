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

package org.apache.druid.catalog.storage;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.catalog.storage.sql.CatalogManager;
import org.apache.druid.catalog.storage.sql.SQLCatalogManager;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.impl.CsvInputFormat;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.metadata.TestDerbyConnector.DerbyConnectorRule;
import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.Authorizer;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceType;

import java.util.Arrays;

public class CatalogTests
{
  public static final String TEST_AUTHORITY = "test";

  public static final String SUPER_USER = "super";
  public static final String READER_USER = "reader";
  public static final String WRITER_USER = "writer";
  public static final String DENY_USER = "denyAll";

  public static final AuthorizerMapper AUTH_MAPPER = new AuthorizerMapper(
      ImmutableMap.of(TEST_AUTHORITY, new TestAuthorizer()));

  private static class TestAuthorizer implements Authorizer
  {
    @Override
    public Access authorize(
        AuthenticationResult authenticationResult,
        Resource resource,
        Action action
    )
    {
      final String userName = authenticationResult.getIdentity();
      if (SUPER_USER.equals(userName)) {
        return Access.OK;
      }
      if (ResourceType.DATASOURCE.equals(resource.getType())) {
        if ("forbidden".equals(resource.getName())) {
          return Access.DENIED;
        }
        return new Access(
            WRITER_USER.equals(userName) ||
            READER_USER.equals(userName) && action == Action.READ);
      }
      return Access.OK;
    }
  }

  public static InputFormat csvFormat()
  {
    return new CsvInputFormat(
        Arrays.asList("x", "y", "z"),
        null,  // listDelimiter
        false, // hasHeaderRow
        false, // findColumnsFromHeader
        0      // skipHeaderRows
    );
  }

  public static final ObjectMapper JSON_MAPPER = new DefaultObjectMapper();

  public static class DbFixture
  {
    public CatalogManager manager;
    public CatalogStorage storage;

    public DbFixture(DerbyConnectorRule derbyConnectorRule)
    {
      MetadataStorageManager metastoreMgr = new MetadataStorageManager(
          JSON_MAPPER,
          derbyConnectorRule.getConnector(),
          () -> derbyConnectorRule.getMetadataConnectorConfig(),
          derbyConnectorRule.metadataTablesConfigSupplier()
      );
      manager = new SQLCatalogManager(metastoreMgr);
      manager.start();
      storage = new CatalogStorage(manager, JSON_MAPPER);
    }

    public void tearDown()
    {
      if (manager != null) {
        manager = null;
      }
    }
  }

  public static void tearDown(DbFixture fixture)
  {
    if (fixture != null) {
      fixture.tearDown();
    }
  }
}
