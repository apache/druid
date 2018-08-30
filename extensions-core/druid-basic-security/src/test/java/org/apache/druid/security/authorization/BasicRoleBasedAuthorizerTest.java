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

package org.apache.druid.security.authorization;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.metadata.MetadataStorageTablesConfig;
import org.apache.druid.metadata.TestDerbyConnector;
import org.apache.druid.security.basic.BasicAuthCommonCacheConfig;
import org.apache.druid.security.basic.authorization.BasicRoleBasedAuthorizer;
import org.apache.druid.security.basic.authorization.db.cache.MetadataStoragePollingBasicAuthorizerCacheManager;
import org.apache.druid.security.basic.authorization.db.updater.CoordinatorBasicAuthorizerMetadataStorageUpdater;
import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.server.security.ResourceType;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

public class BasicRoleBasedAuthorizerTest
{
  private static final String AUTHORIZER_NAME = "test";

  @Rule
  public final TestDerbyConnector.DerbyConnectorRule derbyConnectorRule = new TestDerbyConnector.DerbyConnectorRule();

  private BasicRoleBasedAuthorizer authorizer;
  private TestDerbyConnector connector;
  private MetadataStorageTablesConfig tablesConfig;
  private CoordinatorBasicAuthorizerMetadataStorageUpdater updater;

  @Before
  public void setUp()
  {
    connector = derbyConnectorRule.getConnector();
    tablesConfig = derbyConnectorRule.metadataTablesConfigSupplier().get();
    connector.createConfigTable();

    updater = new CoordinatorBasicAuthorizerMetadataStorageUpdater(
        new AuthorizerMapper(
            ImmutableMap.of(
                AUTHORIZER_NAME,
                new BasicRoleBasedAuthorizer(
                    null,
                    AUTHORIZER_NAME,
                    null,
                    null
                )
            )
        ),
        connector,
        tablesConfig,
        new BasicAuthCommonCacheConfig(null, null, null, null),
        new ObjectMapper(new SmileFactory()),
        new NoopBasicAuthorizerCacheNotifier(),
        null
    );

    updater.start();

    authorizer = new BasicRoleBasedAuthorizer(
        new MetadataStoragePollingBasicAuthorizerCacheManager(
            updater
        ),
        AUTHORIZER_NAME,
        null,
        null
    );
  }

  @After
  public void tearDown()
  {
  }

  @Test
  public void testAuth()
  {
    updater.createUser(AUTHORIZER_NAME, "druid");
    updater.createRole(AUTHORIZER_NAME, "druidRole");
    updater.assignRole(AUTHORIZER_NAME, "druid", "druidRole");

    List<ResourceAction> permissions = Collections.singletonList(
        new ResourceAction(new Resource("testResource", ResourceType.DATASOURCE), Action.WRITE)
    );

    updater.setPermissions(AUTHORIZER_NAME, "druidRole", permissions);

    AuthenticationResult authenticationResult = new AuthenticationResult("druid", "druid", null, null);

    Access access = authorizer.authorize(
        authenticationResult,
        new Resource("testResource", ResourceType.DATASOURCE),
        Action.WRITE
    );
    Assert.assertTrue(access.isAllowed());

    access = authorizer.authorize(
        authenticationResult,
        new Resource("wrongResource", ResourceType.DATASOURCE),
        Action.WRITE
    );
    Assert.assertFalse(access.isAllowed());
  }
}
