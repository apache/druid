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

package io.druid.security.basic;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Key;
import io.druid.guice.Jerseys;
import io.druid.guice.ManageLifecycle;
import io.druid.guice.PolyBind;
import io.druid.initialization.DruidModule;
import io.druid.security.basic.authentication.BasicHTTPAuthenticator;
import io.druid.security.basic.authorization.BasicRoleBasedAuthorizer;
import io.druid.security.basic.db.BasicAuthenticatorStorageConnector;
import io.druid.security.basic.db.BasicAuthorizerStorageConnector;
import io.druid.security.basic.db.SQLBasicAuthenticatorStorageConnector;
import io.druid.security.basic.db.SQLBasicAuthorizerStorageConnector;
import io.druid.security.basic.db.derby.DerbySQLBasicAuthenticatorStorageConnector;
import io.druid.security.basic.db.derby.DerbySQLBasicAuthorizerStorageConnector;
import io.druid.security.basic.db.mysql.MySQLBasicAuthenticatorStorageConnector;
import io.druid.security.basic.db.mysql.MySQLBasicAuthorizerStorageConnector;
import io.druid.security.basic.db.postgres.PostgreSQLBasicAuthenticatorStorageConnector;
import io.druid.security.basic.db.postgres.PostgreSQLBasicAuthorizerStorageConnector;

import java.util.List;

public class BasicSecurityDruidModule implements DruidModule
{
  public final String STORAGE_CONNECTOR_TYPE_PROPERTY = "druid.metadata.storage.type";

  @Override
  public void configure(Binder binder)
  {
    // authentication
    PolyBind.createChoiceWithDefault(
        binder, STORAGE_CONNECTOR_TYPE_PROPERTY, Key.get(BasicAuthenticatorStorageConnector.class), null, "derby"
    );
    PolyBind.createChoiceWithDefault(
        binder, STORAGE_CONNECTOR_TYPE_PROPERTY, Key.get(SQLBasicAuthenticatorStorageConnector.class), null, "derby"
    );

    PolyBind.optionBinder(binder, Key.get(BasicAuthenticatorStorageConnector.class))
            .addBinding("derby")
            .to(DerbySQLBasicAuthenticatorStorageConnector.class)
            .in(ManageLifecycle.class);

    PolyBind.optionBinder(binder, Key.get(SQLBasicAuthenticatorStorageConnector.class))
            .addBinding("derby")
            .to(DerbySQLBasicAuthenticatorStorageConnector.class)
            .in(ManageLifecycle.class);

    PolyBind.optionBinder(binder, Key.get(BasicAuthenticatorStorageConnector.class))
            .addBinding("mysql")
            .to(MySQLBasicAuthenticatorStorageConnector.class)
            .in(ManageLifecycle.class);

    PolyBind.optionBinder(binder, Key.get(SQLBasicAuthenticatorStorageConnector.class))
            .addBinding("mysql")
            .to(MySQLBasicAuthenticatorStorageConnector.class)
            .in(ManageLifecycle.class);

    PolyBind.optionBinder(binder, Key.get(BasicAuthenticatorStorageConnector.class))
            .addBinding("postgresql")
            .to(PostgreSQLBasicAuthenticatorStorageConnector.class)
            .in(ManageLifecycle.class);

    PolyBind.optionBinder(binder, Key.get(SQLBasicAuthenticatorStorageConnector.class))
            .addBinding("postgresql")
            .to(PostgreSQLBasicAuthenticatorStorageConnector.class)
            .in(ManageLifecycle.class);

    Jerseys.addResource(binder, BasicAuthenticatorResource.class);


    // authorization
    PolyBind.createChoiceWithDefault(
        binder, STORAGE_CONNECTOR_TYPE_PROPERTY, Key.get(BasicAuthorizerStorageConnector.class), null, "derby"
    );
    PolyBind.createChoiceWithDefault(
        binder, STORAGE_CONNECTOR_TYPE_PROPERTY, Key.get(SQLBasicAuthorizerStorageConnector.class), null, "derby"
    );

    PolyBind.optionBinder(binder, Key.get(BasicAuthorizerStorageConnector.class))
            .addBinding("derby")
            .to(DerbySQLBasicAuthorizerStorageConnector.class)
            .in(ManageLifecycle.class);

    PolyBind.optionBinder(binder, Key.get(SQLBasicAuthorizerStorageConnector.class))
            .addBinding("derby")
            .to(DerbySQLBasicAuthorizerStorageConnector.class)
            .in(ManageLifecycle.class);

    PolyBind.optionBinder(binder, Key.get(BasicAuthorizerStorageConnector.class))
            .addBinding("mysql")
            .to(MySQLBasicAuthorizerStorageConnector.class)
            .in(ManageLifecycle.class);

    PolyBind.optionBinder(binder, Key.get(SQLBasicAuthorizerStorageConnector.class))
            .addBinding("mysql")
            .to(MySQLBasicAuthorizerStorageConnector.class)
            .in(ManageLifecycle.class);

    PolyBind.optionBinder(binder, Key.get(BasicAuthorizerStorageConnector.class))
            .addBinding("postgresql")
            .to(PostgreSQLBasicAuthorizerStorageConnector.class)
            .in(ManageLifecycle.class);

    PolyBind.optionBinder(binder, Key.get(SQLBasicAuthorizerStorageConnector.class))
            .addBinding("postgresql")
            .to(PostgreSQLBasicAuthorizerStorageConnector.class)
            .in(ManageLifecycle.class);

    Jerseys.addResource(binder, BasicAuthorizerResource.class);
  }

  @Override
  public List<? extends Module> getJacksonModules()
  {
    return ImmutableList.of(
        new SimpleModule("BasicDruidSecurity").registerSubtypes(
            BasicHTTPAuthenticator.class,
            BasicRoleBasedAuthorizer.class
        )
    );
  }
}
