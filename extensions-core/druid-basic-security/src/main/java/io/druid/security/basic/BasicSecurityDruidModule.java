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
import io.druid.guice.JsonConfigProvider;
import io.druid.guice.ManageLifecycle;
import io.druid.guice.PolyBind;
import io.druid.initialization.DruidModule;
import io.druid.security.basic.authentication.BasicHTTPAuthenticator;
import io.druid.security.basic.authorization.BasicRoleBasedAuthorizer;
import io.druid.security.basic.db.BasicSecurityStorageConnector;
import io.druid.security.basic.db.SQLBasicSecurityStorageConnector;
import io.druid.security.basic.db.derby.DerbySQLBasicSecurityStorageConnector;
import io.druid.security.basic.db.mysql.MySQLBasicSecurityStorageConnector;
import io.druid.security.basic.db.postgres.PostgresBasicSecurityStorageConnector;

import java.util.List;

public class BasicSecurityDruidModule implements DruidModule
{
  public final String STORAGE_CONNECTOR_TYPE_PROPERTY = "druid.metadata.storage.type";

  @Override
  public void configure(Binder binder)
  {
    JsonConfigProvider.bind(binder, "druid.auth.basic", BasicAuthConfig.class);

    PolyBind.createChoiceWithDefault(
        binder, STORAGE_CONNECTOR_TYPE_PROPERTY, Key.get(BasicSecurityStorageConnector.class), null, "derby"
    );
    PolyBind.createChoiceWithDefault(
        binder, STORAGE_CONNECTOR_TYPE_PROPERTY, Key.get(SQLBasicSecurityStorageConnector.class), null, "derby"
    );

    PolyBind.optionBinder(binder, Key.get(BasicSecurityStorageConnector.class))
            .addBinding("derby")
            .to(DerbySQLBasicSecurityStorageConnector.class)
            .in(ManageLifecycle.class);

    PolyBind.optionBinder(binder, Key.get(SQLBasicSecurityStorageConnector.class))
            .addBinding("derby")
            .to(DerbySQLBasicSecurityStorageConnector.class)
            .in(ManageLifecycle.class);

    PolyBind.optionBinder(binder, Key.get(BasicSecurityStorageConnector.class))
            .addBinding("mysql")
            .to(MySQLBasicSecurityStorageConnector.class)
            .in(ManageLifecycle.class);

    PolyBind.optionBinder(binder, Key.get(SQLBasicSecurityStorageConnector.class))
            .addBinding("mysql")
            .to(MySQLBasicSecurityStorageConnector.class)
            .in(ManageLifecycle.class);

    PolyBind.optionBinder(binder, Key.get(BasicSecurityStorageConnector.class))
            .addBinding("postgresql")
            .to(PostgresBasicSecurityStorageConnector.class)
            .in(ManageLifecycle.class);

    PolyBind.optionBinder(binder, Key.get(SQLBasicSecurityStorageConnector.class))
            .addBinding("postgresql")
            .to(PostgresBasicSecurityStorageConnector.class)
            .in(ManageLifecycle.class);

    Jerseys.addResource(binder, BasicSecurityResource.class);
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
