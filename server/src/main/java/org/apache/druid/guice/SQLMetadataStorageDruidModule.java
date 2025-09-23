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

package org.apache.druid.guice;

import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Module;
import org.apache.druid.audit.AuditManager;
import org.apache.druid.metadata.MetadataStorageConnector;
import org.apache.druid.metadata.MetadataStorageProvider;
import org.apache.druid.metadata.SQLMetadataConnector;
import org.apache.druid.server.audit.AuditManagerConfig;
import org.apache.druid.server.audit.AuditSerdeHelper;
import org.apache.druid.server.audit.SQLAuditManager;

public class SQLMetadataStorageDruidModule implements Module
{
  public static final String PROPERTY = "druid.metadata.storage.type";
  final String type;

  public SQLMetadataStorageDruidModule(String type)
  {
    this.type = type;
  }

  /**
   * This function only needs to be called by the default SQL metadata storage module
   * Other modules should default to calling super.configure(...) alone
   *
   * @param defaultValue default property value
   */
  public void createBindingChoices(Binder binder, String defaultValue)
  {
    String prop = PROPERTY;
    PolyBind.createChoiceWithDefault(binder, prop, Key.get(MetadataStorageConnector.class), defaultValue);
    PolyBind.createChoiceWithDefault(binder, prop, Key.get(MetadataStorageProvider.class), defaultValue);
    PolyBind.createChoiceWithDefault(binder, prop, Key.get(SQLMetadataConnector.class), defaultValue);

    configureAuditManager(binder);
  }

  @Override
  public void configure(Binder binder)
  {

  }

  private void configureAuditManager(Binder binder)
  {
    JsonConfigProvider.bind(binder, "druid.audit.manager", AuditManagerConfig.class);

    PolyBind.createChoice(
        binder,
        "druid.audit.manager.type",
        Key.get(AuditManager.class),
        Key.get(SQLAuditManager.class)
    );
    PolyBind.optionBinder(binder, Key.get(AuditManager.class))
        .addBinding("sql")
        .to(SQLAuditManager.class)
        .in(LazySingleton.class);

    binder.bind(AuditSerdeHelper.class).in(LazySingleton.class);
  }
}
