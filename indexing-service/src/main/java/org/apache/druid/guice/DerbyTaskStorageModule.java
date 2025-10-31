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
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.guice.annotations.LoadScope;
import org.apache.druid.metadata.DerbyMetadataStorageActionHandlerFactory;
import org.apache.druid.metadata.MetadataStorageActionHandlerFactory;
import org.apache.druid.metadata.storage.derby.DerbyMetadataStorageDruidModule;

/**
 * Module to bind Derby implementation of {@link MetadataStorageActionHandlerFactory}
 * used by {@code MetadataTaskStorage}.
 */
@LoadScope(roles = NodeRole.OVERLORD_JSON_NAME)
public class DerbyTaskStorageModule implements Module
{
  @Override
  public void configure(Binder binder)
  {
    PolyBind.createChoiceWithDefault(
        binder,
        SQLMetadataStorageDruidModule.PROPERTY,
        Key.get(MetadataStorageActionHandlerFactory.class),
        DerbyMetadataStorageDruidModule.TYPE
    );

    PolyBind.optionBinder(binder, Key.get(MetadataStorageActionHandlerFactory.class))
            .addBinding(DerbyMetadataStorageDruidModule.TYPE)
            .to(DerbyMetadataStorageActionHandlerFactory.class)
            .in(LazySingleton.class);
  }
}
