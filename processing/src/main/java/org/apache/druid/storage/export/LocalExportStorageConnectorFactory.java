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

package org.apache.druid.storage.export;

import com.google.inject.Injector;
import org.apache.druid.storage.StorageConnectorProvider;
import org.apache.druid.storage.local.LocalFileStorageConnectorProvider;

import java.io.File;
import java.util.Map;

/**
 * Provides a {@link StorageConnectorProvider} which allows writing to the local machine. Not meaningful for production,
 * and is used for tests or debugging purposes.
 * <br>
 * Not to be bound in Guice modules.
 */
public class LocalExportStorageConnectorFactory implements ExportStorageConnectorFactory
{
  @Override
  public StorageConnectorProvider get(Map<String, String> properties, Injector injector)
  {
    return new LocalFileStorageConnectorProvider(new File(properties.get(LocalFileStorageConnectorProvider.BASE_PATH_FIELD_NAME)));
  }
}
