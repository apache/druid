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

package org.apache.druid.testing.embedded.gcs;

import com.google.cloud.NoCredentials;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.inject.Inject;
import org.apache.druid.storage.google.GoogleStorageDruidModule;

import java.util.Properties;

/**
 * Used to bind {@link Storage} to a test instance.
 */
public class GoogleStorageTestModule extends GoogleStorageDruidModule
{
  private Properties properties;

  @Inject
  public void setProperties(Properties properties)
  {
    this.properties = properties;
  }

  @Override
  public Storage createStorage()
  {
    return createStorageForTests(properties.getProperty("druid.google.storageUrl"));
  }

  /**
   * Creates a {@link Storage} client to be used in embedded tests.
   */
  public static Storage createStorageForTests(String storageUrl)
  {
    return StorageOptions.newBuilder()
                         .setCredentials(NoCredentials.getInstance())
                         .setHost(storageUrl)
                         .setProjectId("embedded-test")
                         .build()
                         .getService();
  }
}
