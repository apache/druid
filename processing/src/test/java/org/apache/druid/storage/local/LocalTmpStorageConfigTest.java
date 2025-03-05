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

package org.apache.druid.storage.local;

import com.google.inject.Guice;
import com.google.inject.Injector;
import org.junit.Assert;
import org.junit.Test;

import java.util.UUID;

public class LocalTmpStorageConfigTest
{
  @Test
  public void testDefaultLocalTmpStorage()
  {
    String tmpString = UUID.randomUUID().toString();
    Injector injector = Guice.createInjector(
        binder -> binder.bind(LocalTmpStorageConfig.class)
                        .toProvider(new LocalTmpStorageConfig.DefaultLocalTmpStorageConfigProvider(tmpString))
    );
    LocalTmpStorageConfig localTmpStorageConfig = injector.getInstance(LocalTmpStorageConfig.class);
    Assert.assertTrue(localTmpStorageConfig.getTmpDir().getAbsolutePath().contains(tmpString));
  }
}
