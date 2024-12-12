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

package org.apache.druid.storage.google;

import com.google.cloud.storage.Storage;
import com.google.common.collect.ImmutableList;
import com.google.inject.Injector;
import org.apache.druid.guice.GuiceInjectors;
import org.apache.druid.segment.loading.OmniDataSegmentKiller;
import org.junit.Assert;
import org.junit.Test;

public class GoogleStorageDruidModuleTest
{
  @Test
  public void testSegmentKillerBoundedSingleton()
  {
    // This test is primarily validating 2 things
    // 1. That the Google credentials are loaded lazily, they are loaded as part of instantiation of the
    //    HttpRquestInitializer, the test throws an exception from that method, meaning that if they are not loaded
    //    lazily, the exception should end up thrown.
    // 2. That the same object is returned.
    Injector injector = GuiceInjectors.makeStartupInjectorWithModules(ImmutableList.of(new GoogleStorageDruidModule()));
    OmniDataSegmentKiller killer = injector.getInstance(OmniDataSegmentKiller.class);
    Assert.assertTrue(killer.getKillers().containsKey(GoogleStorageDruidModule.SCHEME));
    Assert.assertSame(
        killer.getKillers().get(GoogleStorageDruidModule.SCHEME).get(),
        killer.getKillers().get(GoogleStorageDruidModule.SCHEME).get()
    );

    final Storage storage = injector.getInstance(Storage.class);
    Assert.assertSame(storage, injector.getInstance(Storage.class));
  }

  @Test
  public void testLazyInstantiation()
  {
    // This test is primarily validating 2 things
    // 1. That the Google credentials are loaded lazily, they are loaded as part of instantiation of the
    //    HttpRquestInitializer, the test throws an exception from that method, meaning that if they are not loaded
    //    lazily, the exception should end up thrown.
    // 2. That the same object is returned.
    Injector injector = GuiceInjectors.makeStartupInjectorWithModules(ImmutableList.of(new GoogleStorageDruidModule()));
    final GoogleStorage instance = injector.getInstance(GoogleStorage.class);
    Assert.assertSame(instance, injector.getInstance(GoogleStorage.class));
  }
}
