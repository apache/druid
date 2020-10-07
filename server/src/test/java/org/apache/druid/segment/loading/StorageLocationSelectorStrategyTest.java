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

package org.apache.druid.segment.loading;

import com.fasterxml.jackson.module.guice.ObjectMapperModule;
import com.google.inject.Binder;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Provides;
import org.apache.druid.guice.DruidGuiceExtensions;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.JsonConfigurator;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.StorageNodeModule;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.validation.Validation;
import javax.validation.Validator;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

public class StorageLocationSelectorStrategyTest
{

  @Rule
  public final TemporaryFolder tmpFolder = new TemporaryFolder();

  @Test
  public void testLeastBytesUsedLocationSelectorStrategy() throws Exception
  {

    List<StorageLocation> storageLocations = new ArrayList<>();

    final File localStorageFolder1 = tmpFolder.newFolder("local_storage_folder_1");
    final File localStorageFolder2 = tmpFolder.newFolder("local_storage_folder_2");
    final File localStorageFolder3 = tmpFolder.newFolder("local_storage_folder_3");

    StorageLocation storageLocation1 = new StorageLocation(localStorageFolder1, 10000000000L,
        null);
    storageLocations.add(storageLocation1);
    storageLocations.add(new StorageLocation(localStorageFolder2, 10000000000L, null));
    storageLocations.add(new StorageLocation(localStorageFolder3, 10000000000L, null));

    StorageLocationSelectorStrategy leastBytesUsedStrategy =
        new LeastBytesUsedStorageLocationSelectorStrategy(storageLocations);

    storageLocation1.reserve("tmp_loc1", "__seg1", 1024L);

    Iterator<StorageLocation> locations = leastBytesUsedStrategy.getLocations();

    StorageLocation loc1 = locations.next();
    Assert.assertEquals("The next element of the iterator should point to path local_storage_folder_2",
        localStorageFolder2, loc1.getPath());

    StorageLocation loc2 = locations.next();
    Assert.assertEquals("The next element of the iterator should point to path local_storage_folder_3",
        localStorageFolder3, loc2.getPath());

    StorageLocation loc3 = locations.next();
    Assert.assertEquals("The next element of the iterator should point to path local_storage_folder_1",
        localStorageFolder1, loc3.getPath());

  }

  @Test
  public void testRoundRobinLocationSelectorStrategySingleLocation() throws Exception
  {
    List<StorageLocation> storageLocations = new ArrayList<>();
    final File localStorageFolder1 = tmpFolder.newFolder("local_storage_folder_1");
    storageLocations.add(new StorageLocation(localStorageFolder1, 10000000000L, null));
    StorageLocationSelectorStrategy roundRobinStrategy =
        new RoundRobinStorageLocationSelectorStrategy(storageLocations);

    Iterator<StorageLocation> locations = roundRobinStrategy.getLocations();

    StorageLocation loc1 = locations.next();
    Assert.assertEquals("The next element of the iterator should point to path local_storage_folder_1",
        localStorageFolder1, loc1.getPath());

    locations = roundRobinStrategy.getLocations();

    StorageLocation loc2 = locations.next();
    Assert.assertEquals("The next element of the iterator should point to path local_storage_folder_1",
        localStorageFolder1, loc2.getPath());
  }


  @Test
  public void testRoundRobinLocationSelectorStrategy() throws Exception
  {
    List<StorageLocation> storageLocations = new ArrayList<>();

    final File localStorageFolder1 = tmpFolder.newFolder("local_storage_folder_1");
    final File localStorageFolder2 = tmpFolder.newFolder("local_storage_folder_2");
    final File localStorageFolder3 = tmpFolder.newFolder("local_storage_folder_3");

    storageLocations.add(new StorageLocation(localStorageFolder1, 10000000000L, null));
    storageLocations.add(new StorageLocation(localStorageFolder2, 10000000000L, null));
    storageLocations.add(new StorageLocation(localStorageFolder3, 10000000000L, null));

    StorageLocationSelectorStrategy roundRobinStrategy = new RoundRobinStorageLocationSelectorStrategy(storageLocations);

    // First call to getLocations()
    Iterator<StorageLocation> locations = roundRobinStrategy.getLocations();

    StorageLocation loc1 = locations.next();
    Assert.assertEquals("The next element of the iterator should point to path local_storage_folder_1",
        localStorageFolder1, loc1.getPath());

    StorageLocation loc2 = locations.next();
    Assert.assertEquals("The next element of the iterator should point to path local_storage_folder_2",
        localStorageFolder2, loc2.getPath());

    StorageLocation loc3 = locations.next();
    Assert.assertEquals("The next element of the iterator should point to path local_storage_folder_3",
        localStorageFolder3, loc3.getPath());


    // Second call to getLocations()
    locations = roundRobinStrategy.getLocations();

    loc2 = locations.next();
    Assert.assertEquals("The next element of the iterator should point to path local_storage_folder_2",
        localStorageFolder2, loc2.getPath());

    loc3 = locations.next();
    Assert.assertEquals("The next element of the iterator should point to path local_storage_folder_3",
        localStorageFolder3, loc3.getPath());

    loc1 = locations.next();
    Assert.assertEquals("The next element of the iterator should point to path local_storage_folder_1",
        localStorageFolder1, loc1.getPath());
  }

  @Test
  public void testRoundRobinLocationSelectorStrategyMultipleCallsToGetLocations() throws Exception
  {
    List<StorageLocation> storageLocations = new ArrayList<>();

    final File localStorageFolder1 = tmpFolder.newFolder("local_storage_folder_1");
    final File localStorageFolder2 = tmpFolder.newFolder("local_storage_folder_2");
    final File localStorageFolder3 = tmpFolder.newFolder("local_storage_folder_3");

    storageLocations.add(new StorageLocation(localStorageFolder1, 10000000000L, null));
    storageLocations.add(new StorageLocation(localStorageFolder2, 10000000000L, null));
    storageLocations.add(new StorageLocation(localStorageFolder3, 10000000000L, null));

    StorageLocationSelectorStrategy roundRobinStrategy = new RoundRobinStorageLocationSelectorStrategy(storageLocations);

    Iterator<StorageLocation> locations = roundRobinStrategy.getLocations();

    StorageLocation loc1 = locations.next();
    Assert.assertEquals("The next element of the iterator should point to path local_storage_folder_1",
        localStorageFolder1, loc1.getPath());

    locations = roundRobinStrategy.getLocations();

    StorageLocation loc2 = locations.next();
    Assert.assertEquals("The next element of the iterator should point to path local_storage_folder_2",
        localStorageFolder2, loc2.getPath());

    locations = roundRobinStrategy.getLocations();

    StorageLocation loc3 = locations.next();
    Assert.assertEquals("The next element of the iterator should point to path local_storage_folder_3",
        localStorageFolder3, loc3.getPath());

    loc1 = locations.next();
    Assert.assertEquals("The next element of the iterator should point to path local_storage_folder_1",
        localStorageFolder1, loc1.getPath());
  }

  @Test
  public void testRandomLocationSelectorStrategy() throws Exception
  {
    List<StorageLocation> storageLocations = new ArrayList<>();

    final File localStorageFolder1 = tmpFolder.newFolder("local_storage_folder_1");
    final File localStorageFolder2 = tmpFolder.newFolder("local_storage_folder_2");
    final File localStorageFolder3 = tmpFolder.newFolder("local_storage_folder_3");

    storageLocations.add(new StorageLocation(localStorageFolder1, 3000L, null));
    storageLocations.add(new StorageLocation(localStorageFolder2, 2000L, null));
    storageLocations.add(new StorageLocation(localStorageFolder3, 1000L, null));

    StorageLocationSelectorStrategy leastBytesUsedStrategy =
            new RandomStorageLocationSelectorStrategy(storageLocations);


    Iterator<StorageLocation> locations = leastBytesUsedStrategy.getLocations();

    StorageLocation loc1 = locations.next();
    StorageLocation loc2 = locations.next();
    StorageLocation loc3 = locations.next();

    File[] result = new File[]{loc1.getPath(), loc2.getPath(), loc3.getPath()};
    Arrays.sort(result);
    Assert.assertArrayEquals(new File[]{localStorageFolder1, localStorageFolder2, localStorageFolder3}, result);
  }

  @Test
  public void testMostAvailableSizeLocationSelectorStrategy() throws Exception
  {
    List<StorageLocation> storageLocations = new ArrayList<>();

    final File localStorageFolder1 = tmpFolder.newFolder("local_storage_folder_1");
    final File localStorageFolder2 = tmpFolder.newFolder("local_storage_folder_2");
    final File localStorageFolder3 = tmpFolder.newFolder("local_storage_folder_3");

    StorageLocation storageLocation1 = new StorageLocation(localStorageFolder1, 10000000000L, null);
    storageLocations.add(storageLocation1);
    StorageLocation storageLocation2 = new StorageLocation(localStorageFolder2, 20000000000L, null);
    storageLocations.add(storageLocation2);
    StorageLocation storageLocation3 = new StorageLocation(localStorageFolder3, 15000000000L, null);
    storageLocations.add(storageLocation3);

    StorageLocationSelectorStrategy mostAvailableStrategy = new MostAvailableSizeStorageLocationSelectorStrategy(storageLocations);

    Iterator<StorageLocation> locations = mostAvailableStrategy.getLocations();

    StorageLocation loc1 = locations.next();
    Assert.assertEquals("The next element of the iterator should point to path local_storage_folder_2",
        localStorageFolder2, loc1.getPath());

    StorageLocation loc2 = locations.next();
    Assert.assertEquals("The next element of the iterator should point to path local_storage_folder_3",
        localStorageFolder3, loc2.getPath());

    StorageLocation loc3 = locations.next();
    Assert.assertEquals("The next element of the iterator should point to path local_storage_folder_1",
        localStorageFolder1, loc3.getPath());

    storageLocation2.reserve("tmp_loc2", "__seg2", 6000000000L);
    locations = mostAvailableStrategy.getLocations();

    loc1 = locations.next();
    Assert.assertEquals("The next element of the iterator should point to path local_storage_folder_3",
        localStorageFolder3, loc1.getPath());

    loc2 = locations.next();
    Assert.assertEquals("The next element of the iterator should point to path local_storage_folder_2",
        localStorageFolder2, loc2.getPath());

    loc3 = locations.next();
    Assert.assertEquals("The next element of the iterator should point to path local_storage_folder_1",
        localStorageFolder1, loc3.getPath());
  }

  @Test
  public void testDefaultSelectorStrategyConfig()
  {
    //no druid.segmentCache.locationSelector.strategy specified, the default will be used
    final Properties props = new Properties();
    props.setProperty("druid.segmentCache.locations", "[{\"path\": \"/tmp/druid/indexCache\"}]");

    StorageLocationSelectorStrategy strategy = makeInjectorWithProperties(props).getInstance(StorageLocationSelectorStrategy.class);
    Assert.assertEquals(LeastBytesUsedStorageLocationSelectorStrategy.class,
                        strategy.getClass());
    Assert.assertEquals("/tmp/druid/indexCache", strategy.getLocations().next().getPath().getAbsolutePath());
  }

  @Test
  public void testRoundRobinSelectorStrategyConfig()
  {
    final Properties props = new Properties();
    props.setProperty("druid.segmentCache.locations", "[{\"path\": \"/tmp/druid/indexCache\"}]");
    props.setProperty("druid.segmentCache.locationSelector.strategy", "roundRobin");

    Injector injector = makeInjectorWithProperties(props);
    StorageLocationSelectorStrategy strategy = injector.getInstance(StorageLocationSelectorStrategy.class);

    Assert.assertEquals(RoundRobinStorageLocationSelectorStrategy.class,
                        strategy.getClass());
    Assert.assertEquals("/tmp/druid/indexCache", strategy.getLocations().next().getPath().getAbsolutePath());
  }

  @Test
  public void testLeastBytesUsedSelectorStrategyConfig()
  {
    final Properties props = new Properties();
    props.setProperty("druid.segmentCache.locations", "[{\"path\": \"/tmp/druid/indexCache\"}]");
    props.setProperty("druid.segmentCache.locationSelector.strategy", "leastBytesUsed");

    Injector injector = makeInjectorWithProperties(props);
    StorageLocationSelectorStrategy strategy = injector.getInstance(StorageLocationSelectorStrategy.class);

    Assert.assertEquals(LeastBytesUsedStorageLocationSelectorStrategy.class,
                        strategy.getClass());
    Assert.assertEquals("/tmp/druid/indexCache", strategy.getLocations().next().getPath().getAbsolutePath());
  }

  @Test
  public void testRandomSelectorStrategyConfig()
  {
    final Properties props = new Properties();
    props.setProperty("druid.segmentCache.locations", "[{\"path\": \"/tmp/druid/indexCache\"}]");
    props.setProperty("druid.segmentCache.locationSelector.strategy", "random");

    Injector injector = makeInjectorWithProperties(props);
    StorageLocationSelectorStrategy strategy = injector.getInstance(StorageLocationSelectorStrategy.class);

    Assert.assertEquals(RandomStorageLocationSelectorStrategy.class,
                        strategy.getClass());
    Assert.assertEquals("/tmp/druid/indexCache", strategy.getLocations().next().getPath().getAbsolutePath());
  }

  @Test
  public void testMostAvailableSizeSelectorStrategyConfig()
  {
    final Properties props = new Properties();
    props.setProperty("druid.segmentCache.locationSelector.strategy", "mostAvailableSize");
    props.setProperty("druid.segmentCache.locations", "[{\"path\": \"/tmp/druid/indexCache\"}]");

    Injector injector = makeInjectorWithProperties(props);
    StorageLocationSelectorStrategy strategy = injector.getInstance(StorageLocationSelectorStrategy.class);

    Assert.assertEquals(MostAvailableSizeStorageLocationSelectorStrategy.class,
                        strategy.getClass());
    Assert.assertEquals("/tmp/druid/indexCache", strategy.getLocations().next().getPath().getAbsolutePath());
  }

  private Injector makeInjectorWithProperties(final Properties props)
  {
    return Guice.createInjector(
        new Module()
          {
            @Override
            public void configure(Binder binder)
            {
              //ObjectMapperModule introduce Guice injector for jackson
              binder.install(new ObjectMapperModule()
                                 .withObjectMapper(new DefaultObjectMapper()));
              binder.install(new DruidGuiceExtensions());

              binder.bind(Validator.class).toInstance(Validation.buildDefaultValidatorFactory().getValidator());
              binder.bind(JsonConfigurator.class).in(LazySingleton.class);
              binder.bind(Properties.class).toInstance(props);

              JsonConfigProvider.bind(binder, "druid.segmentCache", SegmentLoaderConfig.class);
              StorageNodeModule.bindLocationSelectorStrategy(binder);
            }

            @Provides
            public List provideStorageLocation(SegmentLoaderConfig segmentLoader)
            {
              return segmentLoader.toStorageLocations();
            }
          }
      );
  }
}
