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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.validation.Validation;
import javax.validation.Validator;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

public class StorageLocationSelectorStrategyTest
{

  @TempDir
  public File tmpFolder;

  @Test
  public void testLeastBytesUsedLocationSelectorStrategy() throws Exception
  {

    List<StorageLocation> storageLocations = new ArrayList<>();

    final File localStorageFolder1 = newFolder(tmpFolder, "local_storage_folder_1");
    final File localStorageFolder2 = newFolder(tmpFolder, "local_storage_folder_2");
    final File localStorageFolder3 = newFolder(tmpFolder, "local_storage_folder_3");

    StorageLocation storageLocation1 = new StorageLocation(localStorageFolder1, 10000000000L,
        null);
    storageLocations.add(storageLocation1);
    storageLocations.add(new StorageLocation(localStorageFolder2, 10000000000L, null));
    storageLocations.add(new StorageLocation(localStorageFolder3, 10000000000L, null));

    StorageLocationSelectorStrategy leastBytesUsedStrategy =
        new LeastBytesUsedStorageLocationSelectorStrategy(storageLocations);

    storageLocation1.reserve(makeCacheEntry("tmp_loc1", 1024L));

    Iterator<StorageLocation> locations = leastBytesUsedStrategy.getLocations();

    StorageLocation loc1 = locations.next();
    Assertions.assertEquals(localStorageFolder2, loc1.getPath(), "The next element of the iterator should point to path local_storage_folder_2");

    StorageLocation loc2 = locations.next();
    Assertions.assertEquals(localStorageFolder3, loc2.getPath(), "The next element of the iterator should point to path local_storage_folder_3");

    StorageLocation loc3 = locations.next();
    Assertions.assertEquals(localStorageFolder1, loc3.getPath(), "The next element of the iterator should point to path local_storage_folder_1");

  }

  @Test
  public void testRoundRobinLocationSelectorStrategySingleLocation() throws Exception
  {
    List<StorageLocation> storageLocations = new ArrayList<>();
    final File localStorageFolder1 = newFolder(tmpFolder, "local_storage_folder_1");
    storageLocations.add(new StorageLocation(localStorageFolder1, 10000000000L, null));
    StorageLocationSelectorStrategy roundRobinStrategy =
        new RoundRobinStorageLocationSelectorStrategy(storageLocations);

    Iterator<StorageLocation> locations = roundRobinStrategy.getLocations();

    StorageLocation loc1 = locations.next();
    Assertions.assertEquals(localStorageFolder1, loc1.getPath(), "The next element of the iterator should point to path local_storage_folder_1");

    locations = roundRobinStrategy.getLocations();

    StorageLocation loc2 = locations.next();
    Assertions.assertEquals(localStorageFolder1, loc2.getPath(), "The next element of the iterator should point to path local_storage_folder_1");
  }


  @Test
  public void testRoundRobinLocationSelectorStrategy() throws Exception
  {
    List<StorageLocation> storageLocations = new ArrayList<>();

    final File localStorageFolder1 = newFolder(tmpFolder, "local_storage_folder_1");
    final File localStorageFolder2 = newFolder(tmpFolder, "local_storage_folder_2");
    final File localStorageFolder3 = newFolder(tmpFolder, "local_storage_folder_3");

    storageLocations.add(new StorageLocation(localStorageFolder1, 10000000000L, null));
    storageLocations.add(new StorageLocation(localStorageFolder2, 10000000000L, null));
    storageLocations.add(new StorageLocation(localStorageFolder3, 10000000000L, null));

    StorageLocationSelectorStrategy roundRobinStrategy = new RoundRobinStorageLocationSelectorStrategy(storageLocations);

    // First call to getLocations()
    Iterator<StorageLocation> locations = roundRobinStrategy.getLocations();

    StorageLocation loc1 = locations.next();
    Assertions.assertEquals(localStorageFolder1, loc1.getPath(), "The next element of the iterator should point to path local_storage_folder_1");

    StorageLocation loc2 = locations.next();
    Assertions.assertEquals(localStorageFolder2, loc2.getPath(), "The next element of the iterator should point to path local_storage_folder_2");

    StorageLocation loc3 = locations.next();
    Assertions.assertEquals(localStorageFolder3, loc3.getPath(), "The next element of the iterator should point to path local_storage_folder_3");


    // Second call to getLocations()
    locations = roundRobinStrategy.getLocations();

    loc2 = locations.next();
    Assertions.assertEquals(localStorageFolder2, loc2.getPath(), "The next element of the iterator should point to path local_storage_folder_2");

    loc3 = locations.next();
    Assertions.assertEquals(localStorageFolder3, loc3.getPath(), "The next element of the iterator should point to path local_storage_folder_3");

    loc1 = locations.next();
    Assertions.assertEquals(localStorageFolder1, loc1.getPath(), "The next element of the iterator should point to path local_storage_folder_1");
  }

  @Test
  public void testRoundRobinLocationSelectorStrategyMultipleCallsToGetLocations() throws Exception
  {
    List<StorageLocation> storageLocations = new ArrayList<>();

    final File localStorageFolder1 = newFolder(tmpFolder, "local_storage_folder_1");
    final File localStorageFolder2 = newFolder(tmpFolder, "local_storage_folder_2");
    final File localStorageFolder3 = newFolder(tmpFolder, "local_storage_folder_3");

    storageLocations.add(new StorageLocation(localStorageFolder1, 10000000000L, null));
    storageLocations.add(new StorageLocation(localStorageFolder2, 10000000000L, null));
    storageLocations.add(new StorageLocation(localStorageFolder3, 10000000000L, null));

    StorageLocationSelectorStrategy roundRobinStrategy = new RoundRobinStorageLocationSelectorStrategy(storageLocations);

    Iterator<StorageLocation> locations = roundRobinStrategy.getLocations();

    StorageLocation loc1 = locations.next();
    Assertions.assertEquals(localStorageFolder1, loc1.getPath(), "The next element of the iterator should point to path local_storage_folder_1");

    locations = roundRobinStrategy.getLocations();

    StorageLocation loc2 = locations.next();
    Assertions.assertEquals(localStorageFolder2, loc2.getPath(), "The next element of the iterator should point to path local_storage_folder_2");

    locations = roundRobinStrategy.getLocations();

    StorageLocation loc3 = locations.next();
    Assertions.assertEquals(localStorageFolder3, loc3.getPath(), "The next element of the iterator should point to path local_storage_folder_3");

    loc1 = locations.next();
    Assertions.assertEquals(localStorageFolder1, loc1.getPath(), "The next element of the iterator should point to path local_storage_folder_1");
  }

  @Test
  public void testRandomLocationSelectorStrategy() throws Exception
  {
    List<StorageLocation> storageLocations = new ArrayList<>();

    final File localStorageFolder1 = newFolder(tmpFolder, "local_storage_folder_1");
    final File localStorageFolder2 = newFolder(tmpFolder, "local_storage_folder_2");
    final File localStorageFolder3 = newFolder(tmpFolder, "local_storage_folder_3");

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
    Assertions.assertArrayEquals(new File[]{localStorageFolder1, localStorageFolder2, localStorageFolder3}, result);
  }

  @Test
  public void testMostAvailableSizeLocationSelectorStrategy() throws Exception
  {
    List<StorageLocation> storageLocations = new ArrayList<>();

    final File localStorageFolder1 = newFolder(tmpFolder, "local_storage_folder_1");
    final File localStorageFolder2 = newFolder(tmpFolder, "local_storage_folder_2");
    final File localStorageFolder3 = newFolder(tmpFolder, "local_storage_folder_3");

    StorageLocation storageLocation1 = new StorageLocation(localStorageFolder1, 10000000000L, null);
    storageLocations.add(storageLocation1);
    StorageLocation storageLocation2 = new StorageLocation(localStorageFolder2, 20000000000L, null);
    storageLocations.add(storageLocation2);
    StorageLocation storageLocation3 = new StorageLocation(localStorageFolder3, 15000000000L, null);
    storageLocations.add(storageLocation3);

    StorageLocationSelectorStrategy mostAvailableStrategy = new MostAvailableSizeStorageLocationSelectorStrategy(storageLocations);

    Iterator<StorageLocation> locations = mostAvailableStrategy.getLocations();

    StorageLocation loc1 = locations.next();
    Assertions.assertEquals(localStorageFolder2, loc1.getPath(), "The next element of the iterator should point to path local_storage_folder_2");

    StorageLocation loc2 = locations.next();
    Assertions.assertEquals(localStorageFolder3, loc2.getPath(), "The next element of the iterator should point to path local_storage_folder_3");

    StorageLocation loc3 = locations.next();
    Assertions.assertEquals(localStorageFolder1, loc3.getPath(), "The next element of the iterator should point to path local_storage_folder_1");

    storageLocation2.reserve(makeCacheEntry("tmp_loc2", 6000000000L));
    locations = mostAvailableStrategy.getLocations();

    loc1 = locations.next();
    Assertions.assertEquals(localStorageFolder3, loc1.getPath(), "The next element of the iterator should point to path local_storage_folder_3");

    loc2 = locations.next();
    Assertions.assertEquals(localStorageFolder2, loc2.getPath(), "The next element of the iterator should point to path local_storage_folder_2");

    loc3 = locations.next();
    Assertions.assertEquals(localStorageFolder1, loc3.getPath(), "The next element of the iterator should point to path local_storage_folder_1");
  }

  @Test
  public void testDefaultSelectorStrategyConfig()
  {
    //no druid.segmentCache.locationSelector.strategy specified, the default will be used
    final Properties props = new Properties();
    props.setProperty("druid.segmentCache.locations", "[{\"path\": \"/tmp/druid/indexCache\"}]");

    StorageLocationSelectorStrategy strategy = makeInjectorWithProperties(props).getInstance(StorageLocationSelectorStrategy.class);
    Assertions.assertEquals(LeastBytesUsedStorageLocationSelectorStrategy.class,
                        strategy.getClass());
    Assertions.assertEquals("/tmp/druid/indexCache", strategy.getLocations().next().getPath().getAbsolutePath());
  }

  @Test
  public void testRoundRobinSelectorStrategyConfig()
  {
    final Properties props = new Properties();
    props.setProperty("druid.segmentCache.locations", "[{\"path\": \"/tmp/druid/indexCache\"}]");
    props.setProperty("druid.segmentCache.locationSelector.strategy", "roundRobin");

    Injector injector = makeInjectorWithProperties(props);
    StorageLocationSelectorStrategy strategy = injector.getInstance(StorageLocationSelectorStrategy.class);

    Assertions.assertEquals(RoundRobinStorageLocationSelectorStrategy.class,
                        strategy.getClass());
    Assertions.assertEquals("/tmp/druid/indexCache", strategy.getLocations().next().getPath().getAbsolutePath());
  }

  @Test
  public void testLeastBytesUsedSelectorStrategyConfig()
  {
    final Properties props = new Properties();
    props.setProperty("druid.segmentCache.locations", "[{\"path\": \"/tmp/druid/indexCache\"}]");
    props.setProperty("druid.segmentCache.locationSelector.strategy", "leastBytesUsed");

    Injector injector = makeInjectorWithProperties(props);
    StorageLocationSelectorStrategy strategy = injector.getInstance(StorageLocationSelectorStrategy.class);

    Assertions.assertEquals(LeastBytesUsedStorageLocationSelectorStrategy.class,
                        strategy.getClass());
    Assertions.assertEquals("/tmp/druid/indexCache", strategy.getLocations().next().getPath().getAbsolutePath());
  }

  @Test
  public void testRandomSelectorStrategyConfig()
  {
    final Properties props = new Properties();
    props.setProperty("druid.segmentCache.locations", "[{\"path\": \"/tmp/druid/indexCache\"}]");
    props.setProperty("druid.segmentCache.locationSelector.strategy", "random");

    Injector injector = makeInjectorWithProperties(props);
    StorageLocationSelectorStrategy strategy = injector.getInstance(StorageLocationSelectorStrategy.class);

    Assertions.assertEquals(RandomStorageLocationSelectorStrategy.class,
                        strategy.getClass());
    Assertions.assertEquals("/tmp/druid/indexCache", strategy.getLocations().next().getPath().getAbsolutePath());
  }

  @Test
  public void testMostAvailableSizeSelectorStrategyConfig()
  {
    final Properties props = new Properties();
    props.setProperty("druid.segmentCache.locationSelector.strategy", "mostAvailableSize");
    props.setProperty("druid.segmentCache.locations", "[{\"path\": \"/tmp/druid/indexCache\"}]");

    Injector injector = makeInjectorWithProperties(props);
    StorageLocationSelectorStrategy strategy = injector.getInstance(StorageLocationSelectorStrategy.class);

    Assertions.assertEquals(MostAvailableSizeStorageLocationSelectorStrategy.class,
                        strategy.getClass());
    Assertions.assertEquals("/tmp/druid/indexCache", strategy.getLocations().next().getPath().getAbsolutePath());
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

  private static CacheEntry makeCacheEntry(String label, long size)
  {
    final StorageLocationTest.StringCacheIdentifier identifier = new StorageLocationTest.StringCacheIdentifier(label);
    return new CacheEntry()
    {
      private boolean isMounted = false;

      @Override
      public CacheEntryIdentifier getId()
      {
        return identifier;
      }

      @Override
      public long getSize()
      {
        return size;
      }

      @Override
      public boolean isMounted()
      {
        return isMounted;
      }

      @Override
      public void mount(StorageLocation location)
      {
        isMounted = true;
      }

      @Override
      public void unmount()
      {
        isMounted = false;
      }
    };
  }

  private static File newFolder(File root, String... subDirs) throws IOException
  {
    if (subDirs.length == 0 || (subDirs.length == 1 && "junit".equals(subDirs[0]))) {
      return java.nio.file.Files.createTempDirectory(root.toPath(), "junit").toFile();
    }
    String subFolder = String.join("/", subDirs);
    File result = new File(root, subFolder);
    if (!result.mkdirs()) {
      throw new IOException("Couldn't create folders " + root);
    }
    return result;
  }

}
