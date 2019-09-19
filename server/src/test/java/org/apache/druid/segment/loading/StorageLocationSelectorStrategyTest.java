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

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class StorageLocationSelectorStrategyTest {

  @Rule
  public final TemporaryFolder tmpFolder = new TemporaryFolder();

  @Test
  public void testLeastBytesUsedLocationSelectorStrategy() throws Exception {

    List<StorageLocation> storageLocations = new ArrayList<>();

    final File localStorageFolder1 = tmpFolder.newFolder("local_storage_folder_1");
    final File localStorageFolder2 = tmpFolder.newFolder("local_storage_folder_2");
    final File localStorageFolder3 = tmpFolder.newFolder("local_storage_folder_3");

    StorageLocation storageLocation1 = new StorageLocation(localStorageFolder1, 10000000000L, null);
    storageLocations.add(storageLocation1);
    storageLocations.add(new StorageLocation(localStorageFolder2,10000000000L,null));
    storageLocations.add(new StorageLocation(localStorageFolder3,10000000000L,null));

    StorageLocationSelectorStrategy leastBytesUsedStrategy =
      new LeastBytesUsedStorageLocationSelectorStrategy(storageLocations);

    storageLocation1.reserve("tmp_loc1", "__seg1", 1024L);

    Iterator<StorageLocation> locations = leastBytesUsedStrategy.getLocations();

    StorageLocation loc1 = locations.next();
    Assert.assertTrue("The next element of the iterator should point to path local_storage_folder_2",
      loc1.getPath().equals(localStorageFolder2));

    StorageLocation loc2 = locations.next();
    Assert.assertTrue("The next element of the iterator should point to path local_storage_folder_3",
      loc2.getPath().equals(localStorageFolder3));

    StorageLocation loc3 = locations.next();
    System.out.println(loc3);
    Assert.assertTrue("The next element of the iterator should point to path local_storage_folder_3",
      loc3.getPath().equals(localStorageFolder1));

  }

  @Test
  public void testRoundRobinLocationSelectorStrategy() throws Exception {

    List<StorageLocation> storageLocations = new ArrayList<>();

    final File localStorageFolder1 = tmpFolder.newFolder("local_storage_folder_1");
    final File localStorageFolder2 = tmpFolder.newFolder("local_storage_folder_2");
    final File localStorageFolder3 = tmpFolder.newFolder("local_storage_folder_3");

    storageLocations.add(new StorageLocation(localStorageFolder1,10000000000L, null));
    storageLocations.add(new StorageLocation(localStorageFolder2,10000000000L,null));
    storageLocations.add(new StorageLocation(localStorageFolder3,10000000000L,null));

    StorageLocationSelectorStrategy roundRobinStrategy = new RoundRobinStorageLocationSelectorStrategy(storageLocations);


    Iterator<StorageLocation> locations = roundRobinStrategy.getLocations();
    StorageLocation loc1 = locations.next();
    Assert.assertTrue("The next element of the iterator should point to path local_storage_folder_1",
      loc1.getPath().equals(localStorageFolder1));

    StorageLocation loc2 = locations.next();
    Assert.assertTrue("The next element of the iterator should point to path local_storage_folder_2",
      loc2.getPath().equals(localStorageFolder2));

    StorageLocation loc3 = locations.next();
    Assert.assertTrue("The next element of the iterator should point to path local_storage_folder_3",
      loc3.getPath().equals(localStorageFolder3));

    StorageLocation loc4 = locations.next();
    Assert.assertTrue("The next element of the iterator should point to path local_storage_folder_1",
      loc4.getPath().equals(localStorageFolder1));
  }

}
