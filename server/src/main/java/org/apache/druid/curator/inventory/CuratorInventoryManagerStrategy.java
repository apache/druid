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

package org.apache.druid.curator.inventory;

/**
 * Concurrency guarantees: all calls to {@link #newContainer}, {@link #deadContainer}, {@link #updateContainer} and
 * {@link #inventoryInitialized} (all done within {@link CuratorInventoryManager#pathChildrenCacheExecutor}) are
 * linearizable. Calls to {@link #addInventory}, {@link #updateInventory} and {@link #removeInventory} are linearizable
 * for the each container in independence.
 */
public interface CuratorInventoryManagerStrategy<ContainerClass, InventoryClass>
{
  ContainerClass deserializeContainer(byte[] bytes);

  InventoryClass deserializeInventory(byte[] bytes);

  void newContainer(ContainerClass newContainer);
  void deadContainer(ContainerClass deadContainer);
  ContainerClass updateContainer(ContainerClass oldContainer, ContainerClass newContainer);
  ContainerClass addInventory(ContainerClass container, String inventoryKey, InventoryClass inventory);
  ContainerClass updateInventory(ContainerClass container, String inventoryKey, InventoryClass inventory);
  ContainerClass removeInventory(ContainerClass container, String inventoryKey);
  void inventoryInitialized();
}
