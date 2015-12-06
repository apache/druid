/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.curator.inventory;

/**
 */
public interface CuratorInventoryManagerStrategy<ContainerClass, InventoryClass>
{
  public ContainerClass deserializeContainer(byte[] bytes);
  public byte[] serializeContainer(ContainerClass container);

  public InventoryClass deserializeInventory(byte[] bytes);
  public byte[] serializeInventory(InventoryClass inventory);

  public void newContainer(ContainerClass newContainer);
  public void deadContainer(ContainerClass deadContainer);
  public ContainerClass updateContainer(ContainerClass oldContainer, ContainerClass newContainer);
  public ContainerClass addInventory(ContainerClass container, String inventoryKey, InventoryClass inventory);
  public ContainerClass updateInventory(ContainerClass container, String inventoryKey, InventoryClass inventory);
  public ContainerClass removeInventory(ContainerClass container, String inventoryKey);
  public void inventoryInitialized();
}
