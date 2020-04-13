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
 */
public interface InventoryManagerConfig
{
  /**
   * The ContainerPath is the path where the InventoryManager should look for new containers of inventory.
   *
   * Because ZK does not allow for children under ephemeral nodes, the common interaction for registering Inventory
   * that might be ephemeral is to
   *
   * 1) Create a permanent node underneath the InventoryPath
   * 2) Create an ephemeral node under the ContainerPath with the same name as the permanent node under InventoryPath
   * 3) For each piece of "inventory", create an ephemeral node as a child of the node created in step (1)
   *
   * @return the containerPath
   */
  String getContainerPath();

  /**
   * The InventoryPath is the path where the InventoryManager should look for new inventory.
   *
   * Because ZK does not allow for children under ephemeral nodes, the common interaction for registering Inventory
   * that might be ephemeral is to
   *
   * 1) Create a permanent node underneath the InventoryPath
   * 2) Create an ephemeral node under the ContainerPath with the same name as the permanent node under InventoryPath
   * 3) For each piece of "inventory", create an ephemeral node as a child of the node created in step (1)
   *
   * @return the inventoryPath
   */
  String getInventoryPath();
}
