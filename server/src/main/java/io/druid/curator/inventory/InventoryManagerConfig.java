/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.curator.inventory;

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
  public String getContainerPath();

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
  public String getInventoryPath();
}
