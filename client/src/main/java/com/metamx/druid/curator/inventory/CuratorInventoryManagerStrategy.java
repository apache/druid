/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
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

package com.metamx.druid.curator.inventory;

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
}
