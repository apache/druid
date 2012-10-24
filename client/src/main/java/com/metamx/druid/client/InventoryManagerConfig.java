package com.metamx.druid.client;

/**
 */
public class InventoryManagerConfig
{
  private final String inventoryIdPath;
  private final String inventoryPath;

  public InventoryManagerConfig(
      String inventoryIdPath,
      String inventoryPath
  )
  {
    this.inventoryIdPath = inventoryIdPath;
    this.inventoryPath = inventoryPath;
  }

  /**
   * The InventoryIdPath is the path where the InventoryManager should look for new inventory buckets.  Because ZK does
   * not allow for children under ephemeral nodes, the common interaction for registering Inventory that might be
   * ephemeral is to
   *
   * 1) Create a permanent node underneath the InventoryPath
   * 2) Create an ephemeral node underneath the InventoryIdPath with the same name as the permanent nodes under InventoryPath
   * 3) For each piece of "inventory", create an ephemeral node as a child of the node created in step (1)
   *
   * @return the inventoryIdPath
   */
  public String getInventoryIdPath()
  {
    return inventoryIdPath;
  }

  /**
   * The InventoryPath is the path where the InventoryManager should look for new inventory.
   *
   * Because ZK does not allow for children under ephemeral nodes, the common interaction for registering an Inventory
   * container that might be ephemeral (like a server) is to
   *
   * 1) Create a permanent node underneath the InventoryPath
   * 2) Create an ephemeral node underneath the InventoryIdPath with the same name as the permanent nodes under InventoryPath
   * 3) For each piece of "inventory", create an ephemeral node as a child of the node created in step (1)
   *
   * @return the inventoryIdPath
   */
  public String getInventoryPath()
  {
    return inventoryPath;
  }
}
