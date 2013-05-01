package com.metamx.druid.client;

/**
 */
public interface InventoryView
{
  public DruidServer getInventoryValue(String string);
  public Iterable<DruidServer> getInventory();
}
