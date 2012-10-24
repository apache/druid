package com.metamx.druid.client;

import com.metamx.common.Pair;
import com.metamx.phonebook.PhoneBookPeon;

import java.util.Map;

/**
 */
public interface InventoryManagementStrategy<T>
{
  public Class<T> getContainerClass();
  public Pair<String, PhoneBookPeon<?>> makeSubListener(final T baseObject);
  public void objectRemoved(final T baseObject);

  // These are a hack to get around a poor serialization choice, please do not use
  public boolean doesSerde();
  public T deserialize(String name, Map<String, String> properties);
}
