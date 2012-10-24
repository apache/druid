package com.metamx.druid.loading;

import com.metamx.druid.StorageAdapter;

import java.util.Map;

/**
 */
public interface StorageAdapterLoader
{
  public StorageAdapter getAdapter(Map<String, Object> loadSpec) throws StorageAdapterLoadingException;
  public void cleanupAdapter(Map<String, Object> loadSpec) throws StorageAdapterLoadingException;
}
