package com.metamx.druid.loading;

import com.metamx.druid.StorageAdapter;

import java.io.File;

/**
 */
public interface StorageAdapterFactory
{
  public StorageAdapter factorize(File parentDir) throws StorageAdapterLoadingException;
}
