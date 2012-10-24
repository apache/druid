package com.metamx.druid.loading;

import com.metamx.druid.StorageAdapter;
import com.metamx.druid.index.v1.IndexIO;
import com.metamx.druid.index.v1.MMappedIndexStorageAdapter;

import java.io.File;
import java.io.IOException;

/**
 */
public class MMappedStorageAdapterFactory extends ConvertingBaseQueryableFactory
{
  @Override
  protected StorageAdapter factorizeConverted(File parentDir) throws IOException
  {
    return new MMappedIndexStorageAdapter(IndexIO.mapDir(parentDir));
  }
}
