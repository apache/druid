package com.metamx.druid.loading;

import java.io.File;
import java.util.Map;

/**
 */
public interface SegmentGetter
{
  public File getSegmentFiles(Map<String, Object> loadSpec) throws StorageAdapterLoadingException;
  public boolean cleanSegmentFiles(Map<String, Object> loadSpec) throws StorageAdapterLoadingException;
}
