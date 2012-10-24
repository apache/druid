package com.metamx.druid.loading;

import com.google.inject.Inject;
import com.metamx.druid.StorageAdapter;

import java.util.Map;

/**
 */
public class SingleStorageAdapterLoader implements StorageAdapterLoader
{
  private final SegmentGetter segmentGetter;
  private final StorageAdapterFactory factory;

  @Inject
  public SingleStorageAdapterLoader(
      SegmentGetter segmentGetter,
      StorageAdapterFactory factory
  )
  {
    this.segmentGetter = segmentGetter;
    this.factory = factory;
  }

  @Override
  public StorageAdapter getAdapter(Map<String, Object> loadSpec) throws StorageAdapterLoadingException
  {
    return factory.factorize(segmentGetter.getSegmentFiles(loadSpec));
  }

  @Override
  public void cleanupAdapter(Map<String, Object> loadSpec) throws StorageAdapterLoadingException
  {
    segmentGetter.cleanSegmentFiles(loadSpec);
  }
}
