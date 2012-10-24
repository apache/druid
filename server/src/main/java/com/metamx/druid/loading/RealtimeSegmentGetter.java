package com.metamx.druid.loading;

import com.google.inject.Inject;
import com.metamx.common.logger.Logger;

import java.io.File;
import java.util.Map;

/**
 */
public class RealtimeSegmentGetter implements SegmentGetter
{
  private static final Logger log = new Logger(RealtimeSegmentGetter.class);

  private final S3SegmentGetterConfig config;

  @Inject
  public RealtimeSegmentGetter(
      S3SegmentGetterConfig config
  )
  {
    this.config = config;
  }

  @Override
  public File getSegmentFiles(final Map<String, Object> loadSpec) throws StorageAdapterLoadingException
  {
    try {
      File cacheFile = (File) loadSpec.get("file");

      if (!cacheFile.exists()) {
        throw new StorageAdapterLoadingException("Unable to find persisted file!");
      }
      return cacheFile;
    }
    catch (Exception e) {
      throw new StorageAdapterLoadingException(e, e.getMessage());
    }
  }

  @Override
  public boolean cleanSegmentFiles(Map<String, Object> loadSpec) throws StorageAdapterLoadingException
  {
    throw new UnsupportedOperationException();
  }
}
