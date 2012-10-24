package com.metamx.druid.loading;

import com.metamx.common.MapUtils;
import com.metamx.common.logger.Logger;
import com.metamx.druid.StorageAdapter;

import javax.inject.Inject;
import java.util.Map;

/**
 */
public class DelegatingStorageAdapterLoader implements StorageAdapterLoader
{
  private static final Logger log = new Logger(DelegatingStorageAdapterLoader.class);

  private volatile Map<String, StorageAdapterLoader> loaderTypes;

  @Inject
  public void setLoaderTypes(
      Map<String, StorageAdapterLoader> loaderTypes
  )
  {
    this.loaderTypes = loaderTypes;
  }

  @Override
  public StorageAdapter getAdapter(Map<String, Object> loadSpec) throws StorageAdapterLoadingException
  {
    return getLoader(loadSpec).getAdapter(loadSpec);
  }

  @Override
  public void cleanupAdapter(Map<String, Object> loadSpec) throws StorageAdapterLoadingException
  {
    getLoader(loadSpec).cleanupAdapter(loadSpec);
  }

  private StorageAdapterLoader getLoader(Map<String, Object> loadSpec) throws StorageAdapterLoadingException
  {
    String type = MapUtils.getString(loadSpec, "type");
    StorageAdapterLoader loader = loaderTypes.get(type);

    if (loader == null) {
      throw new StorageAdapterLoadingException("Unknown loader type[%s].  Known types are %s", type, loaderTypes.keySet());
    }
    return loader;
  }
}
