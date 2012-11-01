/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

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
