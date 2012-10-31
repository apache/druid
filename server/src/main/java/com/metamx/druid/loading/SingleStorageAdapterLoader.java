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

import java.util.Map;

import com.google.inject.Inject;
import com.metamx.druid.StorageAdapter;

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
