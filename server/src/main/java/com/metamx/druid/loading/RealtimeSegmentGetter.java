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

import java.io.File;
import java.util.Map;

import com.google.inject.Inject;
import com.metamx.common.logger.Logger;

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
