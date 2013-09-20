/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
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

package io.druid.indexing.common;

import com.google.inject.Inject;
import io.druid.segment.loading.OmniSegmentLoader;
import io.druid.segment.loading.SegmentLoader;
import io.druid.segment.loading.SegmentLoaderConfig;
import io.druid.segment.loading.StorageLocationConfig;

import java.io.File;
import java.util.Arrays;

/**
 */
public class SegmentLoaderFactory
{
  private final OmniSegmentLoader loader;

  @Inject
  public SegmentLoaderFactory(
      OmniSegmentLoader loader
  )
  {
    this.loader = loader;
  }

  public SegmentLoader manufacturate(File storageDir)
  {
    return loader.withConfig(
        new SegmentLoaderConfig().withLocations(Arrays.asList(new StorageLocationConfig().setPath(storageDir)))
    );
  }
}
