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

package io.druid.segment.loading;

import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import java.io.File;

/**
 */
public class StorageLocationConfig
{
  @JsonProperty
  @NotNull
  private File path = null;

  @JsonProperty
  @Min(1)
  private long maxSize = Long.MAX_VALUE;

  public File getPath()
  {
    return path;
  }

  public StorageLocationConfig setPath(File path)
  {
    this.path = path;
    return this;
  }

  public long getMaxSize()
  {
    return maxSize;
  }

  public StorageLocationConfig setMaxSize(long maxSize)
  {
    this.maxSize = maxSize;
    return this;
  }

  @Override
  public String toString()
  {
    return "StorageLocationConfig{" +
           "path=" + path +
           ", maxSize=" + maxSize +
           '}';
  }
}
