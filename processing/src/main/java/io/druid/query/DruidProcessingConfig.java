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

package io.druid.query;

import com.metamx.common.concurrent.ExecutorServiceConfig;
import io.druid.segment.column.ColumnConfig;
import org.skife.config.Config;

public abstract class DruidProcessingConfig extends ExecutorServiceConfig implements ColumnConfig
{
  @Config({"druid.computation.buffer.size", "${base_path}.buffer.sizeBytes"})
  public int intermediateComputeSizeBytes()
  {
    return 1024 * 1024 * 1024;
  }

  @Override @Config(value = "${base_path}.numThreads")
  public int getNumThreads()
  {
    // default to leaving one core for background tasks
    final int processors = Runtime.getRuntime().availableProcessors();
    return processors > 1 ? processors - 1 : processors;
  }

  @Config(value = "${base_path}.columnCache.sizeBytes")
  public int columnCacheSizeBytes()
  {
    return 1024 * 1024;
  }
}
