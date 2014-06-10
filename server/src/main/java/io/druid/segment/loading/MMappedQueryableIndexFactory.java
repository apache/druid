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

import com.google.inject.Inject;
import com.metamx.common.logger.Logger;
import io.druid.segment.IndexIO;
import io.druid.segment.QueryableIndex;
import io.druid.segment.column.ColumnConfig;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;

/**
 */
public class MMappedQueryableIndexFactory implements QueryableIndexFactory
{
  private static final Logger log = new Logger(MMappedQueryableIndexFactory.class);

  private final ColumnConfig columnConfig;

  @Inject
  public MMappedQueryableIndexFactory(
      ColumnConfig columnConfig
  )
  {
    this.columnConfig = columnConfig;
  }

  @Override
  public QueryableIndex factorize(File parentDir) throws SegmentLoadingException
  {
    try {
      return IndexIO.loadIndex(parentDir, columnConfig);
    }
    catch (IOException e) {
      log.warn(e, "Got exception!!!! Going to delete parentDir[%s]", parentDir);
      try {
        FileUtils.deleteDirectory(parentDir);
      }
      catch (IOException e2) {
        log.error(e, "Problem deleting parentDir[%s]", parentDir);
      }
      throw new SegmentLoadingException(e, "%s", e.getMessage());
    }
  }
}
