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

package io.druid.indexer.hadoop;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 */
public class FSSpideringIterator implements Iterator<FileStatus>
{
  public static FSSpideringIterator spiderPathPropogateExceptions(FileSystem fs, Path path)
  {
    try {
      final FileStatus[] statii = fs.listStatus(path);
      return new FSSpideringIterator(fs, statii == null ? new FileStatus[]{} : statii);
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static Iterable<FileStatus> spiderIterable(final FileSystem fs, final Path path)
  {
    return new Iterable<FileStatus>()
    {
      public Iterator<FileStatus> iterator()
      {
        return spiderPathPropogateExceptions(fs, path);
      }
    };
  }

  private final FileSystem fs;
  private final FileStatus[] statii;

  FSSpideringIterator statuses = null;
  int index = 0;

  public FSSpideringIterator(
      FileSystem fs,
      FileStatus[] statii
  )
  {
    this.fs = fs;
    this.statii = statii;
  }

  public boolean hasNext()
  {
    if (statuses != null && !statuses.hasNext()) {
      statuses = null;
      index++;
    }
    return index < statii.length;
  }

  public FileStatus next()
  {
    while (hasNext()) {
      if (statii[index].isDir()) {
        if (statuses == null) {
          statuses = spiderPathPropogateExceptions(fs, statii[index].getPath());
        } else if (statuses.hasNext()) {
          return statuses.next();
        }
      } else {
        ++index;
        return statii[index - 1];
      }
    }
    throw new NoSuchElementException();
  }

  public void remove()
  {
    throw new UnsupportedOperationException();
  }
}
