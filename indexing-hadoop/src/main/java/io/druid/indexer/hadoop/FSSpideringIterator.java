/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.indexer.hadoop;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 */
public class FSSpideringIterator implements Iterator<FileStatus>
{
  public static FSSpideringIterator spiderPathPropagateExceptions(FileSystem fs, Path path)
  {
    try {
      final FileStatus[] statii = fs.listStatus(path);
      return new FSSpideringIterator(fs, statii == null ? new FileStatus[]{} : statii);
    }
    catch (FileNotFoundException e) {
      return new FSSpideringIterator(fs, new FileStatus[]{});
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static Iterable<FileStatus> spiderIterable(final FileSystem fs, final Path path)
  {
    return new Iterable<FileStatus>()
    {
      @Override
      public Iterator<FileStatus> iterator()
      {
        return spiderPathPropagateExceptions(fs, path);
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

  @Override
  public boolean hasNext()
  {
    if (statuses != null && !statuses.hasNext()) {
      statuses = null;
      index++;
    }
    return index < statii.length;
  }

  @Override
  public FileStatus next()
  {
    while (hasNext()) {
      if (statii[index].isDir()) {
        if (statuses == null) {
          statuses = spiderPathPropagateExceptions(fs, statii[index].getPath());
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

  @Override
  public void remove()
  {
    throw new UnsupportedOperationException();
  }
}
