/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.segment.file;

import org.apache.druid.java.util.common.io.smoosh.SmooshedFileMapper;

import javax.annotation.Nullable;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * Immutable map of internal file name to {@link SegmentInternalFileMetadata}, stored as parallel arrays sorted by
 * name to reduce the footprint of each loaded segment. Iteration is in name order, and each file has a position in
 * that order.
 * <p>
 * Positions are persisted: {@link PartialSegmentFileMapperV10} indexes its on-disk download bitmap by them. The order
 * must therefore remain the natural order of {@link String}.
 * <p>
 * Lookups are binary searches, and {@link #get} creates a new {@link SegmentInternalFileMetadata} on each call.
 * Callers that look up files often should use {@link #indexOf} and the positional accessors instead.
 */
public final class SegmentInternalFileMap extends AbstractMap<String, SegmentInternalFileMetadata>
{
  private final String[] names;
  private final int[] containers;
  private final long[] startOffsets;
  private final long[] sizes;

  private SegmentInternalFileMap(String[] names, int[] containers, long[] startOffsets, long[] sizes)
  {
    this.names = names;
    this.containers = containers;
    this.startOffsets = startOffsets;
    this.sizes = sizes;
  }

  /**
   * Returns a map with the same entries as the given map. File names are interned.
   */
  public static SegmentInternalFileMap copyOf(Map<String, SegmentInternalFileMetadata> files)
  {
    if (files instanceof SegmentInternalFileMap) {
      return (SegmentInternalFileMap) files;
    }

    final String[] names = new String[files.size()];
    int i = 0;
    for (String name : files.keySet()) {
      names[i++] = SmooshedFileMapper.STRING_INTERNER.intern(name);
    }
    Arrays.sort(names);

    final int[] containers = new int[names.length];
    final long[] startOffsets = new long[names.length];
    final long[] sizes = new long[names.length];
    for (i = 0; i < names.length; i++) {
      final SegmentInternalFileMetadata metadata = files.get(names[i]);
      containers[i] = metadata.getContainer();
      startOffsets[i] = metadata.getStartOffset();
      sizes[i] = metadata.getSize();
    }
    return new SegmentInternalFileMap(names, containers, startOffsets, sizes);
  }

  /**
   * Returns the position of the given file, or a negative number if there is no such file.
   */
  public int indexOf(String name)
  {
    return Arrays.binarySearch(names, name);
  }

  /**
   * Returns the name of the file at the given position.
   */
  public String getName(int index)
  {
    return names[index];
  }

  /**
   * Returns the container of the file at the given position. See {@link SegmentInternalFileMetadata#getContainer()}.
   */
  public int getContainer(int index)
  {
    return containers[index];
  }

  /**
   * Returns the start offset of the file at the given position. See
   * {@link SegmentInternalFileMetadata#getStartOffset()}.
   */
  public long getStartOffset(int index)
  {
    return startOffsets[index];
  }

  /**
   * Returns the size of the file at the given position. See {@link SegmentInternalFileMetadata#getSize()}.
   */
  public long getSize(int index)
  {
    return sizes[index];
  }

  @Override
  public int size()
  {
    return names.length;
  }

  @Override
  public boolean containsKey(Object key)
  {
    return key instanceof String && indexOf((String) key) >= 0;
  }

  @Nullable
  @Override
  public SegmentInternalFileMetadata get(Object key)
  {
    if (!(key instanceof String)) {
      return null;
    }
    final int index = indexOf((String) key);
    return index < 0 ? null : makeMetadata(index);
  }

  @Override
  public Set<String> keySet()
  {
    return new AbstractSet<>()
    {
      @Override
      public Iterator<String> iterator()
      {
        return Arrays.asList(names).iterator();
      }

      @Override
      public int size()
      {
        return names.length;
      }

      @Override
      public boolean contains(Object o)
      {
        return containsKey(o);
      }
    };
  }

  @Override
  public Set<Entry<String, SegmentInternalFileMetadata>> entrySet()
  {
    return new AbstractSet<>()
    {
      @Override
      public Iterator<Entry<String, SegmentInternalFileMetadata>> iterator()
      {
        return new Iterator<>()
        {
          private int next = 0;

          @Override
          public boolean hasNext()
          {
            return next < names.length;
          }

          @Override
          public Entry<String, SegmentInternalFileMetadata> next()
          {
            if (!hasNext()) {
              throw new NoSuchElementException();
            }
            final int index = next++;
            return Map.entry(names[index], makeMetadata(index));
          }
        };
      }

      @Override
      public int size()
      {
        return names.length;
      }
    };
  }

  private SegmentInternalFileMetadata makeMetadata(int index)
  {
    return new SegmentInternalFileMetadata(containers[index], startOffsets[index], sizes[index]);
  }
}
