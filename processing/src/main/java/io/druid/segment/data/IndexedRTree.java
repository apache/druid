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

package io.druid.segment.data;

import com.google.common.collect.Ordering;
import com.metamx.collections.spatial.ImmutableRTree;

import java.nio.ByteBuffer;

/**
 */
public class IndexedRTree implements Comparable<IndexedRTree>
{
  public static ObjectStrategy<ImmutableRTree> objectStrategy =
      new ImmutableRTreeObjectStrategy();

  private static Ordering<ImmutableRTree> comparator = new Ordering<ImmutableRTree>()
  {
    @Override
    public int compare(
        ImmutableRTree tree, ImmutableRTree tree1
    )
    {
      if (tree.size() == 0 && tree1.size() == 0) {
        return 0;
      }
      if (tree.size() == 0) {
        return -1;
      }
      if (tree1.size() == 0) {
        return 1;
      }
      return tree.compareTo(tree1);
    }
  }.nullsFirst();

  private final ImmutableRTree immutableRTree;

  public IndexedRTree(ImmutableRTree immutableRTree)
  {
    this.immutableRTree = immutableRTree;
  }

  @Override
  public int compareTo(IndexedRTree spatialIndexedInts)
  {
    return immutableRTree.compareTo(spatialIndexedInts.getImmutableRTree());
  }

  public ImmutableRTree getImmutableRTree()
  {
    return immutableRTree;
  }

  private static class ImmutableRTreeObjectStrategy
      implements ObjectStrategy<ImmutableRTree>
  {
    @Override
    public Class<? extends ImmutableRTree> getClazz()
    {
      return ImmutableRTree.class;
    }

    @Override
    public ImmutableRTree fromByteBuffer(ByteBuffer buffer, int numBytes)
    {

      final ByteBuffer readOnlyBuffer = buffer.asReadOnlyBuffer();
      readOnlyBuffer.limit(readOnlyBuffer.position() + numBytes);
      return new ImmutableRTree(readOnlyBuffer);
    }

    @Override
    public byte[] toBytes(ImmutableRTree val)
    {
      if (val == null || val.size() == 0) {
        return new byte[]{};
      }
      return val.toBytes();
    }

    @Override
    public int compare(ImmutableRTree o1, ImmutableRTree o2)
    {
      return comparator.compare(o1, o2);
    }
  }
}
