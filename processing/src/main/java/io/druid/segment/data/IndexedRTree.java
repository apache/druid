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

package io.druid.segment.data;

import com.google.common.collect.Ordering;
import io.druid.collections.bitmap.BitmapFactory;
import io.druid.collections.spatial.ImmutableRTree;

import java.nio.ByteBuffer;

/**
 */
public class IndexedRTree implements Comparable<IndexedRTree>
{
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

  public static class ImmutableRTreeObjectStrategy
      implements ObjectStrategy<ImmutableRTree>
  {
    private final BitmapFactory bitmapFactory;

    public ImmutableRTreeObjectStrategy(BitmapFactory bitmapFactory)
    {
      this.bitmapFactory = bitmapFactory;
    }

    @Override
    public Class<? extends ImmutableRTree> getClazz()
    {
      return ImmutableRTree.class;
    }

    @Override
    public ImmutableRTree fromByteBuffer(ByteBuffer buffer, int numBytes)
    {
      buffer.limit(buffer.position() + numBytes);
      return new ImmutableRTree(buffer, bitmapFactory);
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
