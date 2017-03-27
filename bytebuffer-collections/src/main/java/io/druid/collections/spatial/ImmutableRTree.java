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

package io.druid.collections.spatial;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import io.druid.collections.bitmap.BitmapFactory;
import io.druid.collections.bitmap.ImmutableBitmap;
import io.druid.collections.spatial.search.Bound;
import io.druid.collections.spatial.search.GutmanSearchStrategy;
import io.druid.collections.spatial.search.SearchStrategy;

import java.nio.ByteBuffer;

/**
 * An immutable representation of an {@link RTree} for spatial indexing.
 */
public class ImmutableRTree
{
  private static byte VERSION = 0x0;
  private final int numDims;
  private final ImmutableNode root;
  private final ByteBuffer data;
  private final SearchStrategy defaultSearchStrategy = new GutmanSearchStrategy();

  public ImmutableRTree()
  {
    this.numDims = 0;
    this.data = ByteBuffer.wrap(new byte[]{});
    this.root = null;
  }

  public ImmutableRTree(ByteBuffer data, BitmapFactory bitmapFactory)
  {
    data = data.asReadOnlyBuffer();
    final int initPosition = data.position();
    Preconditions.checkArgument(data.get(0) == VERSION, "Mismatching versions");
    this.numDims = data.getInt(1 + initPosition) & 0x7FFF;
    this.data = data;
    this.root = new ImmutableNode(numDims, initPosition, 1 + Ints.BYTES, data, bitmapFactory);
  }

  public static ImmutableRTree newImmutableFromMutable(RTree rTree)
  {
    if (rTree.getSize() == 0) {
      return new ImmutableRTree();
    }

    ByteBuffer buffer = ByteBuffer.wrap(new byte[calcNumBytes(rTree)]);

    buffer.put(VERSION);
    buffer.putInt(rTree.getNumDims());
    rTree.getRoot().storeInByteBuffer(buffer, buffer.position());
    buffer.position(0);
    return new ImmutableRTree(buffer, rTree.getBitmapFactory());
  }

  private static int calcNumBytes(RTree tree)
  {
    int total = 1 + Ints.BYTES; // VERSION and numDims

    total += calcNodeBytes(tree.getRoot());

    return total;
  }

  private static int calcNodeBytes(Node node)
  {
    int total = 0;

    // find size of this node
    total += node.getSizeInBytes();

    // recursively find sizes of child nodes
    for (Node child : node.getChildren()) {
      if (node.isLeaf()) {
        total += child.getSizeInBytes();
      } else {
        total += calcNodeBytes(child);
      }
    }

    return total;
  }

  public int size()
  {
    return data.capacity();
  }

  public ImmutableNode getRoot()
  {
    return root;
  }

  public int getNumDims()
  {
    return numDims;
  }

  public Iterable<ImmutableBitmap> search(Bound bound)
  {
    return search(defaultSearchStrategy, bound);
  }

  public Iterable<ImmutableBitmap> search(SearchStrategy strategy, Bound bound)
  {
    if (bound.getNumDims() == numDims) {
      return strategy.search(root, bound);
    } else {
      // If the dimension counts don't match (for example, if this is called on a blank `new ImmutableRTree()`)
      return ImmutableList.<ImmutableBitmap>of();
    }
  }

  public byte[] toBytes()
  {
    ByteBuffer buf = ByteBuffer.allocate(data.capacity());
    buf.put(data.asReadOnlyBuffer());
    return buf.array();
  }

  public int compareTo(ImmutableRTree other)
  {
    return this.data.compareTo(other.data);
  }
}
