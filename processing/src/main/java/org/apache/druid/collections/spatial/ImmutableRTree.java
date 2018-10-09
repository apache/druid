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

package org.apache.druid.collections.spatial;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import it.unimi.dsi.fastutil.bytes.ByteArrays;
import org.apache.druid.collections.bitmap.BitmapFactory;
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.collections.spatial.search.Bound;
import org.apache.druid.collections.spatial.search.GutmanSearchStrategy;
import org.apache.druid.collections.spatial.search.SearchStrategy;
import org.apache.druid.io.Channels;
import org.apache.druid.segment.writeout.WriteOutBytes;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * An immutable representation of an {@link RTree} for spatial indexing.
 */
public final class ImmutableRTree implements Comparable<ImmutableRTree>
{
  private static final byte VERSION = 0x0;

  private static final ImmutableRTree EMPTY = new ImmutableRTree();

  public static ImmutableRTree empty()
  {
    return EMPTY;
  }

  private final int numDims;
  private final ImmutableNode root;
  private final ByteBuffer data;
  private final SearchStrategy defaultSearchStrategy = new GutmanSearchStrategy();

  private ImmutableRTree()
  {
    this.numDims = 0;
    this.data = ByteBuffer.wrap(ByteArrays.EMPTY_ARRAY);
    this.root = null;
  }

  public ImmutableRTree(ByteBuffer data, BitmapFactory bitmapFactory)
  {
    final int initPosition = data.position();
    Preconditions.checkArgument(data.get(initPosition) == VERSION, "Mismatching versions");
    this.numDims = data.getInt(1 + initPosition) & 0x7FFF;
    this.data = data;
    this.root = new ImmutableNode(numDims, initPosition, 1 + Integer.BYTES, data, bitmapFactory);
  }

  public static ImmutableRTree newImmutableFromMutable(RTree rTree)
  {
    if (rTree.getSize() == 0) {
      return empty();
    }

    ByteBuffer buffer = ByteBuffer.allocate(calcNumBytes(rTree));

    buffer.put(VERSION);
    buffer.putInt(rTree.getNumDims());
    rTree.getRoot().storeInByteBuffer(buffer, buffer.position());
    buffer.position(0);
    return new ImmutableRTree(buffer, rTree.getBitmapFactory());
  }

  private static int calcNumBytes(RTree tree)
  {
    int total = 1 + Integer.BYTES; // VERSION and numDims

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
    return data.remaining();
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
      return ImmutableList.of();
    }
  }

  public byte[] toBytes()
  {
    if (size() == 0) {
      return ByteArrays.EMPTY_ARRAY;
    }
    byte[] res = new byte[data.remaining()];
    data.duplicate().get(res);
    return res;
  }

  public void writeTo(WriteOutBytes out) throws IOException
  {
    if (size() != 0) {
      Channels.writeFully(out, data.duplicate());
    }
  }

  @Override
  public int compareTo(ImmutableRTree other)
  {
    return this.data.compareTo(other.data);
  }

  @Override
  public boolean equals(Object obj)
  {
    return obj instanceof ImmutableRTree && data.equals(((ImmutableRTree) obj).data);
  }

  @Override
  public int hashCode()
  {
    return data.hashCode();
  }
}
