package com.metamx.druid.kv;

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
      buffer.limit(buffer.position() + numBytes);
      return new ImmutableRTree(buffer.asReadOnlyBuffer());
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
