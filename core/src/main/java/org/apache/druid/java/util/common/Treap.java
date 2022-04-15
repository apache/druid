package org.apache.druid.java.util.common;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

public abstract class Treap<X extends Comparable<X>, Y>
{
  protected TreapNode root;
  protected final TreapNode NULL;

  public Treap()
  {
    NULL = new TreapNode(null);
    NULL.left = NULL.right = NULL;
    NULL.priority = Double.POSITIVE_INFINITY;
    root = NULL;
  }

  public boolean isEmpty()
  {
    return NULL.equals(root);
  }

  public int size()
  {
    return root.size;
  }

  public boolean contains(X val)
  {
    return contains(val, root);
  }

  public X lower(X val)
  {
    return lower(val, root).val;
  }

  public X upper(X val)
  {
    return upper(val, root).val;
  }

  public X floor(X val)
  {
    return floor(val, root).val;
  }

  public X ceil(X val)
  {
    return ceil(val, root).val;
  }

  public boolean insert(X val)
  {
    int oldSize = root.size;
    root = insert(new TreapNode(val), root);
    return root.size > oldSize;
  }

  public boolean remove(X val)
  {
    int oldSize = root.size;
    root = remove(val, root);
    return root.size < oldSize;
  }

  public X getMin()
  {
    TreapNode node = root;
    while (!NULL.equals(node.left)) {
      node = node.left;
    }
    return node.val;
  }

  public X getMax()
  {
    TreapNode node = root;
    while (!NULL.equals(node.right)) {
      node = node.right;
    }
    return node.val;
  }

  public void update(X val, Y lazy, boolean dir)
  {
    if (dir) {
      root = update(root, val, null, lazy);
    } else {
      root = update(root, null, val, lazy);
    }
  }

  public List<X> toList()
  {
    List<X> list = new ArrayList<>();
    accumulate(root, list);
    return list;
  }

  protected abstract Y getVal(X val);

  protected abstract X setVal(X val, Y lazy);

  protected abstract Y add(Y a, Y b);

  protected abstract Y multiply(int a, Y b);

  protected abstract Y zero();

  private boolean contains(X val, TreapNode node)
  {
    if (NULL.equals(node)) {
      return false;
    }
    final int cmp = val.compareTo(node.val);
    if (cmp < 0) {
      return contains(val, node.left);
    }
    if (cmp > 0) {
      return contains(val, node.right);
    }
    return true;
  }

  private TreapNode lower(X val, TreapNode node)
  {
    if (NULL.equals(node)) {
      return node;
    }
    final int cmp = val.compareTo(node.val);
    if (cmp <= 0) {
      return lower(val, node.left);
    } else {
      TreapNode ret = lower(val, node.right);
      return (NULL.equals(ret)) ? node : ret;
    }
  }

  private TreapNode upper(X val, TreapNode node)
  {
    if (NULL.equals(node)) {
      return node;
    }
    final int cmp = val.compareTo(node.val);
    if (cmp >= 0) {
      return upper(val, node.right);
    } else {
      TreapNode ret = upper(val, node.left);
      return (NULL.equals(ret)) ? node : ret;
    }
  }

  private TreapNode floor(X val, TreapNode node)
  {
    if (NULL.equals(node)) {
      return node;
    }
    final int cmp = val.compareTo(node.val);
    if (cmp < 0) {
      return floor(val, node.left);
    } else {
      TreapNode ret = floor(val, node.right);
      return (NULL.equals(ret)) ? node : ret;
    }
  }

  private TreapNode ceil(X val, TreapNode node)
  {
    if (NULL.equals(node)) {
      return node;
    }
    final int cmp = val.compareTo(node.val);
    if (cmp > 0) {
      return ceil(val, node.right);
    } else {
      TreapNode ret = ceil(val, node.left);
      return (NULL.equals(ret)) ? node : ret;
    }
  }

  private TreapNode insert(TreapNode val, TreapNode node)
  {
    if (NULL.equals(node)) {
      return val;
    }
    Pair<TreapNode, TreapNode> pair = split(node, val.val);
    node = merge(pair.lhs, val);
    node = merge(node, pair.rhs);
    return node;
  }

  private TreapNode remove(X val, TreapNode node)
  {
    if (NULL.equals(node)) {
      return node;
    }
    Pair<TreapNode, TreapNode> pair = split(node, val);
    TreapNode lower = lower(val, pair.lhs);
    if (NULL.equals(lower)) {
      return pair.rhs;
    }
    return merge(split(pair.lhs, lower.val).lhs, pair.rhs);
  }

  private Pair<TreapNode, TreapNode> split(TreapNode node, X val)
  {
    if (NULL.equals(node)) {
      return Pair.of(NULL, NULL);
    }
    node.lazyPropogate();
    final int cmp = val.compareTo(node.val);
    Pair<TreapNode, TreapNode> pair;
    if (cmp < 0) {
      pair = split(node.left, val);
      node.left = pair.rhs;
      pair = Pair.of(pair.lhs, node);
    } else {
      pair = split(node.right, val);
      node.right = pair.lhs;
      pair = Pair.of(node, pair.rhs);
    }
    node.recompute();
    return pair;
  }

  private TreapNode merge(TreapNode left, TreapNode right)
  {
    if (NULL.equals(left)) {
      return right;
    }
    if (NULL.equals(right)) {
      return left;
    }
    left.lazyPropogate();
    right.lazyPropogate();
    TreapNode node;
    if (left.priority < right.priority) {
      left.right = merge(left.right, right);
      node = left;
    } else {
      right.left = merge(left, right.left);
      node = right;
    }
    node.recompute();
    return node;
  }

  private TreapNode update(TreapNode node, @Nullable X begin, @Nullable X end, Y lazy)
  {
    TreapNode left = NULL;
    TreapNode right = NULL;
    if (begin != null) {
      Pair<TreapNode, TreapNode> pair = split(node, begin);
      left = pair.lhs;
      node = pair.rhs;
    }
    if (end != null) {
      Pair<TreapNode, TreapNode> pair = split(node, end);
      node = pair.lhs;
      right = pair.rhs;
    }
    node.lazy = add(node.lazy, lazy);
    node = merge(left, node);
    node = merge(node, right);
    return node;
  }

  private void accumulate(TreapNode node, List<X> list)
  {
    if (NULL.equals(node)) {
      return;
    }
    node.lazyPropogate();
    accumulate(node.left, list);
    list.add(node.val);
    accumulate(node.right, list);
  }

  class TreapNode
  {
    X val;
    Y sum;
    Y lazy;
    TreapNode left;
    TreapNode right;
    double priority;
    int size;

    TreapNode(@Nullable X val)
    {
      this(val, NULL, NULL);
      if (val != null) {
        sum = getVal(val);
        size = 1;
      }
    }

    TreapNode(@Nullable X val, @Nullable TreapNode left, @Nullable TreapNode right)
    {
      this.val = val;
      this.left = left;
      this.right = right;
      this.priority = Math.random();
      this.sum = zero();
      this.lazy = zero();
    }

    public void recompute()
    {
      if (NULL.equals(this)) {
        return;
      }
      size = 1 + left.size + right.size;
      sum = getVal(val);
      left.lazyPropogate();
      right.lazyPropogate();
      sum = add(sum, add(left.sum, right.sum));
    }

    public void lazyPropogate()
    {
      if (NULL.equals(this)) {
        return;
      }
      val = setVal(val, lazy);
      sum = add(sum, multiply(size, lazy));
      if (!NULL.equals(left)) {
        left.lazy = add(left.lazy, lazy);
      }
      if (!NULL.equals(right)) {
        right.lazy = add(right.lazy, lazy);
      }
      lazy = zero();
    }

    @Override
    public boolean equals(Object that)
    {
      return this == that;
    }
  }
}
