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

package org.apache.druid.timeline;

import com.google.common.base.Predicate;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.VisibleForTesting;
import org.joda.time.Interval;

import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Set;
import java.util.SortedMap;
import java.util.function.BiConsumer;

/**
 * A variation of Interval Trees (https://en.wikipedia.org/wiki/Interval_tree)
 * Custom optimizations for faster interval search and additional support for specific joda Interval comparator
 * arithmetic used in the project
 * <p>
 * <p>
 * Multiple different intervals can be added to the tree. It can then be searched to find all intervals matching a given
 * interval. The user specifies the match condition, such as encompassing the given interval, overlapping, etc. The
 * search can return multiple results as multiple intervals in the tree could match the criteria.
 * <p>
 * Using the tree, reduces the search time from O(N) iterating through all the intervals, to roughly O(log2(N)).
 * Furthermore, a value can be associated with each interval, which is also returned in the search result.
 *
 * <p>
 * The tree is a binary search tree sorted by interval start time. The intervals are stored as nodes in the tree.
 * Additional state containing the minimum and maximum interval bounds of the entire subtree under a node is also
 * stored in each node. This helps speed up the search for matching intervals by skipping unsuitable subtrees that will
 * not contain a matching candidate interval.
 * <p>
 * To optimize the balancing cost w.r.t the operation time, the tree is not balanced on every modification operation.
 * Rather, a configurable imbalance tolerance from the theoretical ideal height of log2(N) is allowed, breaching which
 * triggers the rebalance.
 * <p>
 * Not thread safe.
 * <p>
 */
public class IntervalTree<T> extends AbstractMap<Interval, T> implements NavigableMap<Interval, T>
{
  // The compartor for comparing the interval start timnes
  private final Comparator<Interval> startComparator;
  // The comparator for comparing interval end times
  private final Comparator<Interval> endComparator;

  @VisibleForTesting
  private Node<T> root;
  private int size;

  // Deviation allowed from ideal height for the maximum height on either side of the tree, expressed as a
  // percentage of ideal height
  private int imbalanceTolerance = 50;

  private final EntrySet entrySet = new EntrySet();

  public IntervalTree(Comparator<Interval> startComparator, Comparator<Interval> endComparator)
  {
    this.startComparator = startComparator;
    this.endComparator = endComparator;
  }

  public int getImbalanceTolerance()
  {
    return imbalanceTolerance;
  }

  public void setImbalanceTolerance(int imbalanceTolerance)
  {
    this.imbalanceTolerance = imbalanceTolerance;
  }

  static class Node<T> implements Map.Entry<Interval, T>
  {
    Interval interval;
    T value;
    int height;
    // The full interval range of the subtree formed by this Node
    Interval range;
    Node<T> parent;
    Node<T> left;
    Node<T> right;

    private static final String PRINT_FORMAT = "{\n"
                                                + "%sinterval = %s\n"
                                                + "%svalue = %s\n"
                                                + "%sheight = %d\n"
                                                + "%srange = %s\n"
                                                + "%sleft = %s\n"
                                                + "%sright = %s\n"
                                                + "%s}";

    private String print(int level)
    {
      String prefix = "\t".repeat(level);
      String eprefix = "\t".repeat(level - 1);
      return String.format(Locale.ENGLISH, PRINT_FORMAT,
                              prefix, interval, prefix, value, prefix, height,
                              prefix, range,
                              prefix, (left != null) ? left.print(level + 1) : null,
                              prefix, (right != null) ? right.print(level + 1) : null,
                              eprefix
                          );
    }

    @Override
    public Interval getKey()
    {
      return interval;
    }

    @Override
    public T getValue()
    {
      return value;
    }

    @Override
    public T setValue(T value)
    {
      T oldValue = this.value;
      this.value = value;
      return oldValue;
    }
  }

  @Override
  public T put(Interval interval, T value)
  {
    //root = insert(root, interval, value);
    T oldValue = insert(null, false, interval, value);
    checkRebalance();
    return oldValue;
  }

  private T insert(Node<T> parent, boolean left, Interval interval, T value)
  {
    // Passing parent so that when a new node is created, it can be added to parent, and we can still use return value
    // for another purpose, namely returning the old value if the key already exists in the tree
    Node<T> node;
    if (parent == null) {
      node = root;
    } else if (left) {
      node = parent.left;
    } else {
      node = parent.right;
    }

    if (node == null) {
      node = new Node<>();
      node.interval = interval;
      node.value = value;
      node.height = 0;
      node.range = interval;
      if (root == null) {
        root = node;
      } else if (left) {
        setLeftNode(parent, node);
      } else {
        setRightNode(parent, node);
      }
      ++size;
      return null;
    }

    T oldValue;

    int cmp = compareInterval(interval, node.interval);

    // If exact interval already exists, just replace the value and return
    if (cmp == 0) {
      oldValue = node.value;
      node.value = value;
      return oldValue;
    }

    if (cmp < 0) {
    // Go to the left
      oldValue = insert(node, true, interval, value);
    } else {
      // Go to the right
      oldValue = insert(node, false, interval, value);
    }
    recomputeState(node);

    //return node;
    return oldValue;
  }

  @Override
  public T get(Object key)
  {
    if (!Interval.class.isAssignableFrom(key.getClass())) {
      throw new ClassCastException("key must be an instance of Interval");
    }
    Interval interval = (Interval) key;

    T value = null;
    Node<T> node = root;
    while (node != null) {
      int cmp = compareInterval(node.getKey(), interval);
      if (cmp == 0) {
        value = node.value;
        break;
      } else if (cmp > 0) {
        node = node.left;
      } else {
        node = node.right;
      }
    }
    return value;
  }

  private int compareInterval(Interval interval1, Interval interval2)
  {
    int cmp = startComparator.compare(interval1, interval2);
    if (cmp == 0) {
      return endComparator.compare(interval1, interval2);
    }
    return cmp;
  }

  public Map<Interval, T> findEncompassing(Interval interval)
  {
    return findMatching(i -> i.contains(interval));
  }

  public Map<Interval, T> findOverlapping(Interval interval)
  {
    return findMatching(i -> i.overlaps(interval));
  }

  /**
   * Get all entries matching a given condition
   * @param condition The match condition
   *
   * This condition should not only return true when a node matches the condition but also when a child node range
   * matches. It is a convenience method for {@link #forEachMatching(Predicate, Predicate, BiConsumer)} and see the
   * method's documentation for more information. It calls the method with rangeCondition set to be same as condition.
   */
  public Map<Interval, T> findMatching(Predicate<Interval> condition)
  {
    Map<Interval, T> result = new HashMap<>();
    forEachMatching(condition, result::put);
    return result;
  }

  /**
   * Find entries matching a given condition by doing a full traversal.
   * @param condition The match condition
   *
   * The method traverses through all the nodes of the tree looking for matches.
   */
  public Map<Interval, T> findMatchingFullTraversal(Predicate<Interval> condition)
  {
    Map<Interval, T> result = new HashMap<>();
    forEachMatchingFullTraversal(condition, result::put);
    return result;
  }

  /**
   * Perform on action for matching nodes.
   * @param condition The match condition
   * @param action The action
   *
   * This condition should not only return true when a node matches the condition but also when the child node range
   * matches. It is a convenience method for {@link #forEachMatching(Predicate, Predicate, BiConsumer)} and see the
   * method's documentation for more information. It calls the method with rangeCondition set to be same as condition.
   */
  public void forEachMatching(Predicate<Interval> condition, BiConsumer<Interval, T> action)
  {
    forEachMatching(condition, condition, action);
  }

  /**
   * Perform on action for matching nodes by doing a full traversal.
   * @param condition The match condition
   * @param action The action
   *
   * The method traverses through all the nodes of the tree looking for matches.
   */
  public void forEachMatchingFullTraversal(Predicate<Interval> condition, BiConsumer<Interval, T> action)
  {
    forEachMatching(condition, null, action);
  }

  /**
   * Perform an action for matching nodes
   * @param condition The condition to match for the node
   * @param rangeCondition The condition to check a child node for, to determine whether to traverse the subtree
   * @param action The action to perform
   *
   * The rangeCondition is applied on the interval range of the child node and only if the condition returns true is the
   * child subtree traversed. Interval range is the min start time to max end time for all the nodes in the child
   * subtree. This is a lookup speedup optimization. If rangeCondition is null, the check is skipped and all the
   * children are traversed to find matches.
   *
   * In some cases such as finding nodes overlapping the given interval or encompassing the given interval, the same
   * predicate can be used for condition and rangeCondition. In other situations a full traversal maybe needed and a
   * null can be passed in for rangeCondition. There are helper methods for these.
   */
  public void forEachMatching(Predicate<Interval> condition, Predicate<Interval> rangeCondition, BiConsumer<Interval, T> action)
  {
    forEachMatching(root, condition, rangeCondition, action);
  }

  private void forEachMatching(Node<T> node, Predicate<Interval> condition, Predicate<Interval> rangeCondition, BiConsumer<Interval, T> action)
  {

    if (node == null) {
      return;
    }

    // Process in-order

    // Search left
    if ((node.left != null) && ((rangeCondition == null) || rangeCondition.apply(node.left.range))) {
      forEachMatching(node.left, condition, rangeCondition, action);
    }

    if (condition.apply(node.interval)) {
      action.accept(node.interval, node.value);
    }

    // Search right
    if (node.right != null && ((rangeCondition == null) || rangeCondition.apply(node.right.range))) {
      forEachMatching(node.right, condition, rangeCondition, action);
    }
  }

  @Override
  public T remove(Object key)
  {
    return remove((Interval) key);
  }

  public T remove(Interval interval)
  {
    List<T> oldValue = new ArrayList<>(1);
    root = removeNode(root, interval, oldValue);
    if (root != null) {
      root.parent = null;
    }
    checkRebalance();
    return oldValue.size() == 1 ? oldValue.get(0) : null;
  }

  private Node<T> removeNode(Node<T> node, Interval interval, List<T> oldValue)
  {
    // When deleting a node, try to replace it with the right most leaf of the left sub-tree.
    // If it is does not exist, i.e., the bottom most right node in the left subtree only has a left child and does not
    // have a right child, this node becomes the replacement. Also, in this scenario, the left child (subtree) of this
    // node is moved up to its parent as the parent's right child.
    if (node == null) {
      return null;
    }

    int cmp = compareInterval(interval, node.interval);

    if (cmp == 0) {
      // This is the node to delete
      --size;
      oldValue.add(node.value);
      if ((node.left != null) && (node.right != null)) {
        // Make the right bottom most child in the left subtree of the node, the new node at the current level
        Node<T> left = node.left;
        Node<T> newNode = unlinkRightLeaf(left);
        // Make the current left and right children, the left and right children of the new node respectively.
        // However, if the new node turns out to be the same as the left node, it means the left node did not have any
        // right child. In this case, only set its right child to be the current node's right child.
        if (left != newNode) {
          // A right child exists
          setLeftNode(newNode, left);
        }
        setRightNode(newNode, node.right);
        recomputeState(newNode);
        return newNode;
      } else if (node.left != null) {
        // Right node is null, make the left node the new node at current level
        return node.left;
      } else if (node.right != null) {
        // Left node is null, make the right node the new node at current level
        return node.right;
      }
      return null;
    }

    // Current node didn't match, search children
    if (cmp < 0) {
      Node<T> left = removeNode(node.left, interval, oldValue);
      setLeftNode(node, left);
    } else {
      Node<T> right = removeNode(node.right, interval, oldValue);
      setRightNode(node, right);
    }

    // Update our state as a modification may have happened somewhere in our subtree
    recomputeState(node);

    return node;
  }

  private Node<T> unlinkRightLeaf(Node<T> node)
  {
    if (node.right == null) {
      return node;
    } else {
      Node<T> rnode = unlinkRightLeaf(node.right);
      // If the right node has a left child, make it new right child
      if (rnode == node.right) {
        setRightNode(node, rnode.left);
        rnode.left = null;
      }
      recomputeState(node);
      return rnode;
    }
  }

  private void inOrderTraverse(Node<T> node, List<Node<T>> nodes)
  {
    if (node == null) {
      return;
    }
    inOrderTraverse(node.left, nodes);
    nodes.add(node);
    inOrderTraverse(node.right, nodes);
  }

  public void rebalance()
  {
    // In order traversal followed by repeated binary segmentation of the list
    List<Node<T>> nodes = new ArrayList<>(size);
    inOrderTraverse(root, nodes);
    root = constructTree(nodes, 0, nodes.size());
    root.parent = null;
  }

  private Node<T> constructTree(List<Node<T>> nodes, int start, int end)
  {
    if (start == end) {
      return null;
    }
    int mid = (start + end - 1) / 2;
    Node<T> node = nodes.get(mid);

    Node<T> left = constructTree(nodes, start, mid);
    setLeftNode(node, left);

    Node<T> right = constructTree(nodes, mid + 1, end);
    setRightNode(node, right);

    recomputeState(node);
    return node;
  }

  @Override
  public Map.Entry<Interval, T> lowerEntry(Interval key)
  {
    Node<T> lnode = null;
    Node<T> node = root;
    while (node != null) {
      // Since we want to return a smaller entry even when there is an exact match, go left in the equality case too
      if (compareInterval(key, node.getKey()) <= 0) {
        node = node.left;
      } else {
        lnode = node;
        node = node.right;
      }
    }
    return lnode;
  }

  @Override
  public Interval lowerKey(Interval key)
  {
    Map.Entry<Interval, T> entry = lowerEntry(key);
    return entry != null ? entry.getKey() : null;
  }

  @Override
  public Map.Entry<Interval, T> floorEntry(Interval key)
  {
    Node<T> fnode = null;
    Node<T> node = root;
    while (node != null) {
      int cmp = compareInterval(node.getKey(), key);
      if (cmp == 0) {
        fnode = node;
        break;
      } else if (cmp > 0) {
        node = node.left;
      } else {
        fnode = node;
        node = node.right;
      }
    }
    return fnode;
  }

  @Override
  public Interval floorKey(Interval key)
  {
    Map.Entry<Interval, T> entry = floorEntry(key);
    return entry != null ? entry.getKey() : null;
  }

  @Override
  public Map.Entry<Interval, T> ceilingEntry(Interval key)
  {
    Node<T> cnode = null;
    Node<T> node = root;
    while (node != null) {
      int cmp = compareInterval(node.getKey(), key);
      if (cmp == 0) {
        cnode = node;
        break;
      } else if (cmp > 0) {
        cnode = node;
        node = node.left;
      } else {
        node = node.right;
      }
    }
    return cnode;
  }

  @Override
  public Interval ceilingKey(Interval key)
  {
    Entry<Interval, T> entry = ceilingEntry(key);
    return entry != null ? entry.getKey() : null;
  }

  @Override
  public Map.Entry<Interval, T> higherEntry(Interval key)
  {
    Node<T> hnode = null;
    Node<T> node = root;
    while (node != null) {
      if (compareInterval(key, node.getKey()) < 0) {
        hnode = node;
        node = node.left;
      } else {
        node = node.right;
      }
    }
    return hnode;
  }

  @Override
  public Interval higherKey(Interval key)
  {
    Entry<Interval, T> entry = higherEntry(key);
    return entry != null ? entry.getKey() : null;
  }

  @Override
  public Map.Entry<Interval, T> firstEntry()
  {
    return firstEntry(root);
  }

  @Override
  public Map.Entry<Interval, T> lastEntry()
  {
    if (root == null) {
      return null;
    }
    Node<T> node = root;
    while (node.right != null) {
      node = node.right;
    }
    return node;
  }

  @Override
  public Interval firstKey()
  {
    Map.Entry<Interval, T> entry = firstEntry();
    return entry != null ? entry.getKey() : null;
  }

  @Override
  public Interval lastKey()
  {
    Map.Entry<Interval, T> entry = lastEntry();
    return entry != null ? entry.getKey() : null;
  }

  @Override
  public Map.Entry<Interval, T> pollFirstEntry()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public Map.Entry<Interval, T> pollLastEntry()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public NavigableMap<Interval, T> descendingMap()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public NavigableSet<Interval> navigableKeySet()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public NavigableSet<Interval> descendingKeySet()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public NavigableMap<Interval, T> subMap(Interval fromKey, boolean fromInclusive, Interval toKey, boolean toInclusive)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public NavigableMap<Interval, T> headMap(Interval toKey, boolean inclusive)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public NavigableMap<Interval, T> tailMap(Interval fromKey, boolean inclusive)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public Comparator<? super Interval> comparator()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public SortedMap<Interval, T> subMap(Interval fromKey, Interval toKey)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public SortedMap<Interval, T> headMap(Interval toKey)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public SortedMap<Interval, T> tailMap(Interval fromKey)
  {
    throw new UnsupportedOperationException();
  }

  private void recomputeState(Node<T> node)
  {
    int lheight = (node.left != null) ? node.left.height : -1;
    int rheight = (node.right != null) ? node.right.height : -1;
    node.height = Math.max(lheight, rheight) + 1;
    node.range = computeRange(node.interval, node.left, node.right);
  }

  @Override
  public void clear()
  {
    root = null;
    size = 0;
  }

  @Override
  public @NotNull Set<Map.Entry<Interval, T>> entrySet()
  {
    return entrySet;
  }

  @Override
  public int size()
  {
    return size;
  }

  @VisibleForTesting
    // returns the number of edges from root to leaf along the longest path
  int height()
  {
    return (root != null) ? root.height : -1;
  }

  class EntrySet extends AbstractSet<Map.Entry<Interval, T>>
  {

    // Currently this returns a distinct collection when iterating
    @Override
    public Iterator<Map.Entry<Interval, T>> iterator()
    {
      return new EntrySetIterator();
    }

    @Override
    public int size()
    {
      return IntervalTree.this.size;
    }

    class EntrySetIterator implements Iterator<Map.Entry<Interval, T>>
    {

      Node<T> current = firstEntry(IntervalTree.this.root);

      @Override
      public boolean hasNext()
      {
        return (current != null);
      }

      @Override
      public Entry<Interval, T> next()
      {
        Entry<Interval, T> entry = current;
        if (entry == null) {
          return entry;
        }
        // Move current to next node
        if (current.right != null) {
          current = firstEntry(current.right);
        } else {
          // No more right children, go up one level to the parent.
          // However, if the current node is right child of parent, keep going up till you find a parent who is on the
          // right side
          Node<T> prev;
          do {
            prev = current;
            current = current.parent;
          } while ((current != null) && (current.right == prev));
        }
        return entry;
      }
    }

  }

  /**
   * Perform a tree rebalance if the imbalance between the left and right sides of the tree has increased beyond a
   * tolerated limit, as opposed to rebalancing all the time. This is to done to strike a balance between performance
   * degradation arising from an imbalance tree and the processing overheard of rebalancing each time the contents of
   * the tree changes.
   *
   * The limit is defined using a configurable tolerance percentage in excess of an ideal balanced tree height for the
   * number of entries in the tree.
   */
  private void checkRebalance()
  {
    if (root != null) {
      int ideal = (int) Math.floor(Math.log10(size + 1) / Math.log10(2));
      double tolerance = ideal * imbalanceTolerance / 100.0;
      int threshold = ideal + (int) tolerance;
      if (root.height > threshold) {
        rebalance();
      }
    }
  }

  private Node<T> firstEntry(Node<T> node)
  {
    if (node == null) {
      return null;
    }
    while (node.left != null) {
      node = node.left;
    }
    return node;
  }

  private void setLeftNode(Node<T> node, Node<T> left)
  {
    if (node.left != left) {
      node.left = left;
      if (left != null) {
        left.parent = node;
      }
    }
  }

  private void setRightNode(Node<T> node, Node<T> right)
  {
    if (node.right != right) {
      node.right = right;
      if (right != null) {
        right.parent = node;
      }
    }
  }

  @VisibleForTesting
  public String print()
  {
    return (root != null) ? root.print(1) : null;
  }

  @SafeVarargs
  private Interval computeRange(Interval interval, Node<T>... nodes)
  {
    // Find the intervals that have the minimum start and the maximum end
    Interval min = interval;
    Interval max = interval;
    for (Node<T> node : nodes) {
      if (node != null) {
        if (startComparator.compare(node.range, min) < 0) {
          min = node.range;
        }
        if (endComparator.compare(node.range, max) > 0) {
          max = node.range;
        }
      }
    }
    // Return an interval with the min and max
    return interval.withStart(min.getStart()).withEnd(max.getEnd());
  }

}
