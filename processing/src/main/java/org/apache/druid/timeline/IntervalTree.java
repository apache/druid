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
import org.apache.druid.java.util.common.StringUtils;
import org.jetbrains.annotations.VisibleForTesting;
import org.joda.time.Interval;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * A variation of Interval Trees (https://en.wikipedia.org/wiki/Interval_tree)
 * Custom optimizations for faster interval search and additional support for specific joda Interval comparator
 * arithmetic used in the project
 * <p>
 * <p>
 * Multiple different intervals can be added to the tree. It can then be searched to find all intervals matching a given
 * interval. The user specifies the match condition, such as encompassing the given interval, overlapping etc. The
 * search can return multiple results as multiple intervals in the tree could match the criteria.
 *
 * Using the tree, reduces the search time from O(N) iterating through all the intervals, to roughly O(log2(N)).
 * Furthermore, a value can be associated with each interval, which is also returned in the search result.
 *
 * <p>
 * The tree is a binary search tree sorted by interval start time. The intervals are stored as nodes in the tree.
 * Additional state containing the minimum and maximum interval bounds of the entire subtree under a node is also
 * stored in each node. This helps speed up the search for matching intervals by skipping unsuitable subtrees that will
 * not contain a matching candidate interval.
 *
 * To optimize the balancing cost w.r.t the operation time, the tree is not balanced on every modification operation.
 * Rather, a configurable imbalance tolerance from the theoretical ideal height of log2(N) is allowed, breaching which
 * triggers the rebalance.
 *
 * Not thread safe.
 * <p>
 */
public class IntervalTree<T>
{
  // The compartor for comparing the interval start timnes
  Comparator<Interval> startComparator;
  // The comparator for comparing interval end times
  Comparator<Interval> endComparator;

  @VisibleForTesting
  Node<T> root;
  int size;

  // Deviation allowed from ideal height for the maximum height on either side of tree, expressed as a
  // percentage of ideal height
  int imbalanceTolerance = 50;

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

  @VisibleForTesting
  static class Entry<T>
  {
    Interval interval;
    T value;

    public Entry(Interval interval, T value)
    {
      this.interval = interval;
      this.value = value;
    }

    @Override
    public String toString()
    {
      return "Entry{" +
              "interval=" + interval +
              ", value=" + value +
              '}';
    }
  }

  static class Node<T>
  {
    Interval interval;
    T value;
    int height;
    // The full interval range of the subtree formed by this Node
    Interval range;
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

    public String print(int level)
    {
      String prefix = "\t".repeat(level);
      String eprefix = "\t".repeat(level - 1);
      return StringUtils.format(PRINT_FORMAT,
                              prefix, interval, prefix, value, prefix, height,
                              prefix, range,
                              prefix, (left != null) ? left.print(level + 1) : null,
                              prefix, (right != null) ? right.print(level + 1) : null,
                              eprefix
                          );
    }
  }

  public void add(Interval interval, T value)
  {
    root = insert(root, interval, value);
    checkRebalance();
  }

  private Node<T> insert(Node<T> node, Interval interval, T value)
  {

    if (node == null) {
      node = new Node<>();
      node.interval = interval;
      node.value = value;
      node.height = 0;
      node.range = interval;
      ++size;
      return node;
    }

    // If exact interval already exists, just replace the value
    //if (doesMatch(node, interval)) {
    if (node.interval.equals(interval)) {
      node.value = value;
      return node;
    }

    // If start of interval matches with node sending to right to preserve stability during in order traversal retrieval
    if (startComparator.compare(interval, node.interval) < 0) {
      node.left = insert(node.left, interval, value);
    } else {
      node.right = insert(node.right, interval, value);
    }

    int lheight = (node.left != null) ? node.left.height : -1;
    int rheight = (node.right != null) ? node.right.height : -1;
    node.height = Math.max(lheight, rheight) + 1;

    if (startComparator.compare(interval, node.range) < 0) {
      node.range = node.range.withStart(interval.getStart());
    }

    if (endComparator.compare(node.range, interval) < 0) {
      node.range = node.range.withEnd(interval.getEnd());
    }

    return node;
  }

  public Map<Interval, T> findEncompassing(Interval interval)
  {
    Map<Interval, T> result = new HashMap<>();
    findMatching(root, result, i -> i.contains(interval));
    return result;
  }

  private void findMatching(Node<T> node, Map<Interval, T> result, Predicate<Interval> condition)
  {

    if (node == null) {
      return;
    }

    if (condition.apply(node.interval)) {
      result.put(node.interval, node.value);
    }

    // Search left
    if ((node.left != null) && condition.apply(node.left.range)) {
      findMatching(node.left, result, condition);
    }

    // Search right
    if (node.right != null && condition.apply(node.right.range)) {
      findMatching(node.right, result, condition);
    }
  }

  public Map<Interval, T> findOverlapping(Interval interval)
  {
    Map<Interval, T> result = new HashMap<>();
    findMatching(root, result, i -> i.overlaps(interval));
    return result;
  }

  public void remove(Interval interval)
  {
    root = removeNode(root, interval);
    checkRebalance();
  }

  private Node<T> removeNode(Node<T> node, Interval interval)
  {
    // When deleting a node, try to replace it with the right most leaf of the left sub-tree.
    // If it is does not exist, i.e. the bottom most right node in the left subtree only has a left child and does not
    // have a right child, this node becomes the replacement. Also, in this scenario, the left child (subtree) of this
    // node is moved up to it's parent as the parent's right child.
    if (node == null) {
      return null;
    }

    if (node.interval.equals(interval)) {
      // This is the node to delete
      --size;
      if ((node.left != null) && (node.right != null)) {
        // Make the right most child of the left node the new node at current level
        Node<T> newNode = unlinkRightLeaf(node.left);
        // If left node did not have any right children, it is the matching candidate
        if (node.left != newNode) {
          newNode.left = node.left;
        }
        newNode.right = node.right;
        recomputeState(newNode);
        return newNode;
      } else if (node.left != null) {
        // Right nodde is null, make the left node the new node at current level
        return node.left;
      } else if (node.right != null) {
        // Left nodde is null, make the right node the new node at current level
        return node.right;
      }
      return null;
    }

    // Current node didn't match, search children
    if (startComparator.compare(interval, node.interval) < 0) {
      node.left = removeNode(node.left, interval);
    } else {
      node.right = removeNode(node.right, interval);
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
        node.right = rnode.left;
        rnode.left = null;
      }
      recomputeState(node);
      return rnode;
    }
  }

  @VisibleForTesting
  Iterator<Entry<T>> inOrderTraverse()
  {
    List<Node<T>> nodes = new ArrayList<>(size);
    inOrderTraverse(root, nodes);
    return nodes.stream().map(node -> new Entry<T>(node.interval, node.value)).iterator();
  }

  public void rebalance()
  {
    // In order traversal followed by repeated binary segmentation of the list
    List<Node<T>> nodes = new ArrayList<>(size);
    inOrderTraverse(root, nodes);
    root = constructTree(nodes, 0, nodes.size());
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

  private Node<T> constructTree(List<Node<T>> nodes, int start, int end)
  {
    if (start == end) {
      return null;
    }
    int mid = (start + end - 1) / 2;
    Node<T> node = nodes.get(mid);
    node.left = constructTree(nodes, start, mid);
    node.right = constructTree(nodes, mid + 1, end);
    recomputeState(node);
    return node;
  }

  private void recomputeState(Node<T> node)
  {
    int lheight = (node.left != null) ? node.left.height : -1;
    int rheight = (node.right != null) ? node.right.height : -1;
    node.height = Math.max(lheight, rheight) + 1;
    node.range = computeRange(node.interval, node.left, node.right);
  }

  public void clear()
  {
    root = null;
    size = 0;
  }

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
        if (startComparator.compare(node.range, min) <= 0) {
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
