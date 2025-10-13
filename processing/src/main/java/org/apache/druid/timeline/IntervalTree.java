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
 * Multiple intervals can be added to the tree. The tree can then be searched for intervals matching a given interval.
 * A match is any interval that fully encompasses or exactly matches the given interval, leading for the search to
 * return multiple results. Using the tree, reduces the search time from O(N) iterating through all the intervals to
 * find all the matches, to roughly O(log2(N)). Furthermore, a value can be associated with each interval, which is also
 * returned during the search.
 *
 * <p>
 * The tree is a binary search tree sorted by interval start time. The intervals are stored as nodes in the tree.
 * Additional state containing the minimum and maximum interval bounds of the entire subtree under a node, is also tored
 * in each node. This helps speed up the search for matching intervals by skipping unsuitable subtrees that will not
 * contain a matching candidate interval.
 * <p>
 *
 * Not thread safe
 */
public class IntervalTree<T>
{
  Comparator<Interval> comparator;
  Comparator<Interval> highComparator;

  @VisibleForTesting
  Node<T> root;
  int size;

  // Deviation allowed from ideal height for the maximum height on either side of tree, expressed as a
  // percentage of ideal height
  int imbalanceTolerance = 50;

  public IntervalTree(Comparator<Interval> comparator)
  {
    this(comparator, comparator);
  }

  public IntervalTree(Comparator<Interval> comparator, Comparator<Interval> highComparator)
  {
    this.comparator = comparator;
    this.highComparator = highComparator;
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
    // The min and max of the range for the subtree
    Interval min;
    Interval max;
    Node<T> left;
    Node<T> right;


    @Override
    public String toString()
    {
      return "Node{" +
              "interval=" + interval +
              ", value=" + value +
              ", min=" + min +
              ", max=" + max +
              ", left=" + left +
              ", right=" + right +
              '}';
    }

    public String print(int level)
    {
      StringBuilder sb = new StringBuilder();
      String prefix = "\t".repeat(level);
      sb.append(prefix).append("{").append("\n");
      sb.append(prefix).append("interval = ").append(interval).append("\n");
      sb.append(prefix).append("value = ").append(value).append("\n");
      sb.append(prefix).append("min = ").append(min).append("\n");
      sb.append(prefix).append("max = ").append(max).append("\n");
      sb.append(prefix).append("left = ").append((left != null) ? left.print(level + 1) : null).append("\n");
      sb.append(prefix).append("right = ").append((right != null) ? right.print(level + 1) : null).append("\n");
      sb.append(prefix).append("}");
      return sb.toString();
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
      node.min = interval;
      node.max = interval;
      ++size;
      return node;
    }

    // If start of interval matches with node sending to right to preserve stability during in order traversal retrieval
    if (comparator.compare(interval, node.interval) < 0) {
      node.left = insert(node.left, interval, value);
    } else {
      node.right = insert(node.right, interval, value);
    }

    int lheight = (node.left != null) ? node.left.height : -1;
    int rheight = (node.right != null) ? node.right.height : -1;
    node.height = Math.max(lheight, rheight) + 1;

    if (comparator.compare(interval, node.min) < 0) {
      node.min = interval;
    }

    if (highComparator.compare(node.max, interval) < 0) {
      node.max = interval;
    }

    return node;
  }

  //public List<Entry<T>> findEncompassing(Interval interval)
  public Map<Interval, T> findEncompassing(Interval interval)
  {
    //List<Entry<T>> result = new ArrayList<>();
    Map<Interval, T> result = new HashMap<>();
    findEncompassing(root, interval, result);
    return result;
  }

  //private void findEncompassing(Node<T> node, Interval interval, List<Entry<T>> result)
  private void findEncompassing(Node<T> node, Interval interval, Map<Interval, T> result)
  {

    if (node == null) {
      return;
    }

    /*
    if ((comparator.compare(interval, node.min) < 0)
            || (highComparator.compare(node.max, interval) < 0)) {
        return;
    }
    */

    if (node.interval.contains(interval)) {
      //result.add(new Entry<>(node.interval, node.value));
      result.put(node.interval, node.value);
    }

    // Matches can be found on both left and right side as the given interval start needs to be just greater
    // than a node start and end less than the node end

    // Look for potential candidates in left and right subtrees
    // If interval falls outside the min to max range of the subtree don't follow the subtree

    // Search left
    if ((node.left != null) && isIntervalInBounds(node.left, interval)) {
      findEncompassing(node.left, interval, result);
    }

    // Search right
    if (node.right != null && isIntervalInBounds(node.right, interval)) {
      findEncompassing(node.right, interval, result);
    }
  }

  private boolean isIntervalInBounds(Node<T> node, Interval interval)
  {
    return (comparator.compare(node.min, interval) <= 0)
            && (highComparator.compare(node.max, interval) >= 0);
  }


  public void remove(Interval interval)
  {
    root = removeNode(root, interval);
    checkRebalance();
  }

  private Node<T> removeNode(Node<T> node, Interval interval)
  {
    if (node == null) {
      return null;
    }

    if (node.interval.equals(interval)) {
      --size;
      if ((node.left != null) && (node.right != null)) {
        makeLeftChild(node.right, node.left);
        return node.right;
      } else if (node.left != null) {
        return node.left;
      } else if (node.right != null) {
        return node.right;
      }
      return null;
    }

    if (comparator.compare(interval, node.interval) <= 0) {
      node.left = removeNode(node.left, interval);
    } else {
      node.right = removeNode(node.right, interval);
    }

    recomputeState(node);

    return node;
  }

  private void makeLeftChild(Node<T> node, Node<T> childNode)
  {
    if (node.left == null) {
      node.left = childNode;
    } else {
      makeLeftChild(node.left, childNode);
    }
    recomputeState(node);
  }

  @VisibleForTesting
  Iterator<Entry<T>> inOrderTraverse()
  {
    List<Node<T>> nodes = new ArrayList<>();
    inOrderTraverse(root, nodes);
    return nodes.stream().map(node -> new Entry<T>(node.interval, node.value)).iterator();
  }

  public void rebalance()
  {
    // In order traversal followed by repeated binary segmentation of the list
    List<Node<T>> nodes = new ArrayList<>();
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
    node.max = maxInterval(node.interval, node.left, node.right);
    node.min = minInterval(node.interval, node.left, node.right);
  }

  public void clear()
  {
    root = null;
    size = 0;
  }

  public int size()
  {
    //return size(root);
    return size;
  }

  @VisibleForTesting
  // returns the number of edges from root to leaf along the longest path
  int height()
  {
    return (root != null) ? root.height : -1;
  }

  private int size(Node<T> node)
  {
    if (node == null) {
      return 0;
    }
    return 1 + size(node.left) + size(node.right);
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
  private Interval maxInterval(Interval interval, Node<T>... nodes)
  {
    Interval max = interval;
    for (Node<T> node : nodes) {
      if (node != null) {
        if (highComparator.compare(node.max, max) > 0) {
          max = node.max;
        }
      }
    }
    return max;
  }

  @SafeVarargs
  private Interval minInterval(Interval interval, Node<T>... nodes)
  {
    Interval min = interval;
    for (Node<T> node : nodes) {
      if (node != null) {
        if (comparator.compare(node.min, min) <= 0) {
          min = node.min;
        }
      }
    }
    return min;
  }

}
