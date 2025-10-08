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

import org.joda.time.Interval;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;


/**
 * A variation of Interval Trees (https://en.wikipedia.org/wiki/Interval_tree)
 * Custom implementation for faster search and specific joda Interval comparator arithmetic used in project
 *
 *
 * Multiple intervals are added to the tree and a value can also be associated with each interval. These are stored in
 * nodes in the tree along with additional state. The tree can then be searched for intervals matching a given interval.
 * A match is any interval in the tree that fully encompasses the given interval. There can be multiple results.
 *
 * The tree is a binary search tree sorted by interval start time. Additional state containing the minimum and maximum
 * interval bounds of the entire subtree under a node, is stored on each node. This state helps speed up search for
 * matching intervals by skipping unsuitable subtrees that won't have a match.
 *
 * TODO:- Add balancing
  */
public class IntervalTree<T>
{
    Comparator<Interval> comparator;
    Comparator<Interval> highComparator;

    Node<T> root;

    public IntervalTree(Comparator<Interval> comparator)
    {
        this(comparator, comparator);
    }

    public IntervalTree(Comparator<Interval> comparator, Comparator<Interval> highComparator)
    {
        this.comparator = comparator;
        this.highComparator = highComparator;
    }

    /*
    public static class Entry<T> {
        Interval interval;
        T value;

        public Entry(Interval interval, T value) {
            this.interval = interval;
            this.value = value;
        }
    }
    */

    static class Node<T>
    {
        Interval interval;
        T value;
        // The min and max of the range for the subtree
        Interval min;
        Interval max;
        Node<T> left;
        Node<T> right;
    }

    public void add(Interval interval, T value)
    {
        root = insert(root, interval, value);
    }

    private Node<T> insert(Node<T> node, Interval interval, T value)
    {

        if (node == null) {
            node = new Node<>();
            node.interval = interval;
            node.min = interval;
            node.max = interval;
            node.value = value;
            return node;
        }

        if (comparator.compare(interval, node.interval) <= 0) {
            node.left = insert(node.left, interval, value);
        } else {
            node.right = insert(node.right, interval, value);
        }

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
        // If interval falls outside the min to max range of the subtree don't follow the subtree
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

        // If there is a potential candidate on left search left
        if ((node.left != null) && isIntervalInBounds(node.left, interval)) {
            findEncompassing(node.left, interval, result);
        }

        // If there is a potential candidate on right search right
        if (node.right != null && isIntervalInBounds(node.right, interval)) {
            findEncompassing(node.right, interval, result);
        }

        /*
        int cmp = comparator.compare(interval, node.interval);
        if (cmp <= 0) {
            findEncompassing(node.left, interval, result);
        } else {
            findEncompassing(node.right, interval, result);
        }
        */
    }

    private boolean isIntervalInBounds(Node<T> node, Interval interval)
    {
        return (comparator.compare(node.min, interval) <= 0)
                && (highComparator.compare(node.max, interval) >= 0);
    }


    public void remove(Interval interval)
    {
        root = removeNode(root, interval);
    }

    private Node<T> removeNode(Node<T> node, Interval interval)
    {
        /*
        if ((node == null) || comparator.compare(node.interval, interval) == 0) {
            return null;
        }
        */
        if (node == null) {
            return null;
        }

        if (node.interval.equals(interval)) {
            if ((node.left != null) && (node.right != null)) {
                makeLeftChild(node.right, node.left);
                return node.right;
            } else if (node.left != null) {
                return node.left;
            }
            return null;
        }

        if (comparator.compare(interval, node.interval) <= 0) {
            node.left = removeNode(node.left, interval);
        } else {
            node.right = removeNode(node.right, interval);
        }

        return node;
    }

    private void makeLeftChild(Node<T> node, Node<T> childNode)
    {
        if (node.left == null) {
            node.left = childNode;
        } else {
            makeLeftChild(node.left, childNode);
        }
    }

    public void clear()
    {
        root = null;
    }

    public int size()
    {
        return size(root);
    }

    private int size(Node<T> node)
    {
        if (node == null) {
            return 0;
        }
        return 1 + size(node.left) + size(node.right);
    }

}
