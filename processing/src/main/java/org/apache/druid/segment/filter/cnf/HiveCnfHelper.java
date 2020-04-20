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

package org.apache.druid.segment.filter.cnf;

import org.apache.druid.query.filter.BooleanFilter;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.segment.filter.AndFilter;
import org.apache.druid.segment.filter.NotFilter;
import org.apache.druid.segment.filter.OrFilter;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * All functions in this class were basically adopted from Apache Hive and modified to use them in Druid.
 * See https://github.com/apache/hive/blob/branch-2.0/storage-api/src/java/org/apache/hadoop/hive/ql/io/sarg/SearchArgumentImpl.java
 * for original implementation.
 */
public class HiveCnfHelper
{
  public static Filter pushDownNot(Filter current)
  {
    if (current instanceof NotFilter) {
      Filter child = ((NotFilter) current).getBaseFilter();
      if (child instanceof NotFilter) {
        return pushDownNot(((NotFilter) child).getBaseFilter());
      }
      if (child instanceof AndFilter) {
        Set<Filter> children = new HashSet<>();
        for (Filter grandChild : ((AndFilter) child).getFilters()) {
          children.add(pushDownNot(new NotFilter(grandChild)));
        }
        return new OrFilter(children);
      }
      if (child instanceof OrFilter) {
        Set<Filter> children = new HashSet<>();
        for (Filter grandChild : ((OrFilter) child).getFilters()) {
          children.add(pushDownNot(new NotFilter(grandChild)));
        }
        return new AndFilter(children);
      }
    }

    if (current instanceof AndFilter) {
      Set<Filter> children = new HashSet<>();
      for (Filter child : ((AndFilter) current).getFilters()) {
        children.add(pushDownNot(child));
      }
      return new AndFilter(children);
    }

    if (current instanceof OrFilter) {
      Set<Filter> children = new HashSet<>();
      for (Filter child : ((OrFilter) current).getFilters()) {
        children.add(pushDownNot(child));
      }
      return new OrFilter(children);
    }
    return current;
  }

  public static Filter convertToCnf(Filter current)
  {
    if (current instanceof NotFilter) {
      return new NotFilter(convertToCnf(((NotFilter) current).getBaseFilter()));
    }
    if (current instanceof AndFilter) {
      Set<Filter> children = new HashSet<>();
      for (Filter child : ((AndFilter) current).getFilters()) {
        children.add(convertToCnf(child));
      }
      return new AndFilter(children);
    }
    if (current instanceof OrFilter) {
      // a list of leaves that weren't under AND expressions
      List<Filter> nonAndList = new ArrayList<Filter>();
      // a list of AND expressions that we need to distribute
      List<Filter> andList = new ArrayList<Filter>();
      for (Filter child : ((OrFilter) current).getFilters()) {
        if (child instanceof AndFilter) {
          andList.add(child);
        } else if (child instanceof OrFilter) {
          // pull apart the kids of the OR expression
          nonAndList.addAll(((OrFilter) child).getFilters());
        } else {
          nonAndList.add(child);
        }
      }
      if (!andList.isEmpty()) {
        Set<Filter> result = new HashSet<>();
        generateAllCombinations(result, andList, nonAndList);
        return new AndFilter(result);
      }
    }
    return current;
  }

  public static Filter flatten(Filter root)
  {
    if (root instanceof BooleanFilter) {
      List<Filter> children = new ArrayList<>(((BooleanFilter) root).getFilters());
      // iterate through the index, so that if we add more children,
      // they don't get re-visited
      for (int i = 0; i < children.size(); ++i) {
        Filter child = flatten(children.get(i));
        // do we need to flatten?
        if (child.getClass() == root.getClass() && !(child instanceof NotFilter)) {
          boolean first = true;
          Set<Filter> grandKids = ((BooleanFilter) child).getFilters();
          for (Filter grandkid : grandKids) {
            // for the first grandkid replace the original parent
            if (first) {
              first = false;
              children.set(i, grandkid);
            } else {
              children.add(++i, grandkid);
            }
          }
        } else {
          children.set(i, child);
        }
      }
      // if we have a singleton AND or OR, just return the child
      if (children.size() == 1 && (root instanceof AndFilter || root instanceof OrFilter)) {
        return children.get(0);
      }

      if (root instanceof AndFilter) {
        return new AndFilter(children);
      } else if (root instanceof OrFilter) {
        return new OrFilter(children);
      }
    }
    return root;
  }

  // A helper function adapted from Apache Hive, see:
  // https://github.com/apache/hive/blob/branch-2.0/storage-api/src/java/org/apache/hadoop/hive/ql/io/sarg/SearchArgumentImpl.java
  private static void generateAllCombinations(
      Set<Filter> result,
      List<Filter> andList,
      List<Filter> nonAndList
  )
  {
    Set<Filter> children = ((AndFilter) andList.get(0)).getFilters();
    if (result.isEmpty()) {
      for (Filter child : children) {
        Set<Filter> a = new HashSet<>(nonAndList);
        a.add(child);
        result.add(new OrFilter(a));
      }
    } else {
      List<Filter> work = new ArrayList<>(result);
      result.clear();
      for (Filter child : children) {
        for (Filter or : work) {
          Set<Filter> a = new HashSet<>((((OrFilter) or).getFilters()));
          a.add(child);
          result.add(new OrFilter(a));
        }
      }
    }
    if (andList.size() > 1) {
      generateAllCombinations(result, andList.subList(1, andList.size()), nonAndList);
    }
  }

  private HiveCnfHelper()
  {
  }
}
