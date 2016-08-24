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

package io.druid.math.expr;

import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.List;

/**
 */
public class Expressions
{
  public static <T extends Expression> T convertToCNF(T current, Expression.Factory<T> factory)
  {
    current = pushDownNot(current, factory);
    current = flatten(current, factory);
    current = convertToCNFInternal(current, factory);
    current = flatten(current, factory);
    return current;
  }

  // CNF conversion functions were adapted from Apache Hive, see:
  // https://github.com/apache/hive/blob/branch-2.0/storage-api/src/java/org/apache/hadoop/hive/ql/io/sarg/SearchArgumentImpl.java
  private static <T extends Expression> T pushDownNot(T current, Expression.Factory<T> factory)
  {
    if (current instanceof Expression.NotExpression) {
      T child = ((Expression.NotExpression) current).getChild();
      if (child instanceof Expression.NotExpression) {
        return pushDownNot(((Expression.NotExpression) child).<T>getChild(), factory);
      }
      if (child instanceof Expression.AndExpression) {
        List<T> children = Lists.newArrayList();
        for (T grandChild : ((Expression.AndExpression) child).<T>getChildren()) {
          children.add(pushDownNot(factory.not(grandChild), factory));
        }
        return factory.or(children);
      }
      if (child instanceof Expression.OrExpression) {
        List<T> children = Lists.newArrayList();
        for (T grandChild : ((Expression.OrExpression) child).<T>getChildren()) {
          children.add(pushDownNot(factory.not(grandChild), factory));
        }
        return factory.and(children);
      }
    }


    if (current instanceof Expression.AndExpression) {
      List<T> children = Lists.newArrayList();
      for (T child : ((Expression.AndExpression) current).<T>getChildren()) {
        children.add(pushDownNot(child, factory));
      }
      return factory.and(children);
    }

    if (current instanceof Expression.OrExpression) {
      List<T> children = Lists.newArrayList();
      for (T child : ((Expression.OrExpression) current).<T>getChildren()) {
        children.add(pushDownNot(child, factory));
      }
      return factory.or(children);
    }
    return current;
  }

  // CNF conversion functions were adapted from Apache Hive, see:
  // https://github.com/apache/hive/blob/branch-2.0/storage-api/src/java/org/apache/hadoop/hive/ql/io/sarg/SearchArgumentImpl.java
  private static <T extends Expression> T convertToCNFInternal(T current, Expression.Factory<T> factory)
  {
    if (current instanceof Expression.NotExpression) {
      return factory.not(convertToCNFInternal(((Expression.NotExpression) current).<T>getChild(), factory));
    }
    if (current instanceof Expression.AndExpression) {
      List<T> children = Lists.newArrayList();
      for (T child : ((Expression.AndExpression) current).<T>getChildren()) {
        children.add(convertToCNFInternal(child, factory));
      }
      return factory.and(children);
    }
    if (current instanceof Expression.OrExpression) {
      // a list of leaves that weren't under AND expressions
      List<T> nonAndList = new ArrayList<T>();
      // a list of AND expressions that we need to distribute
      List<T> andList = new ArrayList<T>();
      for (T child : ((Expression.OrExpression) current).<T>getChildren()) {
        if (child instanceof Expression.AndExpression) {
          andList.add(child);
        } else if (child instanceof Expression.OrExpression) {
          // pull apart the kids of the OR expression
          for (T grandChild : ((Expression.OrExpression) child).<T>getChildren()) {
            nonAndList.add(grandChild);
          }
        } else {
          nonAndList.add(child);
        }
      }
      if (!andList.isEmpty()) {
        List<T> result = Lists.newArrayList();
        generateAllCombinations(result, andList, nonAndList, factory);
        return factory.and(result);
      }
    }
    return current;
  }

  // CNF conversion functions were adapted from Apache Hive, see:
  // https://github.com/apache/hive/blob/branch-2.0/storage-api/src/java/org/apache/hadoop/hive/ql/io/sarg/SearchArgumentImpl.java
  private static <T extends Expression> T flatten(T root, Expression.Factory<T> factory)
  {
    if (root instanceof Expression.BooleanExpression) {
      List<T> children = new ArrayList<>();
      children.addAll(((Expression.BooleanExpression) root).<T>getChildren());
      // iterate through the index, so that if we add more children,
      // they don't get re-visited
      for (int i = 0; i < children.size(); ++i) {
        T child = flatten(children.get(i), factory);
        // do we need to flatten?
        if (child.getClass() == root.getClass() && !(child instanceof Expression.NotExpression)) {
          boolean first = true;
          List<T> grandKids = ((Expression.BooleanExpression) child).getChildren();
          for (T grandkid : grandKids) {
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
      if (children.size() == 1 && (root instanceof Expression.BooleanExpression)) {
        return children.get(0);
      }

      if (root instanceof Expression.AndExpression) {
        return factory.and(children);
      } else if (root instanceof Expression.OrExpression) {
        return factory.or(children);
      }
    }
    return root;
  }

  // CNF conversion functions were adapted from Apache Hive, see:
  // https://github.com/apache/hive/blob/branch-2.0/storage-api/src/java/org/apache/hadoop/hive/ql/io/sarg/SearchArgumentImpl.java
  private static <T extends Expression> void generateAllCombinations(
      List<T> result,
      List<T> andList,
      List<T> nonAndList,
      Expression.Factory<T> factory
  )
  {
    List<T> children = ((Expression.AndExpression) andList.get(0)).getChildren();
    if (result.isEmpty()) {
      for (T child : children) {
        List<T> a = Lists.newArrayList(nonAndList);
        a.add(child);
        result.add(factory.or(a));
      }
    } else {
      List<T> work = new ArrayList<>(result);
      result.clear();
      for (T child : children) {
        for (T or : work) {
          List<T> a = Lists.<T>newArrayList(((Expression.OrExpression) or).<T>getChildren());
          a.add(child);
          result.add(factory.or(a));
        }
      }
    }
    if (andList.size() > 1) {
      generateAllCombinations(
          result, andList.subList(1, andList.size()), nonAndList, factory
      );
    }
  }
}
