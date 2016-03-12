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

package io.druid.query.filter;

import com.google.common.base.Predicate;
import com.google.common.base.Strings;

import java.util.Comparator;

/**
 */
public enum BinaryOperator
{
  GT {
    @Override
    public Predicate<String> toPredicate(final Comparator<String> comparator, final String value)
    {
      return new Predicate<String>()
      {
        @Override
        public boolean apply(String input)
        {
          return comparator.compare(input, value) > 0;
        }
      };
    }
  },
  GTE {
    @Override
    public Predicate<String> toPredicate(final Comparator<String> comparator, final String value)
    {
      return new Predicate<String>()
      {
        @Override
        public boolean apply(String input)
        {
          return comparator.compare(input, value) >= 0;
        }
      };
    }
  },
  LT {
    @Override
    public Predicate<String> toPredicate(final Comparator<String> comparator, final String value)
    {
      return new Predicate<String>()
      {
        @Override
        public boolean apply(String input)
        {
          return comparator.compare(input, value) < 0;
        }
      };
    }
  },
  LTE {
    @Override
    public Predicate<String> toPredicate(final Comparator<String> comparator, final String value)
    {
      return new Predicate<String>()
      {
        @Override
        public boolean apply(String input)
        {
          return comparator.compare(input, value) <= 0;
        }
      };
    }
  },
  EQ {
    @Override
    public Predicate<String> toPredicate(final Comparator<String> comparator, final String value)
    {
      return new Predicate<String>()
      {
        @Override
        public boolean apply(String input)
        {
          return comparator.compare(input, value) == 0;
        }
      };
    }
  },
  NE {
    @Override
    public Predicate<String> toPredicate(final Comparator<String> comparator, final String value)
    {
      return new Predicate<String>()
      {
        @Override
        public boolean apply(String input)
        {
          return comparator.compare(input, value) != 0;
        }
      };
    }
  };

  public abstract Predicate<String> toPredicate(Comparator<String> comparator, final String value);

  public static BinaryOperator get(String value)
  {
    if (Strings.isNullOrEmpty(value)) {
      return EQ;
    }
    value = value.toLowerCase();
    switch (value) {
      case "gt":
      case "greaterthan":
      case ">":
        return GT;
      case "gte":
      case "greaterthanorequalto":
      case ">=":
        return GTE;
      case "lt":
      case "lessthan":
      case "<":
        return LT;
      case "lte":
      case "lessthanorequalto":
      case "<=":
        return LTE;
      case "eq":
      case "equals":
      case "=":
      case "==":
        return EQ;
      case "ne":
      case "notequals":
      case "!=":
      case "<>":
        return NE;
    }
    throw new IllegalArgumentException("Invalid operator " + value);
  }
}
