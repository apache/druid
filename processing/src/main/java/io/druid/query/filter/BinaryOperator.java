package io.druid.query.filter;

import com.google.common.base.Predicate;
import com.google.common.base.Strings;

/**
 */
public enum BinaryOperator
{
  GT {
    @Override
    public Predicate<String> toPredicate(final String value)
    {
      return new Predicate<String>()
      {
        @Override
        public boolean apply(String input)
        {
          return input.compareTo(value) > 0;
        }
      };
    }
  },
  GTE {
    @Override
    public Predicate<String> toPredicate(final String value)
    {
      return new Predicate<String>()
      {
        @Override
        public boolean apply(String input)
        {
          return input.compareTo(value) >= 0;
        }
      };
    }
  },
  LT {
    @Override
    public Predicate<String> toPredicate(final String value)
    {
      return new Predicate<String>()
      {
        @Override
        public boolean apply(String input)
        {
          return input.compareTo(value) < 0;
        }
      };
    }
  },
  LTE {
    @Override
    public Predicate<String> toPredicate(final String value)
    {
      return new Predicate<String>()
      {
        @Override
        public boolean apply(String input)
        {
          return input.compareTo(value) <= 0;
        }
      };
    }
  },
  EQ {
    @Override
    public Predicate<String> toPredicate(final String value)
    {
      return new Predicate<String>()
      {
        @Override
        public boolean apply(String input)
        {
          return input.equals(value);
        }
      };
    }
  },
  NE {
    @Override
    public Predicate<String> toPredicate(final String value)
    {
      return new Predicate<String>()
      {
        @Override
        public boolean apply(String input)
        {
          return !input.equals(value);
        }
      };
    }
  };

  public abstract Predicate<String> toPredicate(final String value);

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
