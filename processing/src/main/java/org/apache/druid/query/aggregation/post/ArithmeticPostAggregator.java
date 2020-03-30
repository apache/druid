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

package org.apache.druid.query.aggregation.post;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.query.Queries;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregator;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.cache.CacheKeyBuilder;

import javax.annotation.Nullable;

import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 */
public class ArithmeticPostAggregator implements PostAggregator
{
  public static final Comparator DEFAULT_COMPARATOR = DoubleSumAggregator.COMPARATOR;

  private final String name;
  private final String fnName;
  private final List<PostAggregator> fields;
  private final Ops op;
  private final Comparator comparator;
  private final String ordering;

  public ArithmeticPostAggregator(
      String name,
      String fnName,
      List<PostAggregator> fields
  )
  {
    this(name, fnName, fields, null);
  }

  @JsonCreator
  public ArithmeticPostAggregator(
      @JsonProperty("name") String name,
      @JsonProperty("fn") String fnName,
      @JsonProperty("fields") List<PostAggregator> fields,
      @JsonProperty("ordering") @Nullable String ordering
  )
  {
    Preconditions.checkArgument(fnName != null, "fn cannot not be null");
    Preconditions.checkArgument(fields != null && fields.size() > 1, "Illegal number of fields[%s], must be > 1");

    this.name = name;
    this.fnName = fnName;
    this.fields = fields;

    this.op = Ops.lookup(fnName);
    if (op == null) {
      throw new IAE("Unknown operation[%s], known operations[%s]", fnName, Ops.getFns());
    }

    this.ordering = ordering;
    this.comparator = ordering == null ? DEFAULT_COMPARATOR : Ordering.valueOf(ordering);
  }

  @Override
  public Set<String> getDependentFields()
  {
    Set<String> dependentFields = new HashSet<>();
    for (PostAggregator field : fields) {
      dependentFields.addAll(field.getDependentFields());
    }
    return dependentFields;
  }

  @Override
  public Comparator getComparator()
  {
    return comparator;
  }

  @Override
  public Object compute(Map<String, Object> values)
  {
    Iterator<PostAggregator> fieldsIter = fields.iterator();
    Double retVal = NullHandling.defaultDoubleValue();
    if (fieldsIter.hasNext()) {
      Number nextVal = (Number) fieldsIter.next().compute(values);
      if (nextVal == null) {
        // As per SQL standard if any of the value is null, arithmetic operators will return null.
        return null;
      }
      retVal = nextVal.doubleValue();
      while (fieldsIter.hasNext()) {
        nextVal = (Number) fieldsIter.next().compute(values);
        if (nextVal == null) {
          // As per SQL standard if any of the value is null, arithmetic operators will return null.
          return null;
        }
        retVal = op.compute(retVal, (nextVal).doubleValue());
      }
    }
    return retVal;
  }

  @Override
  @JsonProperty
  public String getName()
  {
    return name;
  }

  @Override
  public ArithmeticPostAggregator decorate(Map<String, AggregatorFactory> aggregators)
  {
    return new ArithmeticPostAggregator(name, fnName, Queries.decoratePostAggregators(fields, aggregators), ordering);
  }

  @Override
  public byte[] getCacheKey()
  {
    final CacheKeyBuilder builder = new CacheKeyBuilder(PostAggregatorIds.ARITHMETIC)
        .appendString(fnName)
        .appendString(ordering);

    if (preserveFieldOrderInCacheKey(op)) {
      builder.appendCacheables(fields);
    } else {
      builder.appendCacheablesIgnoringOrder(fields);
    }

    return builder.build();
  }

  @JsonProperty("fn")
  public String getFnName()
  {
    return fnName;
  }

  @JsonProperty("ordering")
  public String getOrdering()
  {
    return ordering;
  }

  @JsonProperty
  public List<PostAggregator> getFields()
  {
    return fields;
  }

  @Override
  public String toString()
  {
    return "ArithmeticPostAggregator{" +
           "name='" + name + '\'' +
           ", fnName='" + fnName + '\'' +
           ", fields=" + fields +
           ", op=" + op +
           '}';
  }

  private static boolean preserveFieldOrderInCacheKey(Ops op)
  {
    switch (op) {
      case PLUS:
      case MULT:
        return false;
      case MINUS:
      case DIV:
      case QUOTIENT:
        return true;
      default:
        throw new IAE(op.fn);
    }
  }

  private enum Ops
  {
    PLUS("+") {
      @Override
      public double compute(double lhs, double rhs)
      {
        return lhs + rhs;
      }
    },
    MINUS("-") {
      @Override
      public double compute(double lhs, double rhs)
      {
        return lhs - rhs;
      }
    },
    MULT("*") {
      @Override
      public double compute(double lhs, double rhs)
      {
        return lhs * rhs;
      }
    },
    DIV("/") {
      @Override
      public double compute(double lhs, double rhs)
      {
        return (rhs == 0.0) ? 0 : (lhs / rhs);
      }
    },
    QUOTIENT("quotient") {
      @Override
      public double compute(double lhs, double rhs)
      {
        return lhs / rhs;
      }
    };

    private static final Map<String, Ops> LOOKUP_MAP = new HashMap<>();

    static {
      for (Ops op : Ops.values()) {
        LOOKUP_MAP.put(op.getFn(), op);
      }
    }

    private final String fn;

    Ops(String fn)
    {
      this.fn = fn;
    }

    public String getFn()
    {
      return fn;
    }

    public abstract double compute(double lhs, double rhs);

    static Ops lookup(String fn)
    {
      return LOOKUP_MAP.get(fn);
    }

    static Set<String> getFns()
    {
      return LOOKUP_MAP.keySet();
    }
  }

  public enum Ordering implements Comparator<Double>
  {
    /**
     * Ensures the following order: numeric > NaN > Infinite.
     *
     * The name may be referenced via {@link #valueOf(String)} in the constructor {@link
     * ArithmeticPostAggregator#ArithmeticPostAggregator(String, String, List, String)}.
     */
    @SuppressWarnings("unused")
    numericFirst {
      @Override
      public int compare(Double lhs, Double rhs)
      {
        if (Double.isFinite(lhs) && !Double.isFinite(rhs)) {
          return 1;
        }
        if (!Double.isFinite(lhs) && Double.isFinite(rhs)) {
          return -1;
        }
        return Double.compare(lhs, rhs);
      }
    }
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ArithmeticPostAggregator that = (ArithmeticPostAggregator) o;

    if (!comparator.equals(that.comparator)) {
      return false;
    }
    if (!fields.equals(that.fields)) {
      return false;
    }
    if (!fnName.equals(that.fnName)) {
      return false;
    }
    if (name != null ? !name.equals(that.name) : that.name != null) {
      return false;
    }
    if (op != that.op) {
      return false;
    }
    if (ordering != null ? !ordering.equals(that.ordering) : that.ordering != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    int result = name != null ? name.hashCode() : 0;
    result = 31 * result + fnName.hashCode();
    result = 31 * result + fields.hashCode();
    result = 31 * result + op.hashCode();
    result = 31 * result + comparator.hashCode();
    result = 31 * result + (ordering != null ? ordering.hashCode() : 0);
    return result;
  }
}
