/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.query.aggregation.post;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.metamx.common.IAE;
import io.druid.query.aggregation.PostAggregator;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 */
public class ArithmeticPostAggregator implements PostAggregator
{
  private static final Comparator DEFAULT_COMPARATOR = new Comparator()
  {
    @Override
    public int compare(Object o, Object o1)
    {
      return ((Double) o).compareTo((Double) o1);
    }
  };

  private final String name;
  private final String fnName;
  private final List<PostAggregator> fields;
  private final Op op;
  private final Comparator comparator;
  private final String ordering;
  private final String opStrategy;

  public ArithmeticPostAggregator(
      String name,
      String fnName,
      List<PostAggregator> fields
  )
  {
    this(name, fnName, fields, null, null);
  }

  @JsonCreator
  public ArithmeticPostAggregator(
      @JsonProperty("name") String name,
      @JsonProperty("fn") String fnName,
      @JsonProperty("fields") List<PostAggregator> fields,
      @JsonProperty("ordering") String ordering,
      @JsonProperty("opStrategy") String opStrategy
  )
  {
    this.name = name;
    this.fnName = fnName;
    this.fields = fields;
    if (fields.size() <= 1) {
      throw new IAE("Illegal number of fields[%s], must be > 1", fields.size());
    }

    Ops baseOp = Ops.lookup(fnName);
    if (baseOp == null) {
      throw new IAE("Unknown operation[%s], known operations[%s]", fnName, Ops.getFns());
    }

    this.ordering = ordering;
    this.comparator = ordering == null ? DEFAULT_COMPARATOR : Ordering.valueOf(ordering);

    this.opStrategy = opStrategy == null && baseOp.equals(Ops.DIV) ? OpStrategy.zeroDivisionByZero.name() : opStrategy;

    if(this.opStrategy != null) {
      this.op = Ops.withStrategy(baseOp, OpStrategy.valueOf(this.opStrategy));
    } else {
      this.op = baseOp;
    }
  }

  @Override
  public Set<String> getDependentFields()
  {
    Set<String> dependentFields = Sets.newHashSet();
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
    double retVal = 0.0;
    if (fieldsIter.hasNext()) {
      retVal = ((Number) fieldsIter.next().compute(values)).doubleValue();
      while (fieldsIter.hasNext()) {
        retVal = op.compute(retVal, ((Number) fieldsIter.next().compute(values)).doubleValue());
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

  @JsonProperty("opStrategy")
  public String getOpStrategy()
  {
    return opStrategy;
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

  static interface Op {
    double compute(double lhs, double rhs);
  }

  private static enum Ops implements Op
  {
    PLUS("+")
        {
          public double compute(double lhs, double rhs)
          {
            return lhs + rhs;
          }
        },
    MINUS("-")
        {
          public double compute(double lhs, double rhs)
          {
            return lhs - rhs;
          }
        },
    MULT("*")
        {
          public double compute(double lhs, double rhs)
          {
            return lhs * rhs;
          }
        },
    DIV("/")
        {
          public double compute(double lhs, double rhs)
          {
            return lhs / rhs;
          }
        };

    private static final Map<String, Ops> lookupMap = Maps.newHashMap();

    static {
      for (Ops op : Ops.values()) {
        lookupMap.put(op.getFn(), op);
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

    static Ops lookup(String fn)
    {
      return lookupMap.get(fn);
    }

    static Set<String> getFns()
    {
      return lookupMap.keySet();
    }

    public static Op withStrategy(final Op baseOp, final OpStrategy strategy) {
      if(strategy.equals(OpStrategy.none)) {
        return baseOp;
      }
      return new Op()
      {
        @Override
        public double compute(double lhs, double rhs)
        {
          return strategy.compute(baseOp, lhs, rhs);
        }
      };
    }
  }

  public static enum Ordering implements Comparator<Double> {
    numericFirst {
      public int compare(Double lhs, Double rhs) {
        if(Double.isInfinite(lhs) || Double.isNaN(lhs)) {
          return -1;
        }
        if(Double.isInfinite(rhs) || Double.isNaN(rhs)) {
          return 1;
        }
        return Double.compare(lhs, rhs);
      }
    }
  }

  public static enum OpStrategy
  {
    none {
      public double compute(Op op, double lhs, double rhs) {
        return op.compute(lhs, rhs);
      }
    },

    zeroDivisionByZero {
      public double compute(Op op, double lhs, double rhs) {
        if(rhs == 0) { return 0; }
        else return op.compute(lhs, rhs);
      }
    },

    nanDivisionByZero {
      public double compute(Op op, double lhs, double rhs)
      {
        if(rhs == 0) { return Double.NaN; }
        else return op.compute(lhs, rhs);
      }
    };

    public abstract double compute(Op op, double lhs, double rhs);
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    ArithmeticPostAggregator that = (ArithmeticPostAggregator) o;

    if (fields != null ? !fields.equals(that.fields) : that.fields != null) return false;
    if (fnName != null ? !fnName.equals(that.fnName) : that.fnName != null) return false;
    if (name != null ? !name.equals(that.name) : that.name != null) return false;
    if (op != that.op) return false;

    return true;
  }

  @Override
  public int hashCode()
  {
    int result = name != null ? name.hashCode() : 0;
    result = 31 * result + (fnName != null ? fnName.hashCode() : 0);
    result = 31 * result + (fields != null ? fields.hashCode() : 0);
    result = 31 * result + (op != null ? op.hashCode() : 0);
    return result;
  }
}
