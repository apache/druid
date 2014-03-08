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
  private static final Comparator COMPARATOR = new Comparator()
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
  private final Ops op;

  @JsonCreator
  public ArithmeticPostAggregator(
      @JsonProperty("name") String name,
      @JsonProperty("fn") String fnName,
      @JsonProperty("fields") List<PostAggregator> fields
  )
  {
    this.name = name;
    this.fnName = fnName;
    this.fields = fields;
    if (fields.size() <= 1) {
      throw new IAE("Illegal number of fields[%s], must be > 1", fields.size());
    }

    this.op = Ops.lookup(fnName);
    if (op == null) {
      throw new IAE("Unknown operation[%s], known operations[%s]", fnName, Ops.getFns());
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
    return COMPARATOR;
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

  private static enum Ops
  {
    PLUS("+")
        {
          double compute(double lhs, double rhs)
          {
            return lhs + rhs;
          }
        },
    MINUS("-")
        {
          double compute(double lhs, double rhs)
          {
            return lhs - rhs;
          }
        },
    MULT("*")
        {
          double compute(double lhs, double rhs)
          {
            return lhs * rhs;
          }
        },
    DIV("/")
        {
          double compute(double lhs, double rhs)
          {
            return (rhs == 0.0) ? 0 : (lhs / rhs);
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

    abstract double compute(double lhs, double rhs);

    static Ops lookup(String fn)
    {
      return lookupMap.get(fn);
    }

    static Set<String> getFns()
    {
      return lookupMap.keySet();
    }
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
