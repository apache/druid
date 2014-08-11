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
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import io.druid.query.aggregation.PostAggregator;
import org.mozilla.javascript.Context;
import org.mozilla.javascript.ContextFactory;
import org.mozilla.javascript.ScriptableObject;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;


public class JavaScriptPostAggregator implements PostAggregator
{
  private static final Comparator COMPARATOR = new Comparator()
  {
    @Override
    public int compare(Object o, Object o1)
    {
      return ((Double) o).compareTo((Double) o1);
    }
  };

  private static interface Function
  {
    public double apply(final Object[] args);
  }

  private static Function compile(String function) {
    final ContextFactory contextFactory = ContextFactory.getGlobal();
    final Context context = contextFactory.enterContext();
    context.setOptimizationLevel(9);

    final ScriptableObject scope = context.initStandardObjects();

    final org.mozilla.javascript.Function fn = context.compileFunction(scope, function, "fn", 1, null);
    Context.exit();


    return new Function()
    {
      public double apply(Object[] args)
      {
        // ideally we need a close() function to discard the context once it is not used anymore
        Context cx = Context.getCurrentContext();
        if (cx == null) {
          cx = contextFactory.enterContext();
        }

        return Context.toNumber(fn.call(cx, scope, scope, args));
      }
    };
  }

  private final String name;
  private final List<String> fieldNames;
  private final String function;

  private final Function fn;


  @JsonCreator
  public JavaScriptPostAggregator(
      @JsonProperty("name") String name,
      @JsonProperty("fieldNames") final List<String> fieldNames,
      @JsonProperty("function") final String function
  )
  {
    Preconditions.checkNotNull(name, "Must have a valid, non-null post-aggregator name");
    Preconditions.checkNotNull(fieldNames, "Must have a valid, non-null fieldNames");
    Preconditions.checkNotNull(function, "Must have a valid, non-null function");

    this.name = name;
    this.fieldNames = fieldNames;
    this.function = function;

    this.fn = compile(function);
  }

  @Override
  public Set<String> getDependentFields()
  {
    return Sets.newHashSet(fieldNames);
  }

  @Override
  public Comparator getComparator()
  {
    return COMPARATOR;
  }

  @Override
  public Object compute(Map<String, Object> combinedAggregators)
  {
    final Object[] args = new Object[fieldNames.size()];
    int i = 0;
    for(String field : fieldNames) {
      args[i++] = combinedAggregators.get(field);
    }
    return fn.apply(args);
  }

  @JsonProperty
  @Override
  public String getName()
  {
    return name;
  }

  @JsonProperty
  public List<String> getFieldNames()
  {
    return fieldNames;
  }

  @JsonProperty
  public String getFunction()
  {
    return function;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    JavaScriptPostAggregator that = (JavaScriptPostAggregator) o;

    if (fieldNames != null ? !fieldNames.equals(that.fieldNames) : that.fieldNames != null) return false;
    if (fn != null ? !fn.equals(that.fn) : that.fn != null) return false;
    if (function != null ? !function.equals(that.function) : that.function != null) return false;
    if (name != null ? !name.equals(that.name) : that.name != null) return false;

    return true;
  }

  @Override
  public int hashCode()
  {
    int result = name != null ? name.hashCode() : 0;
    result = 31 * result + (fieldNames != null ? fieldNames.hashCode() : 0);
    result = 31 * result + (function != null ? function.hashCode() : 0);
    result = 31 * result + (fn != null ? fn.hashCode() : 0);
    return result;
  }
}
