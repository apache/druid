/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013, 2014  Metamarkets Group Inc.
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

package io.druid.query.aggregation;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import io.druid.segment.ObjectColumnSelector;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

public class NashornScriptAggregatorFactory
{
  public static interface JSScriptAggregator {
    public double aggregate(double current, Object... values);

    public double combine(double a, double b);

    public double reset();
  }

  private String aggregate;
  private String reset;
  private String combine;

  public NashornScriptAggregatorFactory(String aggregate, String reset, String combine)
  {
    this.aggregate = aggregate;
    this.reset = reset;
    this.combine = combine;
  }

  public ScriptAggregator compileScript()
  {
    Preconditions.checkNotNull(aggregate, "Aggregate script must not be null");
    Preconditions.checkNotNull(combine, "Combining script must not be null");
    Preconditions.checkNotNull(reset, "Reset script must not be null");

    ScriptEngine engine = new ScriptEngineManager().getEngineByName("nashorn");
    final Invocable invocable = (Invocable) engine;

    try {
      // this is a little janky, but seems faster than using `var aggregate = function(...) { ... }`
      engine.eval(aggregate.replace("function", "function aggregate"));
      engine.eval(reset.replace("function", "function reset"));
      engine.eval(combine.replace("function", "function combine"));
    }
    catch (ScriptException e) {
      Throwables.propagate(e);
    }

    final JSScriptAggregator js = invocable.getInterface(JSScriptAggregator.class);
    if(js == null) throw new RuntimeException("Unable to find appropriate js functions");

    return new ScriptAggregator()
    {
      @Override
      public double aggregate(final double current, final ObjectColumnSelector[] selectorList)
      {
        final int size = selectorList.length;
        final Object[] args = new Object[size + 1];

        args[0] = current;
        for (int i = 0; i < size; i++) {
          final ObjectColumnSelector selector = selectorList[i];
          if (selector != null) {
            final Object arg = selector.get();
//            if (arg != null && arg.getClass().isArray()) {
//              // Context.javaToJS on an array sort of works, although it returns false for Array.isArray(...) and
//              // may have other issues too. Let's just copy the array and wrap that.
//              final Object[] arrayAsObjectArray = new Object[Array.getLength(arg)];
//              for (int j = 0; j < Array.getLength(arg); j++) {
//                arrayAsObjectArray[j] = Array.get(arg, j);
//              }
//              args[i + 1] = cx.newArray(scope, arrayAsObjectArray);
//            } else {
            args[i + 1] = arg;
//            }
          }
        }

        return js.aggregate(current, args);
      }

      @Override
      public double combine(final double a, final double b)
      {
        return js.combine(a, b);
      }

      @Override
      public double reset()
      {
        return js.reset();
      }

      @Override
      public void close()
      {
        // TODO: figure out how to close engine resources
      }
    };
  }
}
