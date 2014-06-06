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
import io.druid.segment.ObjectColumnSelector;
import org.mozilla.javascript.Context;
import org.mozilla.javascript.ContextAction;
import org.mozilla.javascript.ContextFactory;
import org.mozilla.javascript.Function;
import org.mozilla.javascript.ScriptableObject;

import java.lang.reflect.Array;

public class RhinoScriptAggregatorFactory
{
  private String aggregate;
  private String reset;
  private String combine;

  public RhinoScriptAggregatorFactory(String aggregate, String reset, String combine)
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

    final ContextFactory contextFactory = ContextFactory.getGlobal();
    Context context = contextFactory.enterContext();
    context.setOptimizationLevel(9);

    final ScriptableObject scope = context.initStandardObjects();

    final Function fnAggregate = context.compileFunction(scope, aggregate, "aggregate", 1, null);
    final Function fnReset = context.compileFunction(scope, reset, "reset", 1, null);
    final Function fnCombine = context.compileFunction(scope, combine, "combine", 1, null);
    Context.exit();

    return new ScriptAggregator()
    {
      @Override
      public double aggregate(final double current, final ObjectColumnSelector[] selectorList)
      {
        Context cx = Context.getCurrentContext();
        if (cx == null) {
          cx = contextFactory.enterContext();

          // Disable primitive wrapping- we want Java strings and primitives to behave like JS entities.
          cx.getWrapFactory().setJavaPrimitiveWrap(false);
        }

        final int size = selectorList.length;
        final Object[] args = new Object[size + 1];

        args[0] = current;
        for (int i = 0 ; i < size ; i++) {
          final ObjectColumnSelector selector = selectorList[i];
          if (selector != null) {
            final Object arg = selector.get();
            if (arg != null && arg.getClass().isArray()) {
              // Context.javaToJS on an array sort of works, although it returns false for Array.isArray(...) and
              // may have other issues too. Let's just copy the array and wrap that.
              final Object[] arrayAsObjectArray = new Object[Array.getLength(arg)];
              for (int j = 0; j < Array.getLength(arg); j++) {
                arrayAsObjectArray[j] = Array.get(arg, j);
              }
              args[i + 1] = cx.newArray(scope, arrayAsObjectArray);
            } else {
              args[i + 1] = Context.javaToJS(arg, scope);
            }
          }
        }

        final Object res = fnAggregate.call(cx, scope, scope, args);
        return Context.toNumber(res);
      }

      @Override
      public double combine(final double a, final double b)
      {
        final Object res = contextFactory.call(
            new ContextAction()
            {
              @Override
              public Object run(final Context cx)
              {
                return fnCombine.call(cx, scope, scope, new Object[]{a, b});
              }
            }
        );
        return Context.toNumber(res);
      }

      @Override
      public double reset()
      {
        final Object res = contextFactory.call(
            new ContextAction()
            {
              @Override
              public Object run(final Context cx)
              {
                return fnReset.call(cx, scope, scope, new Object[]{});
              }
            }
        );
        return Context.toNumber(res);
      }

      @Override
      public void close()
      {
        if (Context.getCurrentContext() != null) {
          Context.exit();
        }
      }
    };
  }
}
