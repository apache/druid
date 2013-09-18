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

package io.druid.segment.filter;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.metamx.common.guava.FunctionalIterable;
import io.druid.query.filter.BitmapIndexSelector;
import io.druid.query.filter.Filter;
import io.druid.query.filter.ValueMatcher;
import io.druid.query.filter.ValueMatcherFactory;
import it.uniroma3.mat.extendedset.intset.ImmutableConciseSet;
import org.mozilla.javascript.Context;
import org.mozilla.javascript.Function;
import org.mozilla.javascript.ScriptableObject;

import javax.annotation.Nullable;

public class JavaScriptFilter implements Filter
{
  private final JavaScriptPredicate predicate;
  private final String dimension;

  public JavaScriptFilter(String dimension, final String script)
  {
    this.dimension = dimension;
    this.predicate = new JavaScriptPredicate(script);
  }

  @Override
  public ImmutableConciseSet goConcise(final BitmapIndexSelector selector)
  {
    final Context cx = Context.enter();
    try {
      ImmutableConciseSet conciseSet = ImmutableConciseSet.union(
              FunctionalIterable.create(selector.getDimensionValues(dimension))
                                .filter(new Predicate<String>()
                                {
                                  @Override
                                  public boolean apply(@Nullable String input)
                                  {
                                    return predicate.applyInContext(cx, input);
                                  }
                                })
                                .transform(
                                    new com.google.common.base.Function<String, ImmutableConciseSet>()
                                    {
                                      @Override
                                      public ImmutableConciseSet apply(@Nullable String input)
                                      {
                                        return selector.getConciseInvertedIndex(dimension, input);
                                      }
                                    }
                                )
          );
      return conciseSet;
    } finally {
      Context.exit();
    }
  }

  @Override
  public ValueMatcher makeMatcher(ValueMatcherFactory factory)
  {
    // suboptimal, since we need create one context per call to predicate.apply()
    return factory.makeValueMatcher(dimension, predicate);
  }

  static class JavaScriptPredicate implements Predicate<String> {
    final ScriptableObject scope;
    final Function fnApply;
    final String script;

    public JavaScriptPredicate(final String script) {
      Preconditions.checkNotNull(script, "script must not be null");
      this.script = script;

      final Context cx = Context.enter();
      try {
        cx.setOptimizationLevel(9);
        scope = cx.initStandardObjects();

        fnApply = cx.compileFunction(scope, script, "script", 1, null);
      } finally {
        Context.exit();
      }
    }

    @Override
    public boolean apply(final String input)
    {
      // one and only one context per thread
      final Context cx = Context.enter();
      try {
        return applyInContext(cx, input);
      } finally {
        Context.exit();
      }

    }

    public boolean applyInContext(Context cx, String input)
    {
      return Context.toBoolean(fnApply.call(cx, scope, scope, new String[]{input}));
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

      JavaScriptPredicate that = (JavaScriptPredicate) o;

      if (!script.equals(that.script)) {
        return false;
      }

      return true;
    }

    @Override
    public int hashCode()
    {
      return script.hashCode();
    }
  }
}
