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

package io.druid.segment.filter;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.metamx.collections.bitmap.ImmutableBitmap;
import com.metamx.common.guava.FunctionalIterable;
import io.druid.query.filter.BitmapIndexSelector;
import io.druid.query.filter.Filter;
import io.druid.query.filter.ValueMatcher;
import io.druid.query.filter.ValueMatcherFactory;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.data.Indexed;
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
  public ImmutableBitmap getBitmapIndex(final BitmapIndexSelector selector)
  {
    final Context cx = Context.enter();
    try {
      final Indexed<String> dimValues = selector.getDimensionValues(dimension);
      ImmutableBitmap bitmap;
      if (dimValues == null) {
        bitmap = selector.getBitmapFactory().makeEmptyImmutableBitmap();
      } else {
        bitmap = selector.getBitmapFactory().union(
            FunctionalIterable.create(dimValues)
                              .filter(
                                  new Predicate<String>()
                                  {
                                    @Override
                                    public boolean apply(@Nullable String input)
                                    {
                                      return predicate.applyInContext(cx, input);
                                    }
                                  }
                              )
                              .transform(
                                  new com.google.common.base.Function<String, ImmutableBitmap>()
                                  {
                                    @Override
                                    public ImmutableBitmap apply(@Nullable String input)
                                    {
                                      return selector.getBitmapIndex(dimension, input);
                                    }
                                  }
                              )
        );
      }
      return bitmap;
    }
    finally {
      Context.exit();
    }
  }

  @Override
  public ValueMatcher makeMatcher(ValueMatcherFactory factory)
  {
    // suboptimal, since we need create one context per call to predicate.apply()
    return factory.makeValueMatcher(dimension, predicate);
  }

  static class JavaScriptPredicate implements Predicate<String>
  {
    final ScriptableObject scope;
    final Function fnApply;
    final String script;

    public JavaScriptPredicate(final String script)
    {
      Preconditions.checkNotNull(script, "script must not be null");
      this.script = script;

      final Context cx = Context.enter();
      try {
        cx.setOptimizationLevel(9);
        scope = cx.initStandardObjects();

        fnApply = cx.compileFunction(scope, script, "script", 1, null);
      }
      finally {
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
      }
      finally {
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

  @Override
  public ValueMatcher makeMatcher(ColumnSelectorFactory factory)
  {
    throw new UnsupportedOperationException();
  }

}
