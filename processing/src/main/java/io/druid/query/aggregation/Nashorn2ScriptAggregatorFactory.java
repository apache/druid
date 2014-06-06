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

public class Nashorn2ScriptAggregatorFactory
{
  public static interface JSScriptAggregator {
    public double aggregate(double current, Object... values);

    public double combine(double a, double b);

    public double reset();
  }

  private String aggregate;
  private String reset;
  private String combine;

  public Nashorn2ScriptAggregatorFactory(String aggregate, String reset, String combine)
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


    final String script =   "function aggregate(current, selectorList) {"
                          + "  return scriptAggregate(current, selectorList[0].get());"
                          + "}"
                          + ""
                          + "function combine(a, b) {"
                          + "  return scriptCombine(a, b);"
                          + "}"
                          + ""
                          + "function reset() {"
                          + "  return scriptReset();"
                          + "}"
                          + ""
                          + "function close() {}"
                          + "";
    try {
      // this is a little janky, but faster than using `var aggregate = function(...) { ... }`
      engine.eval(aggregate.replaceFirst("function", "function scriptAggregate"));
      engine.eval(reset.replaceFirst("function", "function scriptReset"));
      engine.eval(combine.replaceFirst("function", "function scriptCombine"));
      engine.eval(script);
    }
    catch (ScriptException e) {
      Throwables.propagate(e);
    }

    final ScriptAggregator js = invocable.getInterface(ScriptAggregator.class);
    if(js == null) throw new RuntimeException("Unable to find appropriate js functions");

    return js;
  }
}
