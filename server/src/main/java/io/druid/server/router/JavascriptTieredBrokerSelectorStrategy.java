/*
 * Druid - a distributed column store.
 * Copyright (C) 2014  Metamarkets Group Inc.
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

package io.druid.server.router;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.api.client.repackaged.com.google.common.base.Throwables;
import com.google.common.base.Optional;
import io.druid.query.Query;

import javax.script.Compilable;
import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

public class JavascriptTieredBrokerSelectorStrategy implements TieredBrokerSelectorStrategy
{
  private final SelectorFunction function;

  public JavascriptTieredBrokerSelectorStrategy(@JsonProperty("function") String function)
  {
    final ScriptEngine engine = new ScriptEngineManager().getEngineByName("javascript");
    try {
      ((Compilable)engine).compile("var apply = " + function).eval();
    } catch(ScriptException e) {
      Throwables.propagate(e);
    }
    this.function = ((Invocable)engine).getInterface(SelectorFunction.class);
  }

  @Override
  public Optional<String> getBrokerServiceName(
      TieredBrokerConfig config, Query query
  )
  {
    return Optional.fromNullable(function.apply(config, query));
  }

  private static interface SelectorFunction
  {
    public String apply(TieredBrokerConfig config, Query query);
  }
}
