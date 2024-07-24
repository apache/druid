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

package org.apache.druid.sql.avatica;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonStreamContext;
import com.fasterxml.jackson.core.util.DefaultIndenter;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;

import java.io.IOException;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Druid specific pretty printer which makes it more readable.
 */
public class DruidPrettyPrinter extends DefaultPrettyPrinter
{
  @Retention(RetentionPolicy.RUNTIME)
  @Target({ElementType.TYPE})
  public static @interface Inline
  {
  }

  private static final long serialVersionUID = 1L;
  private boolean currentIndent = true;

  @Override
  public DefaultPrettyPrinter createInstance()
  {
    return new DruidPrettyPrinter();
  }

  public DruidPrettyPrinter()
  {
    _objectIndenter = new DruidIndenter(this::shouldIndent);
  }

  public boolean shouldIndent(JsonStreamContext ctx)
  {
    return currentIndent ;
  }

  @Override
  public void writeStartObject(JsonGenerator g) throws IOException
  {
    currentIndent = shouldIndentObject(g.getOutputContext());
    super.writeStartObject(g);
  }
  @Override
  public void writeEndObject(JsonGenerator g, int nrOfEntries) throws IOException
  {
    super.writeEndObject(g, nrOfEntries);
    currentIndent = true;
  }

  public boolean shouldIndentObject(JsonStreamContext ctx)
  {
    Object value = ctx.getCurrentValue();
    Object annotation = value.getClass().getAnnotation(Inline.class);
    if (annotation != null) {
      return false;
    }
    return true;
  }

  static class DruidIndenter implements Indenter
  {
    private static final Indenter DEFAULT_INDENTER = DefaultIndenter.SYSTEM_LINEFEED_INSTANCE;

    private java.util.function.Predicate<JsonStreamContext> predicate;

    public DruidIndenter(java.util.function.Predicate<JsonStreamContext> predicate)
    {
      this.predicate = predicate;
    }

    @Override
    public void writeIndentation(JsonGenerator g, int level) throws IOException
    {
      if (predicate.test(g.getOutputContext()) || level < -11) {
        DEFAULT_INDENTER.writeIndentation(g, level);
      }
    }

    @Override
    public boolean isInline()
    {
      return false;
    }
  }

}
