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

package org.apache.druid.quidem;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.util.DefaultIndenter;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.core.util.MinimalPrettyPrinter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.apache.druid.msq.indexing.report.MSQResultsReport.ColumnAndType;
import org.apache.druid.segment.column.ColumnSignature;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class A12
{
  static class MyIntender implements DefaultPrettyPrinter.Indenter
  {
    boolean isInside = false;

    @Override
    public boolean isInline()
    {
      return getDelegate().isInline();
    }

    private DefaultPrettyPrinter.Indenter getDelegate()
    {
      if (isInside) {
        return DefaultPrettyPrinter.NopIndenter.instance;
      } else {
        return DefaultIndenter.SYSTEM_LINEFEED_INSTANCE;
      }
    }

    @Override
    public void writeIndentation(JsonGenerator g, int level) throws IOException
    {
      if (isX(g)) {
      }
      isInside = isX(g);
      if (isInside) {
        int asd = 1;
      }
      getDelegate().writeIndentation(g, level);
    }

    private boolean isX(JsonGenerator g)
    {
      Object v = g.getCurrentValue();

      return v instanceof ColumnSignature;
      // return false;
      // return v instanceof RowSignature;
      // return g.getCurrentValue() instanceof List;

    }

  }

  static class aa extends DefaultPrettyPrinter
  {

    public aa()
    {
      super();
      _objectIndenter = new MyIntender();
    }

    @Override
    public DefaultPrettyPrinter createInstance()
    {
      return new aa();
    }

    // @Override
    // public void writeObjectEntrySeparator(JsonGenerator g) throws IOException
    // {
    // g.writeRaw(_separators.getObjectEntrySeparator());
    // _objectIndenter.writeIndentation(g, _nesting);
    // }

    @Override
    public void writeStartArray(JsonGenerator g) throws IOException
    {
      Object cv = g.getCurrentValue();
      if (isX(g)) {
        g.setPrettyPrinter(new MinimalPrettyPrinter());
      }
      super.writeStartArray(g);

    }

    @Override
    public void writeObjectEntrySeparator(JsonGenerator g) throws IOException
    {

        super.writeObjectEntrySeparator(g);
    }

    @Override
    public void writeStartObject(JsonGenerator g) throws IOException
    {
      super.writeStartObject(g);
    }

    public void writeEndObject1(JsonGenerator g, int nrOfEntries) throws IOException
    {
      Indenter old = _objectIndenter;
      if(isX(g))
      {
        _objectIndenter=DefaultIndenter.SYSTEM_LINEFEED_INSTANCE;
      }
      super.writeEndObject(g, nrOfEntries);
      if(isX(g))
      {
        _objectIndenter=old;
      }
    }

    private boolean isX(JsonGenerator g)
    {
      Object v = g.getCurrentValue();
      return v instanceof ColumnSignature;
      // return g.getCurrentValue() instanceof List;

    }

    @Override
    public void writeEndArray(JsonGenerator g, int nrOfValues) throws IOException
    {
      Object cv = g.getCurrentValue();
      super.writeEndArray(g, nrOfValues);

    }
  }

  static class Asd
  {
    @JsonProperty
    int asd = 11;
    @JsonProperty
    RowSignature rs;
    @JsonProperty
    ColumnAndType or = new ColumnAndType("Asd", ColumnType.DOUBLE);

    Asd()
    {
      rs = RowSignature.builder()
          .add("s", ColumnType.STRING)
          .add("d", ColumnType.DOUBLE)
          .add("d1", ColumnType.LONG)
          .build();
      asd = 1111;
    }
  }

  @Test
  public void sdf() throws JsonProcessingException
  {
    ObjectMapper om = new ObjectMapper();
    ObjectWriter w = om.writer(new aa());

    final RowSignature.Builder builder = RowSignature.builder()
        .add("s", ColumnType.STRING)
        .add("d", ColumnType.DOUBLE)
        .add("d1", ColumnType.LONG);

    List<org.apache.druid.segment.column.ColumnSignature> cs = builder.build().asColumnSignatures();

    // String s = w.writeValueAsString(builder.build());
    String s = w.writeValueAsString(new Asd());
    System.out.println(s);

  }

}
