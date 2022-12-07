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

package org.apache.druid.catalog.model.table;

import org.apache.curator.shaded.com.google.common.collect.ImmutableList;
import org.apache.druid.catalog.model.CatalogUtils;
import org.apache.druid.catalog.model.ModelProperties.BooleanPropertyDefn;
import org.apache.druid.catalog.model.ModelProperties.IntPropertyDefn;
import org.apache.druid.catalog.model.ModelProperties.PropertyDefn;
import org.apache.druid.catalog.model.ModelProperties.SimplePropertyDefn;
import org.apache.druid.catalog.model.ModelProperties.StringPropertyDefn;
import org.apache.druid.catalog.model.ResolvedTable;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.impl.CsvInputFormat;
import org.apache.druid.data.input.impl.DelimitedInputFormat;
import org.apache.druid.data.input.impl.JsonInputFormat;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Definition of the input formats which converts from property
 * lists in table specs to subclasses of {@link InputFormat}.
 */
public class InputFormats
{
  public interface InputFormatDefn
  {
    String name();
    String typeTag();
    List<PropertyDefn<?>> properties();
    void validate(ResolvedTable table);
    InputFormat convert(ResolvedTable table);
  }

  public abstract static class BaseFormatDefn implements InputFormatDefn
  {
    private final String name;
    private final String typeTag;
    private final List<PropertyDefn<?>> properties;

    public BaseFormatDefn(
        final String name,
        final String typeTag,
        final List<PropertyDefn<?>> properties
    )
    {
      this.name = name;
      this.typeTag = typeTag;
      this.properties = properties;
    }

    @Override
    public String name()
    {
      return name;
    }

    @Override
    public String typeTag()
    {
      return typeTag;
    }

    @Override
    public List<PropertyDefn<?>> properties()
    {
      return properties;
    }

    @Override
    public void validate(ResolvedTable table)
    {
      convert(table);
    }
  }

  /**
   * Definition of a flat text (CSV and delimited text) input format.
   * <p>
   * Note that not all the fields in
   * {@link org.apache.druid.data.input.impl.FlatTextInputFormat
   * FlatTextInputFormat} appear here:
   * <ul>
   * <li>{@code findColumnsFromHeader} - not yet supported in MSQ.</li>
   * <li>{@code hasHeaderRow} - Always set to false since we don't bother to read
   * it. {@code skipHeaderRows} is used to specify the number of header
   * rows to skip.</li>
   * </ul>
   */
  public abstract static class FlatTextFormatDefn extends BaseFormatDefn
  {
    public static final String LIST_DELIMITER_PROPERTY = "listDelimiter";
    public static final String SKIP_ROWS_PROPERTY = "skipRows";

    public FlatTextFormatDefn(
        final String name,
        final String typeTag,
        final List<PropertyDefn<?>> properties
    )
    {
      super(
          name,
          typeTag,
          CatalogUtils.concatLists(
              Arrays.asList(
                  new StringPropertyDefn(LIST_DELIMITER_PROPERTY),
                  new IntPropertyDefn(SKIP_ROWS_PROPERTY)
              ),
              properties
          )
      );
    }

    protected Map<String, Object> gatherFields(ResolvedTable table)
    {
      Map<String, Object> jsonMap = new HashMap<>();
      jsonMap.put(InputFormat.TYPE_PROPERTY, CsvInputFormat.TYPE_KEY);
      jsonMap.put("listDelimiter", table.property(LIST_DELIMITER_PROPERTY));
       // hasHeaderRow is required, even though we don't infer headers.
      jsonMap.put("hasHeaderRow", false);
      jsonMap.put("findColumnsFromHeader", false);
            // Column list is required. Infer from schema.
      List<String> cols = table.spec().columns()
          .stream()
          .map(col -> col.name())
          .collect(Collectors.toList());
      jsonMap.put("columns", cols);
      Object value = table.property(SKIP_ROWS_PROPERTY);
      jsonMap.put("skipHeaderRows", value == null ? 0 : value);
      return jsonMap;
    }
  }

  public static final String CSV_FORMAT_TYPE = CsvInputFormat.TYPE_KEY;

  public static class CsvFormatDefn extends FlatTextFormatDefn
  {
    public CsvFormatDefn()
    {
      super(
          "CSV",
          CSV_FORMAT_TYPE,
          null
      );
    }

    @Override
    protected Map<String, Object> gatherFields(ResolvedTable table)
    {
      Map<String, Object> jsonMap = super.gatherFields(table);
      jsonMap.put(InputFormat.TYPE_PROPERTY, CsvInputFormat.TYPE_KEY);
      return jsonMap;
    }

    @Override
    public InputFormat convert(ResolvedTable table)
    {
      try {
        return table.jsonMapper().convertValue(gatherFields(table), CsvInputFormat.class);
      }
      catch (Exception e) {
        throw new IAE(e, "Invalid format specification");
      }
    }
  }

  public static final String DELIMITED_FORMAT_TYPE = DelimitedInputFormat.TYPE_KEY;

  public static class DelimitedFormatDefn extends FlatTextFormatDefn
  {
    public static final String DELIMITER_PROPERTY = "delimiter";

    public DelimitedFormatDefn()
    {
      super(
          "Delimited Text",
          DELIMITED_FORMAT_TYPE,
          Collections.singletonList(
              new StringPropertyDefn(DELIMITER_PROPERTY)
          )
      );
    }

    @Override
    protected Map<String, Object> gatherFields(ResolvedTable table)
    {
      Map<String, Object> jsonMap = super.gatherFields(table);
      jsonMap.put(InputFormat.TYPE_PROPERTY, DelimitedInputFormat.TYPE_KEY);
      Object value = table.property(DELIMITER_PROPERTY);
      if (value != null) {
        jsonMap.put("delimiter", value);
      }
      return jsonMap;
    }

    @Override
    public InputFormat convert(ResolvedTable table)
    {
      return table.jsonMapper().convertValue(gatherFields(table), DelimitedInputFormat.class);
    }
  }

  public static final String JSON_FORMAT_TYPE = JsonInputFormat.TYPE_KEY;

  public static class JsonFormatDefn extends BaseFormatDefn
  {
    public static final String KEEP_NULLS_PROPERTY = "keepNulls";

    public JsonFormatDefn()
    {
      super(
          "JSON",
          JSON_FORMAT_TYPE,
          Collections.singletonList(
              new BooleanPropertyDefn(KEEP_NULLS_PROPERTY)
          )
      );
    }

    @Override
    public InputFormat convert(ResolvedTable table)
    {
      // TODO flatten & feature specs
      Map<String, Object> jsonMap = new HashMap<>();
      jsonMap.put(InputFormat.TYPE_PROPERTY, JsonInputFormat.TYPE_KEY);
      jsonMap.put("keepNullColumns", table.property(KEEP_NULLS_PROPERTY));
      return table.jsonMapper().convertValue(jsonMap, JsonInputFormat.class);
    }
  }

  /**
   * Generic format which allows a literal input spec. Allows the user to
   * specify any input format and any options directly as JSON. The
   * drawback is that the user must repeat the columns.
   */
  public static class GenericFormatDefn extends BaseFormatDefn
  {
    public static final String INPUT_FORMAT_SPEC_PROPERTY = "inputFormatSpec";
    public static final String FORMAT_KEY = "generic";

    public GenericFormatDefn()
    {
      super(
          "Generic",
          FORMAT_KEY,
          Collections.singletonList(
              new SimplePropertyDefn<InputFormat>(INPUT_FORMAT_SPEC_PROPERTY, InputFormat.class)
          )
      );
    }

    @Override
    public InputFormat convert(ResolvedTable table)
    {
      Object value = table.property(INPUT_FORMAT_SPEC_PROPERTY);
      if (value == null) {
        throw new ISE(
            "An input format must be provided in the %s property when input type is %s",
            INPUT_FORMAT_SPEC_PROPERTY,
            name()
        );
      }
      return table.jsonMapper().convertValue(value, InputFormat.class);
    }
  }

  public static final InputFormatDefn CSV_FORMAT_DEFN = new CsvFormatDefn();
  public static final InputFormatDefn DELIMITED_FORMAT_DEFN = new DelimitedFormatDefn();
  public static final InputFormatDefn JSON_FORMAT_DEFN = new JsonFormatDefn();
  public static final GenericFormatDefn GENERIC_FORMAT_DEFN = new GenericFormatDefn();
  public static final List<InputFormatDefn> ALL_FORMATS = ImmutableList.of(
      CSV_FORMAT_DEFN,
      DELIMITED_FORMAT_DEFN,
      JSON_FORMAT_DEFN,
      GENERIC_FORMAT_DEFN
  );
}
