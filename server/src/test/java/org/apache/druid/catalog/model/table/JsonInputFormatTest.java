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

import com.google.common.collect.ImmutableMap;
import org.apache.druid.catalog.model.ColumnSpec;
import org.apache.druid.catalog.model.Columns;
import org.apache.druid.catalog.model.ResolvedTable;
import org.apache.druid.catalog.model.TableMetadata;
import org.apache.druid.catalog.model.table.TableFunction.ParameterDefn;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.impl.InlineInputSource;
import org.apache.druid.data.input.impl.JsonInputFormat;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class JsonInputFormatTest extends BaseExternTableTest
{
  @Test
  public void testDefaults()
  {
    TableMetadata table = TableBuilder.external("foo")
        .inputSource(toMap(new InlineInputSource("a\n")))
        .inputFormat(ImmutableMap.of("type", JsonInputFormat.TYPE_KEY))
        .column("a", Columns.STRING)
        .build();
    ResolvedTable resolved = registry.resolve(table.spec());
    resolved.validate();

    InputFormatDefn defn = registry.inputFormatDefnFor(JsonInputFormat.TYPE_KEY);
    InputFormat inputFormat = defn.convertFromTable(new ResolvedExternalTable(resolved));
    JsonInputFormat jsonFormat = (JsonInputFormat) inputFormat;
    assertNull(jsonFormat.getFlattenSpec());
    assertTrue(jsonFormat.getFeatureSpec().isEmpty());
    assertFalse(jsonFormat.isKeepNullColumns());
    assertFalse(jsonFormat.isAssumeNewlineDelimited());
    assertFalse(jsonFormat.isUseJsonNodeReader());
  }

  @Test
  public void testConversion()
  {
    JsonInputFormat format = new JsonInputFormat(null, null, true, true, false);
    TableMetadata table = TableBuilder.external("foo")
        .inputSource(toMap(new InlineInputSource("a\n")))
        .inputFormat(formatToMap(format))
        .column("a", Columns.STRING)
        .column("b", Columns.LONG)
        .build();
    ResolvedTable resolved = registry.resolve(table.spec());
    resolved.validate();

    InputFormatDefn defn = registry.inputFormatDefnFor(JsonInputFormat.TYPE_KEY);
    InputFormat inputFormat = defn.convertFromTable(new ResolvedExternalTable(resolved));
    JsonInputFormat jsonFormat = (JsonInputFormat) inputFormat;
    assertNull(jsonFormat.getFlattenSpec());
    assertTrue(jsonFormat.getFeatureSpec().isEmpty());
    assertTrue(jsonFormat.isKeepNullColumns());
    assertTrue(jsonFormat.isAssumeNewlineDelimited());
    assertFalse(jsonFormat.isUseJsonNodeReader());
  }

  @Test
  public void testFunctionParams()
  {
    InputFormatDefn defn = registry.inputFormatDefnFor(JsonInputFormat.TYPE_KEY);
    List<ParameterDefn> params = defn.parameters();
    assertEquals(0, params.size());
  }

  @Test
  public void testCreateFromArgs()
  {
    Map<String, Object> args = new HashMap<>();
    InputFormatDefn defn = registry.inputFormatDefnFor(JsonInputFormat.TYPE_KEY);
    List<ColumnSpec> columns = Collections.singletonList(new ColumnSpec("a", null, null));
    InputFormat inputFormat = defn.convertFromArgs(args, columns, mapper);
    JsonInputFormat jsonFormat = (JsonInputFormat) inputFormat;
    assertNull(jsonFormat.getFlattenSpec());
    assertTrue(jsonFormat.getFeatureSpec().isEmpty());
    assertFalse(jsonFormat.isKeepNullColumns());
    assertFalse(jsonFormat.isAssumeNewlineDelimited());
    assertFalse(jsonFormat.isUseJsonNodeReader());
  }
}
