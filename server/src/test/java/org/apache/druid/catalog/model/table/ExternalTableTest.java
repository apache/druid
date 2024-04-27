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
import org.apache.druid.catalog.model.Columns;
import org.apache.druid.catalog.model.ResolvedTable;
import org.apache.druid.catalog.model.TableDefnRegistry;
import org.apache.druid.catalog.model.TableMetadata;
import org.apache.druid.data.input.impl.CsvInputFormat;
import org.apache.druid.data.input.impl.HttpInputSource;
import org.apache.druid.data.input.impl.HttpInputSourceConfig;
import org.apache.druid.data.input.impl.InlineInputSource;
import org.apache.druid.data.input.impl.JsonInputFormat;
import org.apache.druid.data.input.impl.LocalInputSource;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.metadata.DefaultPasswordProvider;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.Map;

import static org.junit.Assert.assertThrows;

public class ExternalTableTest extends BaseExternTableTest
{
  private static final Logger LOG = new Logger(ExternalTableTest.class);

  private final TableDefnRegistry registry = new TableDefnRegistry(mapper);

  @Test
  public void testValidateEmptyTable()
  {
    // Empty table: not valid
    TableMetadata table = TableBuilder.external("foo").build();
    ResolvedTable resolved = registry.resolve(table.spec());
    assertThrows(IAE.class, () -> resolved.validate());
  }

  @Test
  public void testValidateMissingSourceType()
  {
    // Empty table: not valid
    TableMetadata table = TableBuilder.external("foo")
        .inputSource(ImmutableMap.of())
        .build();
    ResolvedTable resolved = registry.resolve(table.spec());
    assertThrows(IAE.class, () -> resolved.validate());
  }

  @Test
  public void testValidateUnknownSourceType()
  {
    // Empty table: not valid
    TableMetadata table = TableBuilder.external("foo")
        .inputSource(ImmutableMap.of("type", "unknown"))
        .build();
    ResolvedTable resolved = registry.resolve(table.spec());
    assertThrows(IAE.class, () -> resolved.validate());
  }

  @Test
  public void testValidateSourceOnly()
  {
    // Input source only: valid, assumes the format is given later
    LocalInputSource inputSource = new LocalInputSource(new File("/tmp"), "*");
    TableMetadata table = TableBuilder.external("foo")
        .inputSource(toMap(inputSource))
        .build();
    ResolvedTable resolved = registry.resolve(table.spec());
    resolved.validate();
  }

  @Test
  public void testValidateMissingFormatType()
  {
    // Input source only: valid, assumes the format is given later
    TableMetadata table = TableBuilder.external("foo")
        .inputSource(toMap(new InlineInputSource("a\n")))
        .inputFormat(ImmutableMap.of())
        .build();
    ResolvedTable resolved = registry.resolve(table.spec());
    assertThrows(IAE.class, () -> resolved.validate());
  }

  @Test
  public void testValidateUnknownFormatType()
  {
    // Input source only: valid, assumes the format is given later
    TableMetadata table = TableBuilder.external("foo")
        .inputSource(toMap(new InlineInputSource("a\n")))
        .inputFormat(ImmutableMap.of("type", "unknown"))
        .build();
    ResolvedTable resolved = registry.resolve(table.spec());
    assertThrows(IAE.class, () -> resolved.validate());
  }

  @Test
  public void testValidateSourceAndFormat()
  {
    // Format is given without columns: it is validated
    CsvInputFormat format = new CsvInputFormat(
        Collections.singletonList("a"), ";", false, false, 0);
    TableMetadata table = TableBuilder.external("foo")
        .inputSource(toMap(new InlineInputSource("a\n")))
        .inputFormat(formatToMap(format))
        .column("a", Columns.STRING)
        .build();
    ResolvedTable resolved = registry.resolve(table.spec());
    resolved.validate();
  }

  /**
   * Test case for multiple of the {@code ext.md} examples. To use this, enable the
   * test, run it, then copy the JSON from the console. The examples pull out bits
   * and pieces in multiple places.
   */
  @Test
  @Ignore
  public void wikipediaDocExample()
  {
    JsonInputFormat format = new JsonInputFormat(null, null, true, true, false);
    LocalInputSource inputSource = new LocalInputSource(new File("/Users/bob/druid/quickstart/tutorial"), "wikiticker-*-sampled.json");
    TableMetadata table = TableBuilder.external("foo")
        .inputSource(toMap(inputSource))
        .inputFormat(formatToMap(format))
        .description("Sample Wikipedia data")
        .column("timetamp", Columns.STRING)
        .column("page", Columns.STRING)
        .column("language", Columns.STRING)
        .column("unpatrolled", Columns.STRING)
        .column("newPage", Columns.STRING)
        .column("robot", Columns.STRING)
        .column("added", Columns.STRING)
        .column("namespace", Columns.LONG)
        .column("deleted", Columns.LONG)
        .column("delta", Columns.LONG)
        .build();
    LOG.info(table.spec().toString());
  }

  @Test
  @Ignore
  public void httpDocExample() throws URISyntaxException
  {
    HttpInputSource inputSource = new HttpInputSource(
        Collections.singletonList(new URI("https://example.com/my.csv")), // removed
        "bob",
        new DefaultPasswordProvider("secret"),
        null,
        new HttpInputSourceConfig(null)
    );
    Map<String, Object> sourceMap = toMap(inputSource);
    sourceMap.remove("uris");
    TableMetadata table = TableBuilder.external("koala")
        .inputSource(sourceMap)
        .inputFormat(CSV_FORMAT)
        .property(HttpInputSourceDefn.URI_TEMPLATE_PROPERTY, "https://example.com/{}")
        .description("Example parameterized external table")
        .column("timetamp", Columns.STRING)
        .column("metric", Columns.STRING)
        .column("value", Columns.LONG)
        .build();
    LOG.info(table.spec().toString());
  }

  @Test
  @Ignore
  public void httpConnDocExample() throws URISyntaxException
  {
    HttpInputSource inputSource = new HttpInputSource(
        Collections.singletonList(new URI("https://example.com/")),
        "bob",
        new DefaultPasswordProvider("secret"),
        null,
        new HttpInputSourceConfig(null)
    );
    TableMetadata table = TableBuilder.external("koala")
        .inputSource(toMap(inputSource))
        .description("Example connection")
        .build();
    LOG.info(table.spec().toString());
  }

  @Test
  @Ignore
  public void localDocExample()
  {
    Map<String, Object> sourceMap = ImmutableMap.of(
        "type", LocalInputSource.TYPE_KEY,
        "baseDir", "/var/data"
    );
    TableMetadata table = TableBuilder.external("koala")
        .inputSource(sourceMap)
        .build();
    LOG.info(table.spec().toString());
  }
}
