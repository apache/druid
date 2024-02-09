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

import com.fasterxml.jackson.databind.InjectableValues;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.catalog.model.CatalogUtils;
import org.apache.druid.catalog.model.Columns;
import org.apache.druid.catalog.model.ResolvedTable;
import org.apache.druid.catalog.model.TableMetadata;
import org.apache.druid.catalog.model.table.InputFormats.CsvFormatDefn;
import org.apache.druid.data.input.impl.CsvInputFormat;
import org.apache.druid.data.input.impl.HttpInputSource;
import org.apache.druid.data.input.impl.HttpInputSourceConfig;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.metadata.DefaultPasswordProvider;
import org.apache.druid.metadata.EnvironmentVariablePasswordProvider;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class HttpInputSourceDefnTest extends BaseExternTableTest
{
  @Before
  public void setup()
  {
    mapper.setInjectableValues(new InjectableValues.Std().addValue(
        HttpInputSourceConfig.class,
        new HttpInputSourceConfig(HttpInputSourceConfig.DEFAULT_ALLOWED_PROTOCOLS)
    ));
  }

  @Test
  public void testEmptyInputSource()
  {
    // No URIs property or template: not valid. Need at least a URI or template
    TableMetadata table = TableBuilder.external("foo")
        .inputSource(ImmutableMap.of("type", HttpInputSource.TYPE_KEY))
        .inputFormat(CSV_FORMAT)
        .column("x", Columns.STRING)
        .column("y", Columns.LONG)
        .build();
    ResolvedTable resolved = registry.resolve(table.spec());
    assertThrows(IAE.class, () -> resolved.validate());
  }

  @Test
  public void testInvalidTemplate()
  {
    // No format: valid. Format can be provided via a table function
    TableMetadata table = TableBuilder.external("foo")
        .inputSource(ImmutableMap.of("type", HttpInputSource.TYPE_KEY))
        .property(HttpInputSourceDefn.URI_TEMPLATE_PROPERTY, "http://example.com/")
        .inputFormat(CSV_FORMAT)
        .column("x", Columns.STRING)
        .column("y", Columns.LONG)
        .build();
    ResolvedTable resolved = registry.resolve(table.spec());
    assertThrows(IAE.class, () -> resolved.validate());
  }

  @Test
  public void testNoFormatWithURI() throws URISyntaxException
  {
    // No format: not valid if URI is provided
    HttpInputSource inputSource = new HttpInputSource(
        Collections.singletonList(new URI("http://example.com/file.csv")),
        null,
        null,
        null,
        new HttpInputSourceConfig(null)
    );
    TableMetadata table = TableBuilder.external("foo")
        .inputSource(toMap(inputSource))
        .column("x", Columns.STRING)
        .column("y", Columns.LONG)
        .build();
    ResolvedTable resolved = registry.resolve(table.spec());
    assertThrows(IAE.class, () -> resolved.validate());
  }

  @Test
  public void testNoColumnsWithUri() throws URISyntaxException
  {
    // No format: not valid if URI is provided
    HttpInputSource inputSource = new HttpInputSource(
        Collections.singletonList(new URI("http://example.com/file.csv")),
        null,
        null,
        null,
        new HttpInputSourceConfig(null)
    );
    TableMetadata table = TableBuilder.external("foo")
        .inputSource(toMap(inputSource))
        .inputFormat(CSV_FORMAT)
        .build();
    ResolvedTable resolved = registry.resolve(table.spec());
    assertThrows(IAE.class, () -> resolved.validate());
  }

  @Test
  public void testNoFormatWithTemplate()
  {
    // No format: valid. Format can be provided via a table function
    TableMetadata table = TableBuilder.external("foo")
        .inputSource(ImmutableMap.of("type", HttpInputSource.TYPE_KEY))
        .property(HttpInputSourceDefn.URI_TEMPLATE_PROPERTY, "http://example.com/{}")
        .build();
    ResolvedTable resolved = registry.resolve(table.spec());
    resolved.validate();
  }

  @Test
  public void testURIAndTemplate() throws URISyntaxException
  {
    // No format: not valid if URI is provided
    HttpInputSource inputSource = new HttpInputSource(
        Collections.singletonList(new URI("http://example.com/file.csv")),
        null,
        null,
        null,
        new HttpInputSourceConfig(null)
    );
    TableMetadata table = TableBuilder.external("foo")
        .inputSource(toMap(inputSource))
        .inputFormat(CSV_FORMAT)
        .property(HttpInputSourceDefn.URI_TEMPLATE_PROPERTY, "http://example.com/{}")
        .build();
    ResolvedTable resolved = registry.resolve(table.spec());
    assertThrows(IAE.class, () -> resolved.validate());
  }

  @Test
  public void testAdHocBadFormat()
  {
    InputSourceDefn httpDefn = registry.inputSourceDefnFor(HttpInputSourceDefn.TYPE_KEY);

    TableFunction fn = httpDefn.adHocTableFn();
    Map<String, Object> args = new HashMap<>();
    args.put(HttpInputSourceDefn.URIS_PARAMETER, new String[] {"http://foo.com/my.csv"});
    args.put(FormattedInputSourceDefn.FORMAT_PARAMETER, "bogus");
    assertThrows(IAE.class, () -> fn.apply("x", args, COLUMNS, mapper));
  }

  @Test
  public void testAdHocBadUri()
  {
    InputSourceDefn httpDefn = registry.inputSourceDefnFor(HttpInputSourceDefn.TYPE_KEY);

    TableFunction fn = httpDefn.adHocTableFn();
    Map<String, Object> args = new HashMap<>();
    args.put(HttpInputSourceDefn.URIS_PARAMETER, new String[] {"bogus"});
    args.put(FormattedInputSourceDefn.FORMAT_PARAMETER, CsvFormatDefn.TYPE_KEY);
    assertThrows(IAE.class, () -> fn.apply("x", args, COLUMNS, mapper));
  }

  @Test
  public void testAdHocHappyPath()
  {
    InputSourceDefn httpDefn = registry.inputSourceDefnFor(HttpInputSourceDefn.TYPE_KEY);

    TableFunction fn = httpDefn.adHocTableFn();
    assertTrue(hasParam(fn, HttpInputSourceDefn.URIS_PARAMETER));
    assertTrue(hasParam(fn, FormattedInputSourceDefn.FORMAT_PARAMETER));
    assertTrue(hasParam(fn, HttpInputSourceDefn.USER_PARAMETER));

    // Convert to an external table. Must provide the URIs plus format and columns.
    Map<String, Object> args = new HashMap<>();
    args.put(HttpInputSourceDefn.URIS_PARAMETER, Collections.singletonList("http://foo.com/my.csv"));
    args.put(HttpInputSourceDefn.USER_PARAMETER, "bob");
    args.put(HttpInputSourceDefn.PASSWORD_PARAMETER, "secret");
    args.put(FormattedInputSourceDefn.FORMAT_PARAMETER, CsvFormatDefn.TYPE_KEY);
    ExternalTableSpec externSpec = fn.apply("x", args, COLUMNS, mapper);
    validateHappyPath(externSpec, true);

    // But, it fails if there are no columns.
    assertThrows(IAE.class, () -> fn.apply("x", args, Collections.emptyList(), mapper));
  }

  @Test
  public void testFullTableSpecHappyPath() throws URISyntaxException
  {
    HttpInputSource inputSource = new HttpInputSource(
        Collections.singletonList(new URI("http://foo.com/my.csv")),
        "bob",
        new DefaultPasswordProvider("secret"),
        null,
        new HttpInputSourceConfig(null)
    );
    TableMetadata table = TableBuilder.external("foo")
        .inputSource(toMap(inputSource))
        .inputFormat(CSV_FORMAT)
        .column("x", Columns.STRING)
        .column("y", Columns.LONG)
        .build();

    // Check validation
    table.validate();

    // Check registry
    ResolvedTable resolved = registry.resolve(table.spec());
    assertNotNull(resolved);

    // Convert to an external spec
    ExternalTableDefn externDefn = (ExternalTableDefn) resolved.defn();
    ExternalTableSpec externSpec = externDefn.convert(resolved);
    validateHappyPath(externSpec, true);

    // Get the partial table function
    TableFunction fn = externDefn.tableFn(resolved);
    assertTrue(fn.parameters().isEmpty());

    // Convert to an external table.
    externSpec = fn.apply("x", Collections.emptyMap(), Collections.emptyList(), mapper);
    validateHappyPath(externSpec, true);

    // But, it fails columns are provided since the table already has them.
    assertThrows(IAE.class, () -> fn.apply("x", Collections.emptyMap(), COLUMNS, mapper));
  }

  @Test
  public void testTemplateSpecWithFormatHappyPath()
  {
    TableMetadata table = TableBuilder.external("foo")
        .inputSource(ImmutableMap.of("type", HttpInputSource.TYPE_KEY))
        .inputFormat(CSV_FORMAT)
        .property(HttpInputSourceDefn.URI_TEMPLATE_PROPERTY, "http://foo.com/{}")
        .column("x", Columns.STRING)
        .column("y", Columns.LONG)
        .build();

    // Check validation
    table.validate();
    ResolvedTable resolved = registry.resolve(table.spec());
    assertNotNull(resolved);

    // Not a full table, can't directly convert
    // Convert to an external spec
    ExternalTableDefn externDefn = (ExternalTableDefn) resolved.defn();
    assertThrows(IAE.class, () -> externDefn.convert(resolved));

    // Get the partial table function
    TableFunction fn = externDefn.tableFn(resolved);
    assertEquals(4, fn.parameters().size());
    assertTrue(hasParam(fn, HttpInputSourceDefn.URIS_PARAMETER));
    assertTrue(hasParam(fn, HttpInputSourceDefn.USER_PARAMETER));
    assertTrue(hasParam(fn, HttpInputSourceDefn.PASSWORD_PARAMETER));
    assertTrue(hasParam(fn, HttpInputSourceDefn.PASSWORD_ENV_VAR_PARAMETER));

    // Convert to an external table.
    ExternalTableSpec externSpec = fn.apply(
        "x",
        ImmutableMap.of(
            HttpInputSourceDefn.URIS_PARAMETER,
            Collections.singletonList("my.csv")
        ),
        Collections.emptyList(),
        mapper
    );
    validateHappyPath(externSpec, false);
  }

  @Test
  public void testTemplateSpecWithFormatAndPassword()
  {
    TableMetadata table = TableBuilder.external("foo")
        .inputSource(ImmutableMap.of(
            "type", HttpInputSource.TYPE_KEY,
            HttpInputSourceDefn.USERNAME_FIELD, "bob",
            HttpInputSourceDefn.PASSWORD_FIELD, ImmutableMap.of(
                "type", "default",
                "password", "secret"
            )
         ))
        .inputFormat(CSV_FORMAT)
        .property(HttpInputSourceDefn.URI_TEMPLATE_PROPERTY, "http://foo.com/{}")
        .column("x", Columns.STRING)
        .column("y", Columns.LONG)
        .build();

    table.validate();
    ResolvedTable resolved = registry.resolve(table.spec());
    assertNotNull(resolved);

    // Not a full table, can't directly convert
    // Convert to an external spec
    ExternalTableDefn externDefn = (ExternalTableDefn) resolved.defn();
    assertThrows(IAE.class, () -> externDefn.convert(resolved));

    // Get the partial table function
    TableFunction fn = externDefn.tableFn(resolved);
    assertEquals(1, fn.parameters().size());
    assertTrue(hasParam(fn, HttpInputSourceDefn.URIS_PARAMETER));

    // Convert to an external table.
    ExternalTableSpec externSpec = fn.apply(
        "x",
        ImmutableMap.of(
            HttpInputSourceDefn.URIS_PARAMETER,
            Collections.singletonList("my.csv")
        ),
        Collections.emptyList(),
        mapper
    );
    validateHappyPath(externSpec, true);
  }

  @Test
  public void testTemplateSpecWithoutFormatHappyPath() throws URISyntaxException
  {
    HttpInputSource inputSource = new HttpInputSource(
        Collections.singletonList(new URI("http://foo.com/my.csv")), // removed
        "bob",
        new DefaultPasswordProvider("secret"),
        null,
        new HttpInputSourceConfig(null)
    );
    TableMetadata table = TableBuilder.external("foo")
        .inputSource(httpToMap(inputSource))
        .property(HttpInputSourceDefn.URI_TEMPLATE_PROPERTY, "http://foo.com/{}")
        .build();

    // Check validation
    table.validate();

    // Not a full table, can't directly convert
    // Convert to an external spec
    ResolvedTable resolved = registry.resolve(table.spec());
    ExternalTableDefn externDefn = (ExternalTableDefn) resolved.defn();
    assertThrows(IAE.class, () -> externDefn.convert(resolved));

    // Get the partial table function
    TableFunction fn = externDefn.tableFn(resolved);
    assertTrue(hasParam(fn, HttpInputSourceDefn.URIS_PARAMETER));
    assertTrue(hasParam(fn, FormattedInputSourceDefn.FORMAT_PARAMETER));
    assertFalse(hasParam(fn, HttpInputSourceDefn.USER_PARAMETER));

    // Convert to an external table. Must provide the URIs plus format and columns.
    Map<String, Object> args = new HashMap<>();
    args.put(HttpInputSourceDefn.URIS_PARAMETER, Collections.singletonList("my.csv"));
    args.put(FormattedInputSourceDefn.FORMAT_PARAMETER, CsvFormatDefn.TYPE_KEY);
    ExternalTableSpec externSpec = fn.apply("x", args, COLUMNS, mapper);
    validateHappyPath(externSpec, true);
  }

  @Test
  public void testMultipleURIsInTableSpec() throws URISyntaxException
  {
    HttpInputSource inputSource = new HttpInputSource(
        Arrays.asList(new URI("http://foo.com/foo.csv"), new URI("http://foo.com/bar.csv")),
        "bob",
        new EnvironmentVariablePasswordProvider("SECRET"),
        null,
        new HttpInputSourceConfig(null)
    );
    TableMetadata table = TableBuilder.external("foo")
        .inputSource(toMap(inputSource))
        .inputFormat(CSV_FORMAT)
        .column("x", Columns.STRING)
        .column("y", Columns.LONG)
        .build();

    // Check validation
    table.validate();

    // Convert to an external spec
    ResolvedTable resolved = registry.resolve(table.spec());
    ExternalTableDefn externDefn = (ExternalTableDefn) resolved.defn();
    ExternalTableSpec externSpec = externDefn.convert(resolved);

    HttpInputSource sourceSpec = (HttpInputSource) externSpec.inputSource;
    assertEquals(
        CatalogUtils.stringListToUriList(Arrays.asList("http://foo.com/foo.csv", "http://foo.com/bar.csv")),
        sourceSpec.getUris()
    );
    assertEquals(Collections.singleton(HttpInputSourceDefn.TYPE_KEY), externSpec.inputSourceTypesSupplier.get());
  }

  @Test
  public void testMultipleURIsWithTemplate() throws URISyntaxException
  {
    HttpInputSource inputSource = new HttpInputSource(
        Collections.singletonList(new URI("http://foo.com/my.csv")), // removed
        "bob",
        new DefaultPasswordProvider("secret"),
        null,
        new HttpInputSourceConfig(null)
    );
    TableMetadata table = TableBuilder.external("foo")
        .inputSource(httpToMap(inputSource))
        .inputFormat(CSV_FORMAT)
        .property(HttpInputSourceDefn.URI_TEMPLATE_PROPERTY, "http://foo.com/{}")
        .column("x", Columns.STRING)
        .column("y", Columns.LONG)
        .build();

    // Check validation
    table.validate();

    // Get the partial table function
    ResolvedTable resolved = registry.resolve(table.spec());
    ExternalTableDefn externDefn = (ExternalTableDefn) resolved.defn();
    TableFunction fn = externDefn.tableFn(resolved);
    assertEquals(1, fn.parameters().size());
    assertTrue(hasParam(fn, HttpInputSourceDefn.URIS_PARAMETER));

    // Convert to an external table.
    ExternalTableSpec externSpec = fn.apply(
        "x",
        ImmutableMap.of(
            HttpInputSourceDefn.URIS_PARAMETER, Arrays.asList("my.csv", "bar.csv")),
        Collections.emptyList(),
        mapper
    );

    HttpInputSource sourceSpec = (HttpInputSource) externSpec.inputSource;
    assertEquals(
        CatalogUtils.stringListToUriList(Arrays.asList("http://foo.com/my.csv", "http://foo.com/bar.csv")),
        sourceSpec.getUris()
    );
    assertEquals(Collections.singleton(HttpInputSourceDefn.TYPE_KEY), externSpec.inputSourceTypesSupplier.get());
  }

  @Test
  public void testMultipleURIsAdHoc()
  {
    InputSourceDefn httpDefn = registry.inputSourceDefnFor(HttpInputSourceDefn.TYPE_KEY);

    TableFunction fn = httpDefn.adHocTableFn();
    assertTrue(hasParam(fn, HttpInputSourceDefn.URIS_PARAMETER));
    assertTrue(hasParam(fn, FormattedInputSourceDefn.FORMAT_PARAMETER));
    assertTrue(hasParam(fn, HttpInputSourceDefn.USER_PARAMETER));

    // Convert to an external table. Must provide the URIs plus format and columns.
    Map<String, Object> args = new HashMap<>();
    args.put(HttpInputSourceDefn.URIS_PARAMETER, Arrays.asList("http://foo.com/foo.csv", "http://foo.com/bar.csv"));
    args.put(FormattedInputSourceDefn.FORMAT_PARAMETER, CsvFormatDefn.TYPE_KEY);
    ExternalTableSpec externSpec = fn.apply("x", args, COLUMNS, mapper);

    HttpInputSource sourceSpec = (HttpInputSource) externSpec.inputSource;
    assertEquals(
        CatalogUtils.stringListToUriList(Arrays.asList("http://foo.com/foo.csv", "http://foo.com/bar.csv")),
        sourceSpec.getUris()
    );
    assertEquals(Collections.singleton(HttpInputSourceDefn.TYPE_KEY), externSpec.inputSourceTypesSupplier.get());
  }

  @Test
  public void testEnvPassword() throws URISyntaxException
  {
    HttpInputSource inputSource = new HttpInputSource(
        Collections.singletonList(new URI("http://foo.com/my.csv")),
        "bob",
        new EnvironmentVariablePasswordProvider("SECRET"),
        null,
        new HttpInputSourceConfig(null)
    );
    TableMetadata table = TableBuilder.external("foo")
        .inputSource(toMap(inputSource))
        .inputFormat(CSV_FORMAT)
        .column("x", Columns.STRING)
        .column("y", Columns.LONG)
        .build();

    // Check validation
    table.validate();

    // Convert to an external spec
    ResolvedTable resolved = registry.resolve(table.spec());
    ExternalTableDefn externDefn = (ExternalTableDefn) resolved.defn();
    ExternalTableSpec externSpec = externDefn.convert(resolved);

    HttpInputSource sourceSpec = (HttpInputSource) externSpec.inputSource;
    assertEquals("bob", sourceSpec.getHttpAuthenticationUsername());
    assertEquals(
        "SECRET",
        ((EnvironmentVariablePasswordProvider) sourceSpec.getHttpAuthenticationPasswordProvider()).getVariable()
    );
    assertEquals(Collections.singleton(HttpInputSourceDefn.TYPE_KEY), externSpec.inputSourceTypesSupplier.get());
  }

  private void validateHappyPath(ExternalTableSpec externSpec, boolean withUser)
  {
    HttpInputSource sourceSpec = (HttpInputSource) externSpec.inputSource;
    if (withUser) {
      assertEquals("bob", sourceSpec.getHttpAuthenticationUsername());
      assertEquals("secret", ((DefaultPasswordProvider) sourceSpec.getHttpAuthenticationPasswordProvider()).getPassword());
    }
    assertEquals("http://foo.com/my.csv", sourceSpec.getUris().get(0).toString());

    // Just a sanity check: details of CSV conversion are tested elsewhere.
    CsvInputFormat csvFormat = (CsvInputFormat) externSpec.inputFormat;
    assertEquals(Arrays.asList("x", "y"), csvFormat.getColumns());

    RowSignature sig = externSpec.signature;
    assertEquals(Arrays.asList("x", "y"), sig.getColumnNames());
    assertEquals(ColumnType.STRING, sig.getColumnType(0).get());
    assertEquals(ColumnType.LONG, sig.getColumnType(1).get());
    assertEquals(Collections.singleton(HttpInputSourceDefn.TYPE_KEY), externSpec.inputSourceTypesSupplier.get());
  }

  private Map<String, Object> httpToMap(HttpInputSource source)
  {
    Map<String, Object> sourceMap = toMap(source);
    sourceMap.remove("uris");
    return sourceMap;
  }
}
