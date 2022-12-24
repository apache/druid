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
import org.apache.druid.catalog.CatalogTest;
import org.apache.druid.catalog.model.Columns;
import org.apache.druid.catalog.model.ModelProperties.PropertyDefn;
import org.apache.druid.catalog.model.ParameterizedDefn;
import org.apache.druid.catalog.model.PropertyAttributes;
import org.apache.druid.catalog.model.ResolvedTable;
import org.apache.druid.catalog.model.TableDefnRegistry;
import org.apache.druid.catalog.model.table.ExternalTableDefn.FormattedExternalTableDefn;
import org.apache.druid.data.input.impl.CsvInputFormat;
import org.apache.druid.data.input.impl.HttpInputSource;
import org.apache.druid.data.input.impl.HttpInputSourceConfig;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.metadata.DefaultPasswordProvider;
import org.apache.druid.metadata.EnvironmentVariablePasswordProvider;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

@Category(CatalogTest.class)
public class HttpInputTableTest extends BaseExternTableTest
{
  private final HttpTableDefn tableDefn = new HttpTableDefn();
  private final TableBuilder baseBuilder = TableBuilder.of(tableDefn)
      .description("http input")
      .format(InputFormats.CSV_FORMAT_TYPE)
      .column("x", Columns.VARCHAR)
      .column("y", Columns.BIGINT);

  public HttpInputTableTest()
  {
    mapper.setInjectableValues(new InjectableValues.Std().addValue(
        HttpInputSourceConfig.class,
        new HttpInputSourceConfig(HttpInputSourceConfig.DEFAULT_ALLOWED_PROTOCOLS)
    ));
  }

  @Test
  public void testHappyPath()
  {
    ResolvedTable table = baseBuilder.copy()
        .property(HttpTableDefn.USER_PROPERTY, "bob")
        .property(HttpTableDefn.PASSWORD_PROPERTY, "secret")
        .property(HttpTableDefn.URIS_PROPERTY, Collections.singletonList("http://foo.com/my.csv"))
        .buildResolved(mapper);

    // Check validation
    table.validate();

    // Check registry
    TableDefnRegistry registry = new TableDefnRegistry(mapper);
    assertNotNull(registry.resolve(table.spec()));

    // Convert to an external spec
    ExternalTableSpec externSpec = tableDefn.convertToExtern(table);

    HttpInputSource sourceSpec = (HttpInputSource) externSpec.inputSource;
    assertEquals("bob", sourceSpec.getHttpAuthenticationUsername());
    assertEquals("secret", ((DefaultPasswordProvider) sourceSpec.getHttpAuthenticationPasswordProvider()).getPassword());
    assertEquals("http://foo.com/my.csv", sourceSpec.getUris().get(0).toString());

    // Just a sanity check: details of CSV conversion are tested elsewhere.
    CsvInputFormat csvFormat = (CsvInputFormat) externSpec.inputFormat;
    assertEquals(Arrays.asList("x", "y"), csvFormat.getColumns());

    RowSignature sig = externSpec.signature;
    assertEquals(Arrays.asList("x", "y"), sig.getColumnNames());
    assertEquals(ColumnType.STRING, sig.getColumnType(0).get());
    assertEquals(ColumnType.LONG, sig.getColumnType(1).get());
  }

  @Test
  public void testEnvPassword()
  {
    ResolvedTable table = baseBuilder.copy()
        .property(HttpTableDefn.USER_PROPERTY, "bob")
        .property(HttpTableDefn.PASSWORD_ENV_VAR_PROPERTY, "SECRET")
        .property(HttpTableDefn.URIS_PROPERTY, Collections.singletonList("http://foo.com/my.csv"))
        .buildResolved(mapper);

    // Check validation
    table.validate();

    // Convert to an external spec
    ExternalTableSpec externSpec = tableDefn.convertToExtern(table);

    HttpInputSource sourceSpec = (HttpInputSource) externSpec.inputSource;
    assertEquals("bob", sourceSpec.getHttpAuthenticationUsername());
    assertEquals("SECRET", ((EnvironmentVariablePasswordProvider) sourceSpec.getHttpAuthenticationPasswordProvider()).getVariable());
  }

  @Test
  public void testParameters()
  {
    ResolvedTable table = baseBuilder.copy()
        .property(HttpTableDefn.USER_PROPERTY, "bob")
        .property(HttpTableDefn.PASSWORD_ENV_VAR_PROPERTY, "SECRET")
        .property(HttpTableDefn.URI_TEMPLATE_PROPERTY, "http://foo.com/{}")
        .buildResolved(mapper);

    // Check validation
    table.validate();

    // Parameters
    ParameterizedDefn parameterizedTable = tableDefn;
    assertEquals(1, parameterizedTable.parameters().size());
    assertNotNull(findProperty(parameterizedTable.parameters(), HttpTableDefn.URIS_PROPERTY));

    // Apply parameters
    Map<String, Object> params = ImmutableMap.of(
        HttpTableDefn.URIS_PROPERTY, "foo.csv,bar.csv"
    );

    // Convert to an external spec
    ExternalTableSpec externSpec = parameterizedTable.applyParameters(table, params);

    HttpInputSource sourceSpec = (HttpInputSource) externSpec.inputSource;
    assertEquals("bob", sourceSpec.getHttpAuthenticationUsername());
    assertEquals("SECRET", ((EnvironmentVariablePasswordProvider) sourceSpec.getHttpAuthenticationPasswordProvider()).getVariable());
    assertEquals(
        HttpTableDefn.convertUriList(Arrays.asList("http://foo.com/foo.csv", "http://foo.com/bar.csv")),
        sourceSpec.getUris()
    );
  }

  @Test
  public void testNoTemplate()
  {
    ResolvedTable table = baseBuilder.copy()
        .property(HttpTableDefn.URIS_PROPERTY, Collections.singletonList("http://foo.com/my.csv"))
        .buildResolved(mapper);

    // Check validation
    table.validate();

    // Apply parameters
    Map<String, Object> params = ImmutableMap.of(
        HttpTableDefn.URIS_PROPERTY, "foo.csv,bar.csv"
    );

    // Convert to an external spec
    assertThrows(IAE.class, () -> tableDefn.applyParameters(table, params));
  }

  @Test
  public void testNoParameters()
  {
    ResolvedTable table = baseBuilder.copy()
        .property(HttpTableDefn.URI_TEMPLATE_PROPERTY, "http://foo.com/{}")
        .buildResolved(mapper);

    Map<String, Object> params = ImmutableMap.of();
    assertThrows(IAE.class, () -> tableDefn.applyParameters(table, params));
  }

  @Test
  public void testInvalidParameters()
  {
    // The URI parser is forgiving about items in the path, so
    // screw up the head, where URI is particular.
    ResolvedTable table = baseBuilder.copy()
        .property(HttpTableDefn.URI_TEMPLATE_PROPERTY, "//foo.com/{}")
        .buildResolved(mapper);

    Map<String, Object> params = ImmutableMap.of(
        HttpTableDefn.URIS_PROPERTY, "foo.csv"
    );
    assertThrows(IAE.class, () -> tableDefn.applyParameters(table, params));
  }

  @Test
  public void testInvalidURI()
  {
    // The URI parser is forgiving about items in the path, so
    // screw up the head, where URI is particular.
    ResolvedTable table = baseBuilder.copy()
        .property(HttpTableDefn.URIS_PROPERTY, Collections.singletonList("//foo.com"))
        .buildResolved(mapper);

    assertThrows(IAE.class, () -> table.validate());
  }

  @Test
  public void testSqlFunction()
  {
    List<PropertyDefn<?>> params = tableDefn.tableFunctionParameters();

    // Ensure the relevant properties are available as SQL function parameters
    PropertyDefn<?> userProp = findProperty(params, HttpTableDefn.USER_PROPERTY);
    assertNotNull(userProp);
    assertEquals(String.class, PropertyAttributes.sqlParameterType(userProp));

    PropertyDefn<?> pwdProp = findProperty(params, HttpTableDefn.PASSWORD_PROPERTY);
    assertNotNull(pwdProp);
    assertEquals(String.class, PropertyAttributes.sqlParameterType(pwdProp));

    PropertyDefn<?> urisProp = findProperty(params, HttpTableDefn.URIS_PROPERTY);
    assertNotNull(urisProp);
    assertEquals(String.class, PropertyAttributes.sqlParameterType(urisProp));

    assertNull(findProperty(params, HttpTableDefn.URI_TEMPLATE_PROPERTY));

    PropertyDefn<?> formatProp = findProperty(params, FormattedExternalTableDefn.FORMAT_PROPERTY);
    assertNotNull(formatProp);
    assertEquals(String.class, PropertyAttributes.sqlParameterType(formatProp));

    // Pretend to accept values for the SQL parameters.
    final ResolvedTable table = TableBuilder.of(tableDefn)
        .property(userProp.name(), userProp.decodeSqlValue("bob", mapper))
        .property(pwdProp.name(), pwdProp.decodeSqlValue("secret", mapper))
        .property(urisProp.name(), urisProp.decodeSqlValue("http://foo.com/foo.csv, http://foo.com/bar.csv", mapper))
        .property(formatProp.name(), formatProp.decodeSqlValue(InputFormats.CSV_FORMAT_TYPE, mapper))
        .column("x", Columns.VARCHAR)
        .column("y", Columns.BIGINT)
        .buildResolved(mapper);

    ExternalTableSpec externSpec = tableDefn.convertToExtern(table);
    HttpInputSource sourceSpec = (HttpInputSource) externSpec.inputSource;
    assertEquals("bob", sourceSpec.getHttpAuthenticationUsername());
    assertEquals("secret", ((DefaultPasswordProvider) sourceSpec.getHttpAuthenticationPasswordProvider()).getPassword());
    assertEquals(
        HttpTableDefn.convertUriList(Arrays.asList("http://foo.com/foo.csv", "http://foo.com/bar.csv")),
        sourceSpec.getUris()
    );
  }
}
