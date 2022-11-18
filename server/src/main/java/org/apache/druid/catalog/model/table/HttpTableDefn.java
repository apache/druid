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

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.catalog.model.CatalogUtils;
import org.apache.druid.catalog.model.ModelProperties.StringListPropertyDefn;
import org.apache.druid.catalog.model.ModelProperties.StringPropertyDefn;
import org.apache.druid.catalog.model.ParameterizedDefn;
import org.apache.druid.catalog.model.ResolvedTable;
import org.apache.druid.catalog.model.table.ExternalTableDefn.FormattedExternalTableDefn;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.data.input.impl.HttpInputSource;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.metadata.DefaultPasswordProvider;
import org.apache.druid.metadata.EnvironmentVariablePasswordProvider;
import org.apache.druid.utils.CollectionUtils;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Definition of an input table for an HTTP data source. Provides the same
 * properties as the {@link HttpInputSource}, but as top-level properties
 * that can be mapped to SQL function parameters. Property names are
 * cleaned up for ease-of-use. The HTTP input source has multiple quirks,
 * the conversion method smooths over those quirks for a simpler catalog
 * experience. Provides a parameterized
 * form where the user provides the partial URLs to use for a particular
 * query.
 */
public class HttpTableDefn extends FormattedExternalTableDefn implements ParameterizedDefn
{
  public static final String TABLE_TYPE = HttpInputSource.TYPE_KEY;

  // Catalog properties that map to fields in the HttpInputSource. See
  // that class for the meaning of these properties.

  public static final String URI_TEMPLATE_PROPERTY = "uriTemplate";
  public static final String USER_PROPERTY = "user";
  public static final String PASSWORD_PROPERTY = "password";
  public static final String PASSWORD_ENV_VAR_PROPERTY = "passwordEnvVar";
  public static final String URIS_PROPERTY = "uris";
  public static final String URIS_PARAMETER = "uris";

  public HttpTableDefn()
  {
    super(
        "HTTP input table",
        TABLE_TYPE,
        Arrays.asList(
            new StringListPropertyDefn(URIS_PROPERTY),
            new StringPropertyDefn(USER_PROPERTY),
            new StringPropertyDefn(PASSWORD_PROPERTY),
            new StringPropertyDefn(PASSWORD_ENV_VAR_PROPERTY),
            new StringPropertyDefn(URI_TEMPLATE_PROPERTY)
        ),
        Collections.singletonList(INPUT_COLUMN_DEFN),
        InputFormats.ALL_FORMATS,
        Collections.singletonList(
            new ParameterImpl(URIS_PARAMETER, String.class)
        )
    );
  }

  @Override
  public ResolvedTable mergeParameters(ResolvedTable table, Map<String, Object> values)
  {
    String urisValue = CatalogUtils.safeGet(values, URIS_PARAMETER, String.class);
    List<String> uriValues = CatalogUtils.stringToList(urisValue);
    if (CollectionUtils.isNullOrEmpty(uriValues)) {
      throw new IAE("One or more values are required for parameter %s", URIS_PARAMETER);
    }
    String uriTemplate = table.stringProperty(URI_TEMPLATE_PROPERTY);
    if (Strings.isNullOrEmpty(uriTemplate)) {
      throw new IAE("Property %s must provide a URI template.", URI_TEMPLATE_PROPERTY);
    }
    Pattern p = Pattern.compile("\\{}");
    Matcher m = p.matcher(uriTemplate);
    if (!m.find()) {
      throw new IAE(
          "Value [%s] for property %s must include a '{}' placeholder.",
          uriTemplate,
          URI_TEMPLATE_PROPERTY
      );
    }
    List<String> uris = new ArrayList<>();
    for (String uri : uriValues) {
      uris.add(m.replaceFirst(uri));
    }

    Map<String, Object> revisedProps = new HashMap<>(table.properties());
    revisedProps.remove(URI_TEMPLATE_PROPERTY);
    revisedProps.put("uris", uris);
    return table.withProperties(revisedProps);
  }

  @Override
  protected InputSource convertSource(ResolvedTable table)
  {
    Map<String, Object> jsonMap = new HashMap<>();
    jsonMap.put(InputSource.TYPE_PROPERTY, HttpInputSource.TYPE_KEY);
    jsonMap.put("httpAuthenticationUsername", table.stringProperty(USER_PROPERTY));
    String password = table.stringProperty(PASSWORD_PROPERTY);
    String passwordEnvVar = table.stringProperty(PASSWORD_ENV_VAR_PROPERTY);
    if (password != null && passwordEnvVar != null) {
      throw new ISE(
          "Specify only one of %s or %s",
          PASSWORD_PROPERTY,
          PASSWORD_ENV_VAR_PROPERTY
      );
    }
    if (password != null) {
      jsonMap.put(
          "httpAuthenticationPassword",
          ImmutableMap.of("type", DefaultPasswordProvider.TYPE_KEY, "password", password)
      );
    } else if (passwordEnvVar != null) {
      jsonMap.put(
          "httpAuthenticationPassword",
          ImmutableMap.of("type", EnvironmentVariablePasswordProvider.TYPE_KEY, "variable", passwordEnvVar)
      );
    }
    jsonMap.put("uris", convertUriList(table.stringListProperty(URIS_PROPERTY)));
    return convertObject(table.jsonMapper(), jsonMap, HttpInputSource.class);
  }

  @SuppressWarnings("unchecked")
  public static List<URI> convertUriList(Object value)
  {
    if (value == null) {
      return null;
    }
    List<String> list;
    try {
      list = (List<String>) value;
    }
    catch (ClassCastException e) {
      throw new IAE("Value [%s] must be a list of strings", value);
    }
    List<URI> uris = new ArrayList<>();
    for (String strValue : list) {
      try {
        uris.add(new URI(strValue));
      }
      catch (URISyntaxException e) {
        throw new IAE(StringUtils.format("Argument [%s] is not a valid URI", value));
      }
    }
    return uris;
  }

  @Override
  public void validate(ResolvedTable table)
  {
    super.validate(table);

    // Validate the HTTP properties only if we don't have a template.
    // If we do have a template, then we don't know how to form
    // a valid parameter for that template.
    // TODO: plug in a dummy URL so we can validate other properties.
    if (!table.hasProperty(URI_TEMPLATE_PROPERTY)) {
      convertSource(table);
    }
  }
}
