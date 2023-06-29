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
import org.apache.druid.catalog.model.CatalogUtils;
import org.apache.druid.catalog.model.ColumnSpec;
import org.apache.druid.catalog.model.table.BaseTableFunction.Parameter;
import org.apache.druid.catalog.model.table.TableFunction.ParameterDefn;
import org.apache.druid.catalog.model.table.TableFunction.ParameterType;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.data.input.impl.HttpInputSource;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.metadata.DefaultPasswordProvider;
import org.apache.druid.metadata.EnvironmentVariablePasswordProvider;
import org.apache.druid.utils.CollectionUtils;

import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Definition of an HTTP input source source.
 * <p>
 * Provides a parameterized form where the user defines a value for the
 * {@code uriTemplate} table property in the table spec, then provides the partial URLs
 * in a table function to use for that one query. The final URIs are created by combining
 * the template and the arguments. Example:
 * <li>{@code uriTemplate} property): "http://example.com/data/kttm-{}.json"</li>
 * <li>{@code uris} function argument: "22-Nov-21, 22-Nov-22"</li>
 * </ul>
 * <p>
 * When the template is used, the format is optional: it can be provided either with
 * the table spec or at runtime, depending on what the user wants to accomplish. In the
 * above, where the ".json" is encoded in the template, it makes sense to include the format
 * with the spec. If the template was "http://example.com/data/{}", and the data comes in
 * multiple formats, it might make sense to specify the format in the query. In this case,
 * the table spec acts more like a connection.
 * <p>
 * If the template is not used, then the {@code uris} property must be provided in the
 * table spec, along with the corresponding format.
 * <p>
 * The above semantics make a bit more sense when we realize that the spec can also
 * provide a user name and password. When those are provided, then the input source must
 * name a single site: the one for which the credentials are valid. Given this, the only
 * table spec that makes sense is one where the URI is defined: either as a template or
 * explicitly.
 * <p>
 * When used as an ad-hoc function, the user specifies the uris and optional user name
 * and password: the template is not available (or useful) in the ad-hoc case.
 * <p>
 * Table function parameters are cleaned up relative to the input source field names to
 * make them a bit easier to use.
 */
public class HttpInputSourceDefn extends FormattedInputSourceDefn
{
  public static final String TYPE_KEY = HttpInputSource.TYPE_KEY;

  // Catalog properties that map to fields in the HttpInputSource. See
  // that class for the meaning of these properties.

  public static final String URI_TEMPLATE_PROPERTY = "uriTemplate";

  public static final String URIS_PARAMETER = "uris";

  // Note, cannot be the simpler "user" since USER is a reserved word in SQL
  // and we don't want to require users to quote "user" each time it is used.
  public static final String USER_PARAMETER = "userName";
  public static final String PASSWORD_PARAMETER = "password";
  public static final String PASSWORD_ENV_VAR_PARAMETER = "passwordEnvVar";

  private static final List<ParameterDefn> URI_PARAMS = Collections.singletonList(
      new Parameter(URIS_PARAMETER, ParameterType.VARCHAR_ARRAY, true)
  );

  private static final List<ParameterDefn> USER_PWD_PARAMS = Arrays.asList(
      new Parameter(USER_PARAMETER, ParameterType.VARCHAR, true),
      new Parameter(PASSWORD_PARAMETER, ParameterType.VARCHAR, true),
      new Parameter(PASSWORD_ENV_VAR_PARAMETER, ParameterType.VARCHAR, true)
  );

  // Field names in the HttpInputSource
  protected static final String URIS_FIELD = "uris";
  protected static final String PASSWORD_FIELD = "httpAuthenticationPassword";
  protected static final String USERNAME_FIELD = "httpAuthenticationUsername";

  @Override
  public String typeValue()
  {
    return TYPE_KEY;
  }

  @Override
  protected Class<? extends InputSource> inputSourceClass()
  {
    return HttpInputSource.class;
  }

  @Override
  public void validate(ResolvedExternalTable table)
  {
    final Map<String, Object> sourceMap = table.inputSourceMap;
    final boolean hasUri = sourceMap.containsKey(URIS_FIELD);
    final String uriTemplate = table.resolvedTable().stringProperty(URI_TEMPLATE_PROPERTY);
    final boolean hasTemplate = uriTemplate != null;
    final boolean hasFormat = table.inputFormatMap != null;
    final boolean hasColumns = !CollectionUtils.isNullOrEmpty(table.resolvedTable().spec().columns());

    if (!hasUri && !hasTemplate) {
      throw new IAE(
          "External HTTP tables must provide either a URI or a %s property",
          URI_TEMPLATE_PROPERTY
      );
    }
    if (hasUri && hasTemplate) {
      throw new IAE(
          "External HTTP tables must provide only one of a URI or a %s property",
          URI_TEMPLATE_PROPERTY
      );
    }
    if (hasUri && !hasFormat) {
      throw new IAE(
          "An external HTTP table with a URI must also provide the corresponding format"
      );
    }
    if (hasUri && !hasColumns) {
      throw new IAE(
          "An external HTTP table with a URI must also provide the corresponding columns"
      );
    }
    if (hasTemplate) {

      // Verify the template
      templateMatcher(uriTemplate);

      // Patch in a dummy URI so that validation of the rest of the fields
      // will pass.
      try {
        sourceMap.put(
            URIS_FIELD,
            Collections.singletonList(new URI("https://bogus.com/file"))
        );
      }
      catch (Exception e) {
        throw new ISE(e, "URI parse failed");
      }
    }
    super.validate(table);
  }

  @Override
  protected void auditInputSource(Map<String, Object> jsonMap)
  {
    // A partial table may not include the URI parameter. For example, we might
    // define an HTTP input source "with URIs to be named later." Even though the
    // input source is partial, we still want to validate the other parameters.
    // The HttpInputSource will fail if the URIs is not set. So, we have to make
    // up a fake one just so we can validate the other fields by asking the
    // input source to deserialize itself from the jsonMap.
    jsonMap.putIfAbsent(URIS_PARAMETER, "http://bogus.com");
  }

  private Matcher templateMatcher(String uriTemplate)
  {
    Pattern p = Pattern.compile("\\{}");
    Matcher m = p.matcher(uriTemplate);
    if (!m.find()) {
      throw new IAE(
          "Value [%s] for property %s must include a '{}' placeholder",
          uriTemplate,
          URI_TEMPLATE_PROPERTY
      );
    }
    return m;
  }

  @Override
  protected List<ParameterDefn> adHocTableFnParameters()
  {
    return CatalogUtils.concatLists(URI_PARAMS, USER_PWD_PARAMS);
  }

  @Override
  protected void convertArgsToSourceMap(Map<String, Object> jsonMap, Map<String, Object> args)
  {
    jsonMap.put(InputSource.TYPE_PROPERTY, HttpInputSource.TYPE_KEY);
    convertUriArg(jsonMap, args);
    convertUserPasswordArgs(jsonMap, args);
  }

  @Override
  public TableFunction partialTableFn(ResolvedExternalTable table)
  {
    List<ParameterDefn> params = Collections.emptyList();

    // Does the table define URIs?
    Map<String, Object> sourceMap = table.inputSourceMap;
    if (!sourceMap.containsKey(URIS_FIELD)) {
      params = CatalogUtils.concatLists(params, URI_PARAMS);
    }

    // Does the table define a user or password?
    if (!sourceMap.containsKey(USERNAME_FIELD) && !sourceMap.containsKey(PASSWORD_FIELD)) {
      params = CatalogUtils.concatLists(params, USER_PWD_PARAMS);
    }

    // Does the table define a format?
    if (table.inputFormatMap == null) {
      params = addFormatParameters(params);
    }
    return new PartialTableFunction(table, params);
  }

  @Override
  protected ExternalTableSpec convertCompletedTable(
      final ResolvedExternalTable table,
      final Map<String, Object> args,
      final List<ColumnSpec> columns
  )
  {
    // Get URIs from table if defined, else from arguments.
    final Map<String, Object> sourceMap = new HashMap<>(table.inputSourceMap);
    final String uriTemplate = table.resolvedTable().stringProperty(URI_TEMPLATE_PROPERTY);
    if (uriTemplate != null) {
      convertUriTemplateArgs(sourceMap, uriTemplate, args);
    } else if (!sourceMap.containsKey(URIS_FIELD)) {
      convertUriArg(sourceMap, args);
    }

    // Get user and password from the table if defined, else from arguments.
    if (!sourceMap.containsKey(USERNAME_FIELD) && !sourceMap.containsKey(PASSWORD_FIELD)) {
      convertUserPasswordArgs(sourceMap, args);
    }
    return convertPartialFormattedTable(table, args, columns, sourceMap);
  }

  private void convertUriTemplateArgs(Map<String, Object> jsonMap, String uriTemplate, Map<String, Object> args)
  {
    List<String> uriStrings = CatalogUtils.getStringArray(args, URIS_PARAMETER);
    if (CollectionUtils.isNullOrEmpty(uriStrings)) {
      throw new IAE("One or more URIs is required in parameter %s", URIS_PARAMETER);
    }
    final Matcher m = templateMatcher(uriTemplate);
    final List<String> uris = uriStrings.stream()
        .map(uri -> m.replaceFirst(uri))
        .collect(Collectors.toList());
    jsonMap.put(URIS_FIELD, CatalogUtils.stringListToUriList(uris));
  }

  /**
   * URIs in SQL is in the form of a string that contains a comma-delimited
   * set of URIs. Done since SQL doesn't support array scalars.
   */
  private void convertUriArg(Map<String, Object> jsonMap, Map<String, Object> args)
  {
    List<String> uris = CatalogUtils.getStringArray(args, URIS_PARAMETER);
    if (uris != null) {
      jsonMap.put(URIS_FIELD, CatalogUtils.stringListToUriList(uris));
    }
  }

  /**
   * Convert the user name and password. All are SQL strings. Passwords must be in
   * the form of a password provider, so do the needed conversion. HTTP provides
   * two kinds of passwords (plain test an reference to an env var), but at most
   * one can be provided.
   */
  private void convertUserPasswordArgs(Map<String, Object> jsonMap, Map<String, Object> args)
  {
    String user = CatalogUtils.getString(args, USER_PARAMETER);
    if (user != null) {
      jsonMap.put(USERNAME_FIELD, user);
    }
    String password = CatalogUtils.getString(args, PASSWORD_PARAMETER);
    String passwordEnvVar = CatalogUtils.getString(args, PASSWORD_ENV_VAR_PARAMETER);
    if (password != null && passwordEnvVar != null) {
      throw new ISE(
          "Specify only one of %s or %s",
          PASSWORD_PARAMETER,
          PASSWORD_ENV_VAR_PARAMETER
      );
    }
    if (password != null) {
      jsonMap.put(
          PASSWORD_FIELD,
          ImmutableMap.of("type", DefaultPasswordProvider.TYPE_KEY, "password", password)
      );
    } else if (passwordEnvVar != null) {
      jsonMap.put(
          PASSWORD_FIELD,
          ImmutableMap.of("type", EnvironmentVariablePasswordProvider.TYPE_KEY, "variable", passwordEnvVar)
      );
    }
  }
}
