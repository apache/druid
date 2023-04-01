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

import org.apache.druid.catalog.model.CatalogUtils;
import org.apache.druid.catalog.model.ColumnSpec;
import org.apache.druid.catalog.model.table.BaseTableFunction.Parameter;
import org.apache.druid.catalog.model.table.TableFunction.ParameterDefn;
import org.apache.druid.catalog.model.table.TableFunction.ParameterType;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.data.input.impl.CloudObjectLocation;
import org.apache.druid.data.input.s3.S3InputSource;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.storage.s3.S3StorageDruidModule;
import org.apache.druid.utils.CollectionUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Catalog definition for the S3 input source.
 * <p>
 * The catalog entry contains a serialized S3 input source, with some simplifying variations.
 * The catalog entry can define the {@code bucket} table property which is the (single) bucket
 * value to use when creating the list of objects: the catalog provides the bucket, the table
 * function provides the list of objects. (Done this way since defining two correlated lists
 * in SQL is awkward, and credentials make the most sense when working with a single bucket.)
 * <p>
 * The ad-hoc function allows the various ways to specify the objects, but not the configuration
 * parameters. If the user wishes to use such parameters, they should be defined in a catalog
 * entry, since providing maps in SQL is awkward.
 * <p>
 * The partial table function can be of various forms:
 * <ul>
 * <li>Fully define the table, which means providing the full set of S3 properties and not
 * providing the table-level {@code bucket} property. This form is complete and does't need
 * a table function. If used with a table function, the function provides the {@code glob}
 * parameter (if not already provided in the table spec.)</li>
 * <li>Partially define the table: using URIs with the {@code glob} to be provided later, or
 * by using the {@code bucket} table property. The table function provides the {@code objects}
 * parameter to specify the specific objects. This form provides both the format and the list
 * of columns.</li>
 * <li>Partially define the table as a connection: provide only the {@code bucket} property,
 * and omit both the format and the columns. The table function requests the {@code objects}
 * and the {@code format}. The user must provide the list of columns.</li>
 * </ul>
 *
 * @see {@link S3InputSource} for details on the meaning of the various properties, and the rules
 * about valid combinations
 */
public class S3InputSourceDefn extends FormattedInputSourceDefn
{
  public static final String TYPE_KEY = S3StorageDruidModule.SCHEME;
  public static final String URIS_PARAMETER = "uris";
  public static final String PREFIXES_PARAMETER = "prefixes";
  public static final String BUCKET_PARAMETER = "bucket";
  public static final String PATHS_PARAMETER = "paths";
  public static final String ACCESS_KEY_ID_PARAMETER = "accessKeyId";
  public static final String SECRET_ACCESS_KEY_PARAMETER = "secretAccessKey";
  public static final String ASSUME_ROLE_ARN_PARAMETER = "assumeRoleArn";

  /**
   * The {@code objectGlob} property exists in S3, but is not documented. The corresponding
   * function parameter also exists, but is not documented.
   */
  public static final String OBJECT_GLOB_PARAMETER = "objectGlob";

  /**
   * External data table spec property that lets the user define one bucket in the catalog,
   * so that the corresponding table function can supply just the relative path names within
   * that bucket. That is, if the user sets this, Druid will generate the {@code objects}
   * field from this entry and files provided in the table function.
   */
  public static final String BUCKET_PROPERTY = "bucket";

  private static final ParameterDefn URI_PARAM_DEFN = new Parameter(URIS_PARAMETER, ParameterType.VARCHAR_ARRAY, true);
  private static final ParameterDefn PREFIXES_PARAM_DEFN = new Parameter(PREFIXES_PARAMETER, ParameterType.VARCHAR_ARRAY, true);
  private static final ParameterDefn BUCKET_PARAM_DEFN = new Parameter(BUCKET_PARAMETER, ParameterType.VARCHAR, true);
  private static final ParameterDefn PATHS_PARAM_DEFN = new Parameter(PATHS_PARAMETER, ParameterType.VARCHAR_ARRAY, true);
  private static final ParameterDefn OBJECT_GLOB_PARAM_DEFN = new Parameter(OBJECT_GLOB_PARAMETER, ParameterType.VARCHAR, true);
  private static final List<ParameterDefn> SECURITY_PARAMS = Arrays.asList(
      new Parameter(ACCESS_KEY_ID_PARAMETER, ParameterType.VARCHAR, true),
      new Parameter(SECRET_ACCESS_KEY_PARAMETER, ParameterType.VARCHAR, true),
      new Parameter(ASSUME_ROLE_ARN_PARAMETER, ParameterType.VARCHAR, true)
  );

  // Field names in the S3InputSource
  private static final String URIS_FIELD = "uris";
  private static final String PREFIXES_FIELD = "prefixes";
  private static final String OBJECTS_FIELD = "objects";
  private static final String OBJECT_GLOB_FIELD = "objectGlob";
  private static final String PROPERTIES_FIELD = "properties";
  private static final String ACCESS_KEY_ID_FIELD = "accessKeyId";
  private static final String SECRET_ACCESS_KEY_FIELD = "secretAccessKey";
  private static final String ASSUME_ROLE_ARN_FIELD = "assumeRoleArn";

  @Override
  public String typeValue()
  {
    return TYPE_KEY;
  }

  @Override
  protected Class<? extends InputSource> inputSourceClass()
  {
    return S3InputSource.class;
  }

  @Override
  public void validate(ResolvedExternalTable table)
  {
    final boolean hasFormat = table.inputFormatMap != null;
    final boolean hasColumns = !CollectionUtils.isNullOrEmpty(table.resolvedTable().spec().columns());

    if (hasFormat && !hasColumns) {
      throw new IAE(
          "An external S3 table with a format must also provide the corresponding columns"
      );
    }

    // The user can either provide a bucket, or can provide one of the valid items.
    final String bucket = table.resolvedTable().stringProperty(BUCKET_PROPERTY);
    final boolean hasBucket = bucket != null;
    final Map<String, Object> sourceMap = table.inputSourceMap;
    final boolean hasUris = sourceMap.containsKey(URIS_FIELD);
    final boolean hasPrefix = sourceMap.containsKey(PREFIXES_FIELD);
    final boolean hasObjects = sourceMap.containsKey(OBJECTS_FIELD);
    final boolean hasGlob = sourceMap.containsKey(OBJECT_GLOB_FIELD);
    if (hasBucket) {
      if (hasUris || hasPrefix || hasObjects) {
        throw new IAE(
            "Provide either the %s property, or one of the S3 input source fields %s, %s or %s, but not both.",
            BUCKET_PROPERTY,
            URIS_FIELD,
            PREFIXES_FIELD,
            OBJECTS_FIELD
        );
      }
      if (hasGlob) {
        throw new IAE(
            "The %s property cannot be provided when the %s property is set",
            OBJECT_GLOB_FIELD,
            BUCKET_PROPERTY
        );
      }

      // Patch in a dummy URI so that validation of the rest of the fields
      // will pass.
      sourceMap.put(URIS_FIELD, Collections.singletonList(bucket));
    }
    super.validate(table);
  }

  @Override
  protected List<ParameterDefn> adHocTableFnParameters()
  {
    return CatalogUtils.concatLists(
        Arrays.asList(
            URI_PARAM_DEFN,
            PREFIXES_PARAM_DEFN,
            BUCKET_PARAM_DEFN,
            PATHS_PARAM_DEFN,
            OBJECT_GLOB_PARAM_DEFN
        ),
        SECURITY_PARAMS
    );
  }

  @Override
  protected void convertArgsToSourceMap(Map<String, Object> jsonMap, Map<String, Object> args)
  {
    jsonMap.put(InputSource.TYPE_PROPERTY, S3StorageDruidModule.SCHEME);
    final List<String> uris = CatalogUtils.getStringArray(args, URIS_PARAMETER);
    final List<String> prefixes = CatalogUtils.getStringArray(args, PREFIXES_PARAMETER);
    final String bucket = CatalogUtils.getNonBlankString(args, BUCKET_PARAMETER);
    final List<String> paths = CatalogUtils.getStringArray(args, PATHS_PARAMETER);
    final String objectGlob = CatalogUtils.getNonBlankString(args, OBJECT_GLOB_PARAMETER);
    final boolean hasUris = uris != null;
    final boolean hasPrefixes = prefixes != null;
    final boolean hasBucket = bucket != null;
    final boolean hasPaths = !CollectionUtils.isNullOrEmpty(paths);
    if (hasPaths && !hasBucket) {
      throw new IAE(
          "S3 requires the %s parameter if %s is set",
          BUCKET_PARAMETER,
          PATHS_PARAMETER
      );
    }
    if ((hasUris && (hasPrefixes || hasBucket)) || (hasPrefixes && hasBucket)) {
      throw new IAE(
          "S3 accepts only one of %s, %s or %s",
          PATHS_PARAMETER,
          BUCKET_PARAMETER,
          PREFIXES_PARAMETER
      );
    }
    if (!hasUris && !hasPrefixes && !hasBucket) {
      throw new IAE(
          "S3 requires one of %s, %s or %s",
          PATHS_PARAMETER,
          BUCKET_PARAMETER,
          PREFIXES_PARAMETER
      );
    }
    if (hasUris) {
      jsonMap.put(URIS_FIELD, CatalogUtils.stringListToUriList(uris));
    }
    if (hasPrefixes) {
      jsonMap.put(PREFIXES_FIELD, prefixes);
    }
    if (hasBucket) {
      if (!hasPaths) {
        throw new IAE("When using the %s parameter, %s must also be provided", BUCKET_PARAMETER, PATHS_PARAMETER);
      }
      List<CloudObjectLocation> objects = new ArrayList<>();
      for (String obj : paths) {
        objects.add(new CloudObjectLocation(bucket, obj));
      }
      jsonMap.put(OBJECTS_FIELD, objects);
    }
    if (objectGlob != null) {
      jsonMap.put(OBJECT_GLOB_FIELD, objectGlob);
    }
    applySecurityParams(jsonMap, args);
  }

  private void applySecurityParams(Map<String, Object> jsonMap, Map<String, Object> args)
  {
    final String accessKeyId = CatalogUtils.getNonBlankString(args, ACCESS_KEY_ID_PARAMETER);
    final String secretAccessKey = CatalogUtils.getNonBlankString(args, SECRET_ACCESS_KEY_PARAMETER);
    final String assumeRoleArn = CatalogUtils.getNonBlankString(args, ASSUME_ROLE_ARN_PARAMETER);
    if (accessKeyId != null || secretAccessKey != null || assumeRoleArn != null) {
      Map<String, Object> properties = new HashMap<>();
      if (accessKeyId != null) {
        properties.put(ACCESS_KEY_ID_FIELD, accessKeyId);
      }
      if (secretAccessKey != null) {
        properties.put(SECRET_ACCESS_KEY_FIELD, secretAccessKey);
      }
      if (assumeRoleArn != null) {
        properties.put(ASSUME_ROLE_ARN_FIELD, assumeRoleArn);
      }
      jsonMap.put(PROPERTIES_FIELD, properties);
    }
  }

  @Override
  public TableFunction partialTableFn(final ResolvedExternalTable table)
  {
    // Allow parameters depending on what is available.
    final Map<String, Object> sourceMap = table.inputSourceMap;
    List<ParameterDefn> params = new ArrayList<>();

    // If a bucket is provided, then the user can specify objects.
    if (table.resolvedTable().spec().properties().containsKey(BUCKET_PROPERTY)) {
      params.add(PATHS_PARAM_DEFN);

    // Else, if no glob is provided, the user can specify the glob.
    } else if (!sourceMap.containsKey(OBJECT_GLOB_FIELD)) {
      params.add(OBJECT_GLOB_PARAM_DEFN);
    }

    // Add security arguments if table does not provide them.
    if (!sourceMap.containsKey(PROPERTIES_FIELD)) {
      params.addAll(SECURITY_PARAMS);
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
    final Map<String, Object> sourceMap = new HashMap<>(table.inputSourceMap);

    // If a bucket parameter is provided, user provides the objects. Else, use the
    // catalog input source definition.
    final String bucket = table.resolvedTable().stringProperty(BUCKET_PROPERTY);
    if (bucket != null) {
      List<String> paths = CatalogUtils.getStringArray(args, PATHS_PARAMETER);
      if (CollectionUtils.isNullOrEmpty(paths)) {
        throw new IAE(
            "S3 external table defines the %s property. The table function must provide the %s parameter",
            BUCKET_PROPERTY,
            PATHS_PARAMETER
        );
      }
      List<CloudObjectLocation> objects = new ArrayList<>();
      for (String obj : paths) {
        objects.add(new CloudObjectLocation(bucket, obj));
      }
      sourceMap.put(OBJECTS_FIELD, objects);
    }

    applySecurityParams(sourceMap, args);
    return convertPartialFormattedTable(table, args, columns, sourceMap);
  }
}
