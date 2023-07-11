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

package org.apache.druid.sql.calcite.external;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import org.apache.druid.catalog.model.CatalogUtils;
import org.apache.druid.catalog.model.ColumnSpec;
import org.apache.druid.catalog.model.Columns;
import org.apache.druid.catalog.model.table.BaseTableFunction;
import org.apache.druid.catalog.model.table.ExternalTableSpec;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.segment.column.RowSignature;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Registers the "EXTERN" operator, which is used in queries like
 * <pre>{@code
 * INSERT INTO dst SELECT * FROM TABLE(EXTERN(
 *   "<input source>",
 *   "<input format>",
 *   "<signature>"))
 *
 * INSERT INTO dst SELECT * FROM TABLE(EXTERN(
 *   inputSource => "<input source>",
 *   inputFormat => "<input format>"))
 *   EXTEND (<columns>)
 * }</pre>
 * Where either the by-position or by-name forms are usable with either
 * a Druid JSON signature, or an SQL {@code EXTEND} list of columns.
 * As with all table functions, the {@code EXTEND} is optional.
 */
public class ExternalOperatorConversion extends DruidExternTableMacroConversion
{
  public static final String FUNCTION_NAME = "EXTERN";

  public static final String INPUT_SOURCE_PARAM = "inputSource";
  public static final String INPUT_FORMAT_PARAM = "inputFormat";
  public static final String SIGNATURE_PARAM = "signature";

  /**
   * The use of a table function allows the use of optional arguments,
   * so that the signature can be given either as the original-style
   * serialized JSON signature, or the updated SQL-style EXTEND clause.
   */
  private static class ExternFunction extends BaseTableFunction
  {
    public ExternFunction()
    {
      super(Arrays.asList(
          new Parameter(INPUT_SOURCE_PARAM, ParameterType.VARCHAR, false),
          new Parameter(INPUT_FORMAT_PARAM, ParameterType.VARCHAR, false),

          // Optional: the user can either provide the signature OR
          // an EXTEND clause. Checked in the implementation.
          new Parameter(SIGNATURE_PARAM, ParameterType.VARCHAR, true)
      ));
    }

    @Override
    public ExternalTableSpec apply(
        final String fnName,
        final Map<String, Object> args,
        final List<ColumnSpec> columns,
        final ObjectMapper jsonMapper
    )
    {
      try {
        final String sigValue = CatalogUtils.getString(args, SIGNATURE_PARAM);
        if (sigValue == null && columns == null) {
          throw new IAE(
              "EXTERN requires either a %s value or an EXTEND clause",
              SIGNATURE_PARAM
          );
        }
        if (sigValue != null && columns != null) {
          throw new IAE(
              "EXTERN requires either a %s value or an EXTEND clause, but not both",
              SIGNATURE_PARAM
          );
        }
        final RowSignature rowSignature;
        if (columns != null) {
          rowSignature = Columns.convertSignature(columns);
        } else {
          rowSignature = jsonMapper.readValue(sigValue, RowSignature.class);
        }

        String inputSrcStr = CatalogUtils.getString(args, INPUT_SOURCE_PARAM);
        InputSource inputSource = jsonMapper.readValue(inputSrcStr, InputSource.class);
        return new ExternalTableSpec(
            inputSource,
            jsonMapper.readValue(CatalogUtils.getString(args, INPUT_FORMAT_PARAM), InputFormat.class),
            rowSignature,
            inputSource::getTypes
        );
      }
      catch (JsonProcessingException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Inject
  public ExternalOperatorConversion(@Json final ObjectMapper jsonMapper)
  {
    super(FUNCTION_NAME, new ExternFunction(), jsonMapper);
  }
}
