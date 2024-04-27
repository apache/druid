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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.catalog.model.ColumnSpec;
import org.apache.druid.catalog.model.table.TableFunction.ParameterDefn;
import org.apache.druid.data.input.InputFormat;

import java.util.List;
import java.util.Map;

/**
 * Metadata about a Druid {@link InputFormat}. Provides the logic to
 * convert from a table spec or SQL function arguments to a specific
 * form of {@code InputFormat}. There is one instance of this interface for each
 * supported {@code InputFormat}.
 */
public interface InputFormatDefn
{
  /**
   * @return the string used to identify the input format type in the serialized
   * JSON for the input format. This is also the value used in the {@code format}
   * property for SQL functions.
   */
  String typeValue();

  /**
   * Given a resolved table that has the serialized JSON converted to a Java map,
   * validate the values of that map, typically by converting that map the target
   * input format object (after adjustments and filling in dummy columns.) THe
   * goal is to validate the information the user has provided in the table spec.
   * The final format information is validated elsewhere.
   */
  void validate(ResolvedExternalTable table);

  /**
   * Obtain the parameters used to fully define an input format in a SQL function
   * that defines an external table from scratch. Note that the final list of table
   * function arguments combines parameters from all the various input sources. It
   * is legal for multiple formats to define the same parameter, as long as both
   * definitions are of the same type.
   * @return
   */
  List<ParameterDefn> parameters();

  /**
   * Create an input format instance from the values provided as arguments that
   * correspond to the defined parameters. The map provided is guaranteed to have
   * a value for each parameter defined as non-optional, and those values will be
   * of the type defined in the parameter. The map will also contain values for
   * the input source: the format should ignore values that don't correspond to
   * parameters it defined.
   *
   * @param args       the actual arguments for the defined parameters
   * @param columns    the columns provided in SQL, typically via the `EXTEND` clause,
   *                   but perhaps from an the table spec, if the function is for
   *                   a partial table
   * @param jsonMapper the mapper to use to perform conversions
   *
   * @return an input format as defined by the arguments
   * @throws org.apache.druid.java.util.common.IAE if the arguments are not valid
   */
  InputFormat convertFromArgs(
      Map<String, Object> args,
      List<ColumnSpec> columns,
      ObjectMapper jsonMapper
  );

  /**
   * Create an input format from a resolved catalog table spec. The format is given
   * by the Java map within the given object.
   *
   * @param table resolved form of a table spec, with the format JSON parsed into
   *              a JSON map
   *
   * @return an input format as defined by the table spec
   * @throws org.apache.druid.java.util.common.IAE if the spec is not valid
   */
  InputFormat convertFromTable(ResolvedExternalTable table);
}
