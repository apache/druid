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

import org.apache.druid.catalog.model.TableDefnRegistry;

/**
 * Metadata definition for one Druid input source.
 *
 * @see {@link ExternalTableDefn} for a detailed explanation.
 */
public interface InputSourceDefn
{
  /**
   * Gather information about the set of format definitions.
   */
  void bind(TableDefnRegistry registry);

  /**
   * Type value for this format: same as the type string used in the serialized
   * JSON for this input source. Used as the key for this definition within the
   * table registry, and associates the serialized JSON with the corresponding
   * input source definition.
   */
  String typeValue();

  /**
   * Given a external table catalog spec, with the JSON input source and format
   * properties parsed to generic Java maps, validate that the properties are
   * valid prior to saving the spec into the catalog.
   *
   * @param table a catalog table spec with the input source and input format
   *              properties parsed into generic Java maps
   *
   * @throws org.apache.druid.java.util.common.IAE if the spec
   *         properties are invalid
   */
  void validate(ResolvedExternalTable table);

  /**
   * Provide a definition for a SQL table function that defines an ad-hoc external
   * table "from scratch" for this input source. Typically defines parameters for the
   * input source and all allowed input formats.
   *
   * @return a fully-defined external table to be handed off to the Calcite planner.
   *
   * @throws org.apache.druid.java.util.common.IAE if the function arguments
   *         are invalid
   */
  TableFunction adHocTableFn();

  /**
   * Provide a definition for a SQL table function that completes a partial table
   * spec from the catalog. Used when the spec represents a "partial table" or a
   * "connection". The function provides parameters needed to complete the table
   * (typically the set of input files, objects, etc.) If the catalog table spec
   * does not provide a format, and this input source requires a format, then the
   * parameters also include parameters for all supported input formats, same as
   * for {@link InputSourceDefn#adHocTableFn()}.
   *
   * @param table a catalog table spec with the input source and input format
   *              properties parsed into generic Java maps
   *
   * @return a fully-defined external table to be handed off to the Calcite planner.
   *
   * @throws org.apache.druid.java.util.common.IAE if the function arguments
   *         are invalid
   */
  TableFunction partialTableFn(ResolvedExternalTable table);

  /**
   * Convert a complete (that is, fully-defined) table spec to an external table spec.
   * Used when SQL references the catalog table directly by name in the {@code FROM}
   * clause without using a table function.
   *
   * @return a fully-defined external table to be handed off to the Calcite planner.
   *
   * @throws org.apache.druid.java.util.common.IAE if the spec properties are invalid,
   *         or if the spec is partial and requires the use of a table function to
   *         complete
   */
  ExternalTableSpec convertTable(ResolvedExternalTable table);
}
