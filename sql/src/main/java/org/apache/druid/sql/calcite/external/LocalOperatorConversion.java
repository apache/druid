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

import com.google.inject.Inject;
import org.apache.druid.catalog.model.TableDefnRegistry;
import org.apache.druid.catalog.model.table.LocalInputSourceDefn;

public class LocalOperatorConversion extends DruidUserDefinedTableMacroConversion
{
  // Cannot use "local" because it is a SQL keyword and the user would
  // be required to quote the name.
  public static final String FUNCTION_NAME = "localfiles";

  @Inject
  public LocalOperatorConversion(final TableDefnRegistry registry)
  {
    super(FUNCTION_NAME, registry, LocalInputSourceDefn.TYPE_KEY, registry.jsonMapper());
  }
}
