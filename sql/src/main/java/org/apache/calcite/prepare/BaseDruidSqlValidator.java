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

//CHECKSTYLE.OFF: PackageName - Must be in Calcite

package org.apache.calcite.prepare;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.validate.SqlConformance;

/**
 * Extend the Java-focused {@link CalciteSqlValidator} to make it visible
 * to Druid. {@link CalciteSqlValidator} itself is protected and can be
 * extended only if this class is in the same Calcite package.
 */
public class BaseDruidSqlValidator extends CalciteSqlValidator
{
  public BaseDruidSqlValidator(
      SqlOperatorTable opTab,
      CalciteCatalogReader catalogReader,
      JavaTypeFactory typeFactory,
      SqlConformance conformance)
  {
    super(opTab, catalogReader, typeFactory, conformance);
  }
}
