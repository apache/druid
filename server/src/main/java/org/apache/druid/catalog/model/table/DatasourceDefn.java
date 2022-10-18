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
import org.apache.druid.catalog.model.ColumnDefn;
import org.apache.druid.catalog.model.ColumnSpec;

import java.util.Collections;

/**
 * Definition of a Druid datasource. The datasource may use rollup, but rollup
 * is not represented in the catalog: it is just a way that applications store
 * data into a table.
 */
public class DatasourceDefn extends AbstractDatasourceDefn
{
  /**
   * Definition of a column in a datasource.
   */
  public static class DatasourceColumnDefn extends ColumnDefn
  {
    public static final String COLUMN_TYPE = "column";

    public DatasourceColumnDefn()
    {
      super(
          "Column",
          COLUMN_TYPE,
          null
      );
    }

    @Override
    public void validate(ColumnSpec spec, ObjectMapper jsonMapper)
    {
      super.validate(spec, jsonMapper);
      validateScalarColumn(spec);
    }
  }

  public static final String TABLE_TYPE = "datasource";

  public DatasourceDefn()
  {
    super(
        "Datasource",
        TABLE_TYPE,
        null,
        Collections.singletonList(new DatasourceDefn.DatasourceColumnDefn())
    );
  }
}
