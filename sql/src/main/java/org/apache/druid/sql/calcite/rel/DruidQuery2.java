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

package org.apache.druid.sql.calcite.rel;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.druid.query.Query;
import org.apache.druid.segment.column.RowSignature;

/**
 * A fully formed Druid query, built from a {@link PartialDruidQuery}. The work
 * to develop this query is done during construction, which may throw
 * {@link CannotBuildQueryException}.
 */
public class DruidQuery2
{
  private DruidQuery druidQuery;

  public DruidQuery2(DruidQuery druidQuery)
  {
    this.druidQuery = druidQuery;
  }

  public Query<?> getQuery()
  {
    return druidQuery.getQuery();
  }

  public RowSignature getOutputRowSignature()
  {
    return druidQuery.getOutputRowSignature();
  }

  @Deprecated
  public RelDataType getOutputRowType()
  {
    return druidQuery.getOutputRowType();
  }

  @Deprecated
  public Grouping getGrouping()
  {
    return druidQuery.getGrouping();
  }
}
