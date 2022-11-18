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

package org.apache.druid.catalog.model.facade;

import org.apache.druid.catalog.model.CatalogUtils;
import org.apache.druid.catalog.model.ResolvedTable;
import org.apache.druid.catalog.model.table.AbstractDatasourceDefn;
import org.apache.druid.catalog.model.table.ClusterKeySpec;
import org.apache.druid.java.util.common.granularity.Granularity;

import java.util.Collections;
import java.util.List;

/**
 * Convenience wrapper on top of a resolved table (a table spec and its corresponding
 * definition.) To be used by consumers of catalog objects that work with specific
 * datasource properties rather than layers that work with specs generically.
 */
public class DatasourceFacade extends TableFacade
{
  public DatasourceFacade(ResolvedTable resolved)
  {
    super(resolved);
  }

  public String segmentGranularityString()
  {
    return stringProperty(AbstractDatasourceDefn.SEGMENT_GRANULARITY_PROPERTY);
  }

  public Granularity segmentGranularity()
  {
    String value = stringProperty(AbstractDatasourceDefn.SEGMENT_GRANULARITY_PROPERTY);
    return value == null ? null : CatalogUtils.asDruidGranularity(value);
  }

  public Integer targetSegmentRows()
  {
    return intProperty(AbstractDatasourceDefn.TARGET_SEGMENT_ROWS_PROPERTY);
  }

  @SuppressWarnings("unchecked")
  public List<ClusterKeySpec> clusterKeys()
  {
    return (List<ClusterKeySpec>) property(AbstractDatasourceDefn.CLUSTER_KEYS_PROPERTY);
  }

  @SuppressWarnings("unchecked")
  public List<String> hiddenColumns()
  {
    Object value = property(AbstractDatasourceDefn.HIDDEN_COLUMNS_PROPERTY);
    return value == null ? Collections.emptyList() : (List<String>) value;
  }
}
