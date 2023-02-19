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
import org.apache.druid.catalog.model.table.ClusterKeySpec;
import org.apache.druid.catalog.model.table.DatasourceDefn;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.logger.Logger;

import java.util.Collections;
import java.util.List;

/**
 * Convenience wrapper on top of a resolved table (a table spec and its corresponding
 * definition.) To be used by consumers of catalog objects that work with specific
 * datasource properties rather than layers that work with specs generically.
 */
public class DatasourceFacade extends TableFacade
{
  private static final Logger LOG = new Logger(DatasourceFacade.class);


  public DatasourceFacade(ResolvedTable resolved)
  {
    super(resolved);
  }

  public String segmentGranularityString()
  {
    return stringProperty(DatasourceDefn.SEGMENT_GRANULARITY_PROPERTY);
  }

  public Granularity segmentGranularity()
  {
    String definedGranularity = segmentGranularityString();
    return definedGranularity == null ? null : CatalogUtils.asDruidGranularity(definedGranularity);
  }

  public Integer targetSegmentRows()
  {
    return intProperty(DatasourceDefn.TARGET_SEGMENT_ROWS_PROPERTY);
  }

  public List<ClusterKeySpec> clusterKeys()
  {
    Object value = property(DatasourceDefn.CLUSTER_KEYS_PROPERTY);
    if (value == null) {
      return Collections.emptyList();
    }
    try {
      return jsonMapper().convertValue(value, ClusterKeySpec.CLUSTER_KEY_LIST_TYPE_REF);
    }
    catch (Exception e) {
      LOG.error("Failed to convert a catalog %s property of value [%s]",
          DatasourceDefn.CLUSTER_KEYS_PROPERTY,
          value
      );
      return Collections.emptyList();
    }
  }

  @SuppressWarnings("unchecked")
  public List<String> hiddenColumns()
  {
    Object value = property(DatasourceDefn.HIDDEN_COLUMNS_PROPERTY);
    return value == null ? Collections.emptyList() : (List<String>) value;
  }

  public boolean isSealed()
  {
    return booleanProperty(DatasourceDefn.SEALED_PROPERTY);
  }
}
