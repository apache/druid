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

import org.apache.druid.query.DataSource;
import org.apache.druid.sql.calcite.table.DruidTable;

import java.util.Optional;

public class DruidRels
{
  /**
   * Returns the DataSource involved in a leaf query of class {@link DruidQueryRel}.
   */
  public static Optional<DataSource> dataSourceIfLeafRel(final DruidRel<?> druidRel)
  {
    if (druidRel instanceof DruidQueryRel) {
      return Optional.of(druidRel.getTable().unwrap(DruidTable.class).getDataSource());
    } else {
      return Optional.empty();
    }
  }

  /**
   * Check if a druidRel is a simple table scan, or a projection that merely remaps columns without transforming them.
   * Like {@link #isScanOrProject} but more restrictive: only remappings are allowed.
   *
   * @param druidRel  the rel to check
   * @param canBeJoin consider a 'join' that doesn't do anything fancy to be a scan-or-mapping too.
   */
  public static boolean isScanOrMapping(final DruidRel<?> druidRel, final boolean canBeJoin)
  {
    if (isScanOrProject(druidRel, canBeJoin)) {
      // Like isScanOrProject, but don't allow transforming projections.
      final PartialDruidQuery partialQuery = druidRel.getPartialDruidQuery();
      return partialQuery.getSelectProject() == null || partialQuery.getSelectProject().isMapping();
    } else {
      return false;
    }
  }

  /**
   * Check if a druidRel is a simple table scan or a scan + projection.
   *
   * @param druidRel  the rel to check
   * @param canBeJoin consider a 'join' that doesn't do anything fancy to be a scan-or-mapping too.
   */
  private static boolean isScanOrProject(final DruidRel<?> druidRel, final boolean canBeJoin)
  {
    if (druidRel instanceof DruidQueryRel || (canBeJoin && druidRel instanceof DruidJoinQueryRel)) {
      final PartialDruidQuery partialQuery = druidRel.getPartialDruidQuery();
      final PartialDruidQuery.Stage stage = partialQuery.stage();
      return (stage == PartialDruidQuery.Stage.SCAN || stage == PartialDruidQuery.Stage.SELECT_PROJECT)
             && partialQuery.getWhereFilter() == null;
    } else {
      return false;
    }
  }
}
