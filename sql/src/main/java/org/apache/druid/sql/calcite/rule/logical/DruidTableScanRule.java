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

package org.apache.druid.sql.calcite.rule.logical;

import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.druid.sql.calcite.rel.logical.DruidLogicalConvention;
import org.apache.druid.sql.calcite.rel.logical.DruidTableScan;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * {@link ConverterRule} to convert {@link org.apache.calcite.rel.core.TableScan} to {@link DruidTableScan}
 */
public class DruidTableScanRule extends ConverterRule
{
  public DruidTableScanRule(Class<? extends RelNode> clazz, RelTrait in, RelTrait out, String descriptionPrefix)
  {
    super(
        Config.INSTANCE
            .withConversion(clazz, in, out, descriptionPrefix)
    );
  }

  @Override
  public @Nullable RelNode convert(RelNode rel)
  {
    LogicalTableScan tableScan = (LogicalTableScan) rel;
    RelTraitSet newTrait = tableScan.getTraitSet().replace(DruidLogicalConvention.instance());
    DruidTableScan druidTableScan = new DruidTableScan(
        tableScan.getCluster(),
        newTrait,
        tableScan.getTable()
    );
    return druidTableScan;
  }
}
