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
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.druid.sql.calcite.rel.logical.DruidLogicalConvention;
import org.apache.druid.sql.calcite.rel.logical.DruidSort;

/**
 * {@link ConverterRule} to convert {@link Sort} to {@link DruidSort}
 */
public class DruidSortRule extends ConverterRule
{

  public DruidSortRule(
      Class<? extends RelNode> clazz,
      RelTrait in,
      RelTrait out,
      String descriptionPrefix
  )
  {
    super(clazz, in, out, descriptionPrefix);
  }

  @Override
  public RelNode convert(RelNode rel)
  {
    LogicalSort sort = (LogicalSort) rel;
    return DruidSort.create(
        convert(
            sort.getInput(),
            sort.getInput().getTraitSet().replace(DruidLogicalConvention.instance())
        ),
        sort.getCollation(),
        sort.offset,
        sort.fetch
    );
  }
}
