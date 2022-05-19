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

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.RelTraitSet;

public class DruidConvention implements Convention
{
  private static final DruidConvention INSTANCE = new DruidConvention();
  private static final String NAME = "DRUID";

  private DruidConvention()
  {
  }

  public static DruidConvention instance()
  {
    return INSTANCE;
  }

  @Override
  public Class getInterface()
  {
    return DruidRel.class;
  }

  @Override
  public String getName()
  {
    return NAME;
  }

  @Override
  public boolean canConvertConvention(Convention toConvention)
  {
    return false;
  }

  @Override
  public boolean useAbstractConvertersForConversion(RelTraitSet fromTraits, RelTraitSet toTraits)
  {
    return false;
  }

  @Override
  public RelTraitDef getTraitDef()
  {
    return ConventionTraitDef.INSTANCE;
  }

  @Override
  public boolean satisfies(RelTrait trait)
  {
    return trait.equals(this);
  }

  @Override
  public void register(RelOptPlanner planner)
  {
  }

  @Override
  public String toString()
  {
    return NAME;
  }
}
