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

package org.apache.druid.sql.calcite.schema;

import org.apache.druid.com.google.common.base.Preconditions;
import org.apache.druid.com.google.common.collect.ImmutableMultimap;
import org.apache.druid.com.google.common.collect.Multimap;
import com.google.inject.Inject;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.druid.sql.calcite.view.DruidViewMacro;
import org.apache.druid.sql.calcite.view.ViewManager;

import java.util.Map;

public class ViewSchema extends AbstractSchema
{
  private final ViewManager viewManager;

  @Inject
  public ViewSchema(
      final ViewManager viewManager
  )
  {
    this.viewManager = Preconditions.checkNotNull(viewManager, "viewManager");
  }

  @Override
  protected Multimap<String, Function> getFunctionMultimap()
  {
    final ImmutableMultimap.Builder<String, Function> builder = ImmutableMultimap.builder();
    for (Map.Entry<String, DruidViewMacro> entry : viewManager.getViews().entrySet()) {
      builder.put(entry);
    }
    return builder.build();
  }
}
