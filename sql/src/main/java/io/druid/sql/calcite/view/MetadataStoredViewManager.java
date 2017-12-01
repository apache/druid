/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.sql.calcite.view;

import com.google.common.collect.Maps;
import com.google.inject.Inject;
import io.druid.metadata.MetadataViewManager;
import io.druid.sql.calcite.planner.PlannerFactory;

import java.util.Map;

public class MetadataStoredViewManager implements ViewManager {

    private final MetadataViewManager metadataManager;
    private PlannerFactory plannerFactory;

    @Inject
    public MetadataStoredViewManager(MetadataViewManager metadataManager) {
        this.metadataManager = metadataManager;
    }

    @Override
    public void createView(PlannerFactory plannerFactory, String viewName, String viewSql) {
        this.metadataManager.insertOrUpdate(viewName, viewSql);
    }

    @Override
    public void alterView(PlannerFactory plannerFactory, String viewName, String viewSql) {
        this.metadataManager.insertOrUpdate(viewName, viewSql);
    }

    @Override
    public void dropView(String viewName) {
        this.metadataManager.remove(viewName);
    }

    @Override
    public Map<String, DruidViewMacro> getViews() {
        if(this.plannerFactory == null) {
            throw new IllegalStateException("Planner Factory is not initialized before invoking getViews");
        }

        // get the view list every time from metadata storage
        Map<String, DruidViewMacro> viewMacroMap = Maps.newHashMap();
        Map<String, String> viewDefinitions = this.metadataManager.getAll();
        for(Map.Entry<String, String> entry : viewDefinitions.entrySet()) {
            viewMacroMap.put(entry.getKey(), new DruidViewMacro(this.plannerFactory, entry.getValue()));
        }

        return viewMacroMap;
    }

    public void setPlannerFactory(PlannerFactory plannerFactory) {
        this.plannerFactory = plannerFactory;
    }
}
