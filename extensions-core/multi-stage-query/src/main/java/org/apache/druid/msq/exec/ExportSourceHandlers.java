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

package org.apache.druid.msq.exec;

import com.google.inject.Inject;
import org.apache.druid.catalog.model.table.export.ExportSourceConfig;

import java.util.Map;

public class ExportSourceHandlers
{
  public Map<String, ExportSourceConfig> getConnectorProviderMap()
  {
    return connectorProviderMap;
  }

  private final Map<String, ExportSourceConfig> connectorProviderMap;

  @Inject
  public ExportSourceHandlers(Map<String, ExportSourceConfig> connectorProviderMap) {
    this.connectorProviderMap = connectorProviderMap;
  }
}
