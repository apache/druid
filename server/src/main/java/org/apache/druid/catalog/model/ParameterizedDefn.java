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

package org.apache.druid.catalog.model;

import org.apache.druid.catalog.model.table.ExternalSpec;

import java.util.List;
import java.util.Map;

public interface ParameterizedDefn
{
  interface ParameterDefn
  {
    String name();
    Class<?> valueClass();
  }

  class ParameterImpl implements ParameterDefn
  {
    private final String name;
    private final Class<?> type;

    public ParameterImpl(final String name, final Class<?> type)
    {
      this.name = name;
      this.type = type;
    }

    @Override
    public String name()
    {
      return name;
    }

    @Override
    public Class<?> valueClass()
    {
      return type;
    }
  }

  List<ParameterDefn> parameters();
  ParameterDefn parameter(String name);
  ExternalSpec applyParameters(ResolvedTable table, Map<String, Object> parameters);
}
