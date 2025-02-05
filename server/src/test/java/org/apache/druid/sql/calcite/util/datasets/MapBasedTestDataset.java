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

package org.apache.druid.sql.calcite.util.datasets;

import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.impl.MapInputRowParser;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public abstract class MapBasedTestDataset extends AbstractRowBasedTestDataset
{
  protected MapBasedTestDataset(String name)
  {
    super(name);
  }

  public final Iterable<InputRow> getRows()
  {
    return getRawRows()
        .stream()
        .map(raw -> createRow(raw, getInputRowSchema()))
        .collect(Collectors.toList());
  }

  public static InputRow createRow(final Map<String, ?> map, InputRowSchema inputRowSchema)
  {
    return MapInputRowParser.parse(inputRowSchema, (Map<String, Object>) map);
  }

  public abstract List<Map<String, Object>> getRawRows();
}