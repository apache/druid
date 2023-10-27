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

package org.apache.druid.spectator.histogram;

import org.apache.druid.segment.column.ComplexColumn;

public class SpectatorHistogramIndexBasedComplexColumn implements ComplexColumn
{
  private final SpectatorHistogramIndexed index;
  private final String typeName;

  public SpectatorHistogramIndexBasedComplexColumn(String typeName, SpectatorHistogramIndexed index)
  {
    this.index = index;
    this.typeName = typeName;
  }

  @Override
  public Class<?> getClazz()
  {
    return index.getClazz();
  }

  @Override
  public String getTypeName()
  {
    return typeName;
  }

  @Override
  public Object getRowValue(int rowNum)
  {
    return index.get(rowNum);
  }

  @Override
  public int getLength()
  {
    return index.size();
  }

  @Override
  public void close()
  {
  }
}
