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

package org.apache.druid.iceberg.input;

import javax.annotation.Nullable;
import java.util.List;

/**
 * Holds the result of extracting data files from an Iceberg table, including
 * both the file paths and any vended credentials from the catalog.
 */
public class IcebergDataFilesWithCredentials
{
  private final List<String> dataFilePaths;
  private final VendedCredentials vendedCredentials;

  public IcebergDataFilesWithCredentials(
      List<String> dataFilePaths,
      @Nullable VendedCredentials vendedCredentials
  )
  {
    this.dataFilePaths = dataFilePaths;
    this.vendedCredentials = vendedCredentials;
  }

  public List<String> getDataFilePaths()
  {
    return dataFilePaths;
  }

  @Nullable
  public VendedCredentials getVendedCredentials()
  {
    return vendedCredentials;
  }

  public boolean hasVendedCredentials()
  {
    return vendedCredentials != null && !vendedCredentials.isEmpty();
  }
}
