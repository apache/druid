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

package org.apache.druid.catalog.storage;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Represents a request sent from a client to update just the list of hidden
 * columns for a datasource table. Allows sending "delta encoded" changes: just
 * the entries to add or remove. Exists as a separate operation since the
 * generic merge can't handle removing items from a list.
 *
 * @see {@link org.apache.druid.catalog.http.CatalogResource#hideColumns(String, String, HideColumns, javax.servlet.http.HttpServletRequest)}
 */
public class HideColumns
{
  @JsonProperty
  public final List<String> hide;
  @JsonProperty
  public final List<String> unhide;

  @JsonCreator
  public HideColumns(
      @JsonProperty("hide") @Nullable final List<String> hide,
      @JsonProperty("unhide") @Nullable final List<String> unhide
  )
  {
    this.hide = hide;
    this.unhide = unhide;
  }

  @JsonIgnore
  public boolean isEmpty()
  {
    return (hide == null || hide.isEmpty())
        && (unhide == null || unhide.isEmpty());
  }

  /**
   * Given the existing list of hidden columns, perform the update action to add the
   * requested new columns (if they don't yet exist) and remove the requested columns
   * (if they do exist). If someone is silly enough to include the same column in
   * both lists, the remove action takes precedence.
   *
   * @param hiddenColumns exiting hidden columns list
   * @return revised hidden columns list after applying the requested changes
   */
  public List<String> perform(List<String> hiddenColumns)
  {
    if (hiddenColumns == null) {
      hiddenColumns = Collections.emptyList();
    }
    Set<String> existing = new HashSet<>(hiddenColumns);
    if (unhide != null) {
      for (String col : unhide) {
        existing.remove(col);
      }
    }
    List<String> revised = new ArrayList<>();
    for (String col : hiddenColumns) {
      if (existing.contains(col)) {
        revised.add(col);
      }
    }
    if (hide != null) {
      for (String col : hide) {
        if (!existing.contains(col)) {
          revised.add(col);
        }
      }
    }
    return revised.isEmpty() ? null : revised;
  }

  @Override
  public boolean equals(Object o)
  {
    if (o == null || o.getClass() != getClass()) {
      return false;
    }
    HideColumns other = (HideColumns) o;
    return Objects.equals(this.hide, other.hide)
        && Objects.equals(this.unhide, other.unhide);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(hide, unhide);
  }
}
