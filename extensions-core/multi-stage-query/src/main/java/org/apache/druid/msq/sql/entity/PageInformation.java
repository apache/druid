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

package org.apache.druid.msq.sql.entity;


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;
import java.util.Comparator;
import java.util.Objects;

/**
 * Contains information about a single page in the results.
 */
public class PageInformation
{
  private final long id;
  @Nullable
  private final Long numRows;
  @Nullable
  private final Long sizeInBytes;

  @JsonCreator
  public PageInformation(
      @JsonProperty("id") long id,
      @JsonProperty("numRows") @Nullable Long numRows,
      @JsonProperty("sizeInBytes") @Nullable Long sizeInBytes
  )
  {
    this.id = id;
    this.numRows = numRows;
    this.sizeInBytes = sizeInBytes;
  }

  @JsonProperty
  public long getId()
  {
    return id;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  @Nullable
  public Long getNumRows()
  {
    return numRows;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  @Nullable
  public Long getSizeInBytes()
  {
    return sizeInBytes;
  }


  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    PageInformation that = (PageInformation) o;
    return id == that.id && Objects.equals(numRows, that.numRows) && Objects.equals(
        sizeInBytes,
        that.sizeInBytes
    );
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(id, numRows, sizeInBytes);
  }

  @Override
  public String toString()
  {
    return "PageInformation{" +
           "id=" + id +
           ", numRows=" + numRows +
           ", sizeInBytes=" + sizeInBytes +
           '}';
  }

  public static Comparator<PageInformation> getIDComparator()
  {
    return new PageComparator();
  }

  public static class PageComparator implements Comparator<PageInformation>
  {
    @Override
    public int compare(PageInformation s1, PageInformation s2)
    {
      return Long.compare(s1.getId(), s2.getId());
    }
  }
}
