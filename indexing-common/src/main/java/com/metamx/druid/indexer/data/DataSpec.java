/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package com.metamx.druid.indexer.data;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.metamx.common.parsers.Parser;
import com.metamx.druid.index.v1.SpatialDimensionSchema;

import java.util.List;

/**
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "format", defaultImpl = DelimitedDataSpec.class)
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "json", value = JSONDataSpec.class),
    @JsonSubTypes.Type(name = "csv", value = CSVDataSpec.class),
    @JsonSubTypes.Type(name = "tsv", value = DelimitedDataSpec.class)
})
public interface DataSpec
{
  public void verify(List<String> usedCols);

  public boolean hasCustomDimensions();

  public List<String> getDimensions();

  public List<SpatialDimensionSchema> getSpatialDimensions();

  public Parser<String, Object> getParser();
}
