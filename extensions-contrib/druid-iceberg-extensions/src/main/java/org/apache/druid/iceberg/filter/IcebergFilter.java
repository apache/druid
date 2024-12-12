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

package org.apache.druid.iceberg.filter;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.expressions.Expression;

/**
 * Interface to manage iceberg expressions which can be used to perform filtering on the iceberg table
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "interval", value = IcebergIntervalFilter.class),
    @JsonSubTypes.Type(name = "equals", value = IcebergEqualsFilter.class),
    @JsonSubTypes.Type(name = "and", value = IcebergAndFilter.class),
    @JsonSubTypes.Type(name = "not", value = IcebergNotFilter.class),
    @JsonSubTypes.Type(name = "or", value = IcebergOrFilter.class),
    @JsonSubTypes.Type(name = "range", value = IcebergRangeFilter.class)
})
public interface IcebergFilter
{
  TableScan filter(TableScan tableScan);

  Expression getFilterExpression();
}
