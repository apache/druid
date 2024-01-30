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

package org.apache.druid.sql.destination;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

import java.util.Map;
import java.util.Objects;

/**
 * Destination that represents an ingestion to an external source.
 */
@JsonTypeName(ExportDestination.TYPE_KEY)
public class ExportDestination implements IngestDestination
{
  public static final String TYPE_KEY = "external";
  private final String destination;
  private final Map<String, String> properties;

  public ExportDestination(@JsonProperty("destination") String destination, @JsonProperty("properties") Map<String, String> properties)
  {
    this.destination = destination;
    this.properties = properties;
  }

  @JsonProperty("destination")
  public String getDestination()
  {
    return destination;
  }

  @JsonProperty("properties")
  public Map<String, String> getProperties()
  {
    return properties;
  }

  @Override
  public String getType()
  {
    return TYPE_KEY;
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
    ExportDestination that = (ExportDestination) o;
    return Objects.equals(destination, that.destination) && Objects.equals(
        properties,
        that.properties
    );
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(destination, properties);
  }

  @Override
  public String toString()
  {
    return "ExportDestination{" +
           "destination='" + destination + '\'' +
           ", properties=" + properties +
           '}';
  }
}
