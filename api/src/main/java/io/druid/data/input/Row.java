/*
* Licensed to Metamarkets Group Inc. (Metamarkets) under one
* or more contributor license agreements. See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership. Metamarkets licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License. You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied. See the License for the
* specific language governing permissions and limitations
* under the License.
*/

package io.druid.data.input;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.joda.time.DateTime;

import java.util.List;

/**
 * A Row of data.  This can be used for both input and output into various parts of the system.  It assumes
 * that the user already knows the schema of the row and can query for the parts that they care about.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "version", defaultImpl = MapBasedRow.class)
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "v1", value = MapBasedRow.class)
})
public interface Row extends Comparable<Row>
{
  /**
   * Returns the timestamp from the epoch in milliseconds.  If the event happened _right now_, this would return the
   * same thing as System.currentTimeMillis();
   *
   * @return the timestamp from the epoch in milliseconds.
   */
  public long getTimestampFromEpoch();

  /**
   * Returns the timestamp from the epoch as an org.joda.time.DateTime.  If the event happened _right now_, this would return the
   * same thing as new DateTime();
   *
   * @return the timestamp from the epoch as an org.joda.time.DateTime object.
   */
  public DateTime getTimestamp();

  /**
   * Returns the list of dimension values for the given column name.
   * <p/>
   *
   * @param dimension the column name of the dimension requested
   *
   * @return the list of values for the provided column name
   */
  public List<String> getDimension(String dimension);

  /**
   * Returns the raw dimension value for the given column name. This is different from #getDimension which
   * all values to strings before returning them.
   *
   * @param dimension the column name of the dimension requested
   *
   * @return the value of the provided column name
   */
  public Object getRaw(String dimension);

  /**
   * Returns the float value of the given metric column.
   * <p/>
   *
   * @param metric the column name of the metric requested
   *
   * @return the float value for the provided column name.
   */
  public float getFloatMetric(String metric);

  /**
   * Returns the long value of the given metric column.
   * <p/>
   *
   * @param metric the column name of the metric requested
   *
   * @return the long value for the provided column name.
   */
  public long getLongMetric(String metric);
}
