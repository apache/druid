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

package org.apache.druid.indexing.seekablestream;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.indexing.overlord.DataSourceMetadata;

import java.util.Objects;

public abstract class SeekableStreamDataSourceMetadata<T1, T2> implements DataSourceMetadata
{
  private final SeekableStreamPartitions<T1, T2> seekableStreamPartitions;

  @JsonCreator
  public SeekableStreamDataSourceMetadata(
      @JsonProperty("partitions") SeekableStreamPartitions<T1, T2> seekableStreamPartitions
  )
  {
    this.seekableStreamPartitions = seekableStreamPartitions;
  }

  @JsonProperty("partitions")
  public SeekableStreamPartitions<T1, T2> getSeekableStreamPartitions()
  {
    return seekableStreamPartitions;
  }

  @Override
  public boolean isValidStart()
  {
    return true;
  }

  @Override
  public boolean matches(DataSourceMetadata other)
  {
    if (getClass() != other.getClass()) {
      return false;
    }

    return plus(other).equals(other.plus(this));
  }

  @Override
  public abstract DataSourceMetadata plus(DataSourceMetadata other);

  @Override
  public abstract DataSourceMetadata minus(DataSourceMetadata other);

  @Override
  public abstract boolean equals(Object o);

  @Override
  public int hashCode()
  {
    return Objects.hash(seekableStreamPartitions);
  }

  @Override
  public abstract String toString();
}
