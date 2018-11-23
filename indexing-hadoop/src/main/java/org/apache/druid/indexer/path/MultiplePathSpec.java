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

package org.apache.druid.indexer.path;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.indexer.HadoopDruidIndexerConfig;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;
import java.util.List;

public class MultiplePathSpec implements PathSpec
{
  private List<PathSpec> children;

  public MultiplePathSpec(
      @JsonProperty("children") List<PathSpec> children
  )
  {
    Preconditions.checkArgument(children != null && children.size() > 0, "Null/Empty list of child PathSpecs");
    this.children = children;
  }

  @JsonProperty
  public List<PathSpec> getChildren()
  {
    return children;
  }

  @Override
  public Job addInputPaths(HadoopDruidIndexerConfig config, Job job) throws IOException
  {
    for (PathSpec spec : children) {
      spec.addInputPaths(config, job);
    }
    return job;
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

    MultiplePathSpec that = (MultiplePathSpec) o;

    return children.equals(that.children);

  }

  @Override
  public int hashCode()
  {
    return children.hashCode();
  }
}
