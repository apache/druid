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

package io.druid.indexer.path;

import io.druid.jackson.DefaultObjectMapper;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.junit.Assert;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

public class StaticPathSpecTest
{
  private final ObjectMapper jsonMapper = new DefaultObjectMapper();

  @Test
  public void testDeserialization() throws Exception
  {
    testDeserialization("/sample/path", TextInputFormat.class);
  }

  @Test
  public void testDeserializationNoInputFormat() throws Exception
  {
    testDeserialization("/sample/path", null);
  }

  private void testDeserialization(String path, Class inputFormat) throws Exception
  {
    StringBuilder sb = new StringBuilder();
    sb.append("{\"paths\" : \"");
    sb.append(path);
    sb.append("\",");
    if(inputFormat != null) {
      sb.append("\"inputFormat\" : \"");
      sb.append(inputFormat.getName());
      sb.append("\",");
    }
    sb.append("\"type\" : \"static\"}");
    StaticPathSpec pathSpec = (StaticPathSpec)jsonMapper.readValue(sb.toString(), PathSpec.class);
    Assert.assertEquals(inputFormat, pathSpec.getInputFormat());
    
    Job job = Job.getInstance();
    pathSpec.addInputPaths(null, job);
    Assert.assertEquals(
        "file:" + path,
        job.getConfiguration().get(FileInputFormat.INPUT_DIR));
  }
}
