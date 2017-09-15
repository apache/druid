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

package io.druid.indexer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;


public class HadoopConfHandlerTest
{
    @Test
    public void testHandle()
    {
        FileSplit fileSplit = new FileSplit(new Path("root_path/dt=20170905"), 0, 0, null);
        Map<String, Object> prevMap = new HashMap<>();

        HadoopConfHandler hadoopConfHandler = new HadoopConfHandler();
        hadoopConfHandler.setup(fileSplit);

        Map<String, Object> handle = hadoopConfHandler.handle(prevMap);

        Assert.assertEquals(handle.get("dt"), "20170905");
    }
}
