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

import io.druid.data.input.impl.ParseHandler;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 */
public class HadoopConfHandler implements ParseHandler<FileSplit, Map<String, Object>>
{
    private final Map<String, Object> map = new HashMap<>();
    static final Pattern pat = Pattern.compile("([^/]+)=([^/]+)");

    @Override
    public void setup(FileSplit fileSplit)
    {
        if (fileSplit != null) {
            makeSpecFromName(fileSplit.getPath());
        }
    }

    @Override
    public Map<String, Object> handle(Map<String, Object> value)
    {
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            value.put(entry.getKey(), entry.getValue());
        }
        return value;
    }

    private void makeSpecFromName(Path name)
    {
        map.clear();
        if (name == null) {
            return;
        }
        makeSpecFromName(map, name);
    }

    private void makeSpecFromName(Map<String, Object> partSpec, Path currPath)
    {
        List<String[]> kvs = new ArrayList<String[]>();
        do {
            String component = currPath.getName();
            Matcher m = pat.matcher(component);
            if (m.matches()) {
                String k = m.group(1);
                String v = m.group(2);
                String[] kv = new String[2];
                kv[0] = k;
                kv[1] = v;
                kvs.add(kv);
            }
            currPath = currPath.getParent();
        } while (currPath != null && !currPath.getName().isEmpty());

        // reverse the list since we checked the part from leaf dir to table's base dir
        for (int i = kvs.size(); i > 0; i--) {
            partSpec.put(kvs.get(i - 1)[0], kvs.get(i - 1)[1]);
        }
    }
}
