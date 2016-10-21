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

package io.druid.java.util.common.io.smoosh;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import io.druid.java.util.common.collect.Utils;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 */
public class Smoosh
{
  public static Map<String, File> smoosh(File inDir, File outDir) throws IOException
  {
    final List<File> files = Arrays.asList(inDir.listFiles());
    return smoosh(
        inDir,
        outDir,
        Utils.zipMap(
            Iterables.transform(
                files,
                new Function<File, String>()
                {
                  @Override
                  public String apply(File input)
                  {
                    return input.getName();
                  }
                }
            ),
            files
        )
    );
  }

  public static Map<String, File> smoosh(File inDir, File outDir, Map<String, File> filesToSmoosh) throws IOException
  {
    FileSmoosher smoosher = new FileSmoosher(outDir);
    try {
      for (Map.Entry<String, File> entry : filesToSmoosh.entrySet()) {
        smoosher.add(entry.getKey(), entry.getValue());
      }
    }
    finally {
      smoosher.close();
    }

    return filesToSmoosh;
  }

  public static void smoosh(File outDir, Map<String, ByteBuffer> bufferstoSmoosh)
      throws IOException
  {
    FileSmoosher smoosher = new FileSmoosher(outDir);
    try {
      for (Map.Entry<String, ByteBuffer> entry : bufferstoSmoosh.entrySet()) {
        smoosher.add(entry.getKey(), entry.getValue());
      }
    }
    finally {
      smoosher.close();
    }
  }

  public static SmooshedFileMapper map(File inDir) throws IOException
  {
    return SmooshedFileMapper.load(inDir);
  }
}
