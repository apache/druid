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

package io.druid.benchmark;

import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteSink;
import io.druid.benchmark.datagen.BenchmarkColumnSchema;
import io.druid.benchmark.datagen.BenchmarkColumnValueGenerator;
import io.druid.segment.column.ValueType;
import io.druid.segment.data.CompressedObjectStrategy;
import io.druid.segment.data.CompressionFactory;
import io.druid.segment.data.FloatSupplierSerializer;
import io.druid.segment.data.TmpFileIOPeon;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FloatCompressionBenchmarkFileGenerator
{
  public static final int ROW_NUM = 5000000;
  public static final List<CompressedObjectStrategy.CompressionStrategy> compressions =
      ImmutableList.of(
          CompressedObjectStrategy.CompressionStrategy.LZ4,
          CompressedObjectStrategy.CompressionStrategy.NONE
      );

  private static String dirPath = "floatCompress/";

  public static void main(String[] args) throws IOException, URISyntaxException
  {
    if (args.length >= 1) {
      dirPath = args[0];
    }

    BenchmarkColumnSchema enumeratedSchema = BenchmarkColumnSchema.makeEnumerated("", ValueType.FLOAT, true, 1, 0d,
                                                                                  ImmutableList.<Object>of(
                                                                                      0f,
                                                                                      1.1f,
                                                                                      2.2f,
                                                                                      3.3f,
                                                                                      4.4f
                                                                                  ),
                                                                                  ImmutableList.of(
                                                                                      0.95,
                                                                                      0.001,
                                                                                      0.0189,
                                                                                      0.03,
                                                                                      0.0001
                                                                                  )
    );
    BenchmarkColumnSchema zipfLowSchema = BenchmarkColumnSchema.makeZipf(
        "",
        ValueType.FLOAT,
        true,
        1,
        0d,
        -1,
        1000,
        1d
    );
    BenchmarkColumnSchema zipfHighSchema = BenchmarkColumnSchema.makeZipf(
        "",
        ValueType.FLOAT,
        true,
        1,
        0d,
        -1,
        1000,
        3d
    );
    BenchmarkColumnSchema sequentialSchema = BenchmarkColumnSchema.makeSequential(
        "",
        ValueType.FLOAT,
        true,
        1,
        0d,
        1470187671,
        2000000000
    );
    BenchmarkColumnSchema uniformSchema = BenchmarkColumnSchema.makeContinuousUniform(
        "",
        ValueType.FLOAT,
        true,
        1,
        0d,
        0,
        1000
    );

    Map<String, BenchmarkColumnValueGenerator> generators = new HashMap<>();
    generators.put("enumerate", new BenchmarkColumnValueGenerator(enumeratedSchema, 1));
    generators.put("zipfLow", new BenchmarkColumnValueGenerator(zipfLowSchema, 1));
    generators.put("zipfHigh", new BenchmarkColumnValueGenerator(zipfHighSchema, 1));
    generators.put("sequential", new BenchmarkColumnValueGenerator(sequentialSchema, 1));
    generators.put("uniform", new BenchmarkColumnValueGenerator(uniformSchema, 1));

    File dir = new File(dirPath);
    dir.mkdir();

    // create data files using BenchmarkColunValueGenerator
    for (Map.Entry<String, BenchmarkColumnValueGenerator> entry : generators.entrySet()) {
      final File dataFile = new File(dir, entry.getKey());
      dataFile.delete();
      try (Writer writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(dataFile)))) {
        for (int i = 0; i < ROW_NUM; i++) {
          writer.write((Float) entry.getValue().generateRowValue() + "\n");
        }
      }
    }

    // create compressed files using all combinations of CompressionStrategy and FloatEncoding provided
    for (Map.Entry<String, BenchmarkColumnValueGenerator> entry : generators.entrySet()) {
      for (CompressedObjectStrategy.CompressionStrategy compression : compressions) {
        String name = entry.getKey() + "-" + compression.toString();
        System.out.print(name + ": ");
        File compFile = new File(dir, name);
        compFile.delete();
        File dataFile = new File(dir, entry.getKey());

        TmpFileIOPeon iopeon = new TmpFileIOPeon(true);
        FloatSupplierSerializer writer = CompressionFactory.getFloatSerializer(
            iopeon,
            "float",
            ByteOrder.nativeOrder(),
            compression
        );
        BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(dataFile)));

        try (FileChannel output = FileChannel.open(
            compFile.toPath(),
            StandardOpenOption.CREATE_NEW,
            StandardOpenOption.WRITE
        )) {
          writer.open();
          String line;
          while ((line = br.readLine()) != null) {
            writer.add(Float.parseFloat(line));
          }
          final ByteArrayOutputStream baos = new ByteArrayOutputStream();
          writer.closeAndConsolidate(
              new ByteSink()
              {
                @Override
                public OutputStream openStream() throws IOException
                {
                  return baos;
                }
              }
          );
          output.write(ByteBuffer.wrap(baos.toByteArray()));
        }
        finally {
          iopeon.cleanup();
          br.close();
        }
        System.out.print(compFile.length() / 1024 + "\n");
      }
    }
  }
}
