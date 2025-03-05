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

package org.apache.druid.k8s.overlord.common;

import org.apache.commons.io.IOUtils;
import org.apache.druid.java.util.common.StringUtils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class Base64Compression
{
  public static String compressBase64(String srcTxt) throws IOException
  {
    ByteArrayOutputStream rstBao = new ByteArrayOutputStream();
    try (GZIPOutputStream zos = new GZIPOutputStream(rstBao)) {
      zos.write(srcTxt.getBytes(StandardCharsets.UTF_8));
    }
    byte[] bytes = rstBao.toByteArray();
    return StringUtils.encodeBase64String(bytes);
  }

  public static String decompressBase64(String zippedBase64Str) throws IOException
  {
    byte[] bytes = StringUtils.decodeBase64String(zippedBase64Str);
    try (GZIPInputStream zi = new GZIPInputStream(new ByteArrayInputStream(bytes))) {
      return IOUtils.toString(zi, StandardCharsets.UTF_8);
    }
  }
}
