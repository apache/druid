/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.common.guava;

import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.io.CharStreams;
import com.google.common.io.InputSupplier;

import javax.annotation.Nullable;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.List;
import java.util.zip.GZIPInputStream;

/**
 */
public class GuavaUtils
{
  public static Function<String, String> formatFunction(final String formatString)
  {
    return new Function<String, String>()
    {
      @Override
      public String apply(@Nullable String input)
      {
        return String.format(formatString, input);
      }
    };
  }

  public static InputSupplier<BufferedReader> joinFiles(final File... files)
  {
    return joinFiles(Arrays.asList(files));
  }

  public static InputSupplier<BufferedReader> joinFiles(final List<File> files)
  {

    return new InputSupplier<BufferedReader>()
    {
      @Override
      public BufferedReader getInput() throws IOException
      {
        return new BufferedReader(
            CharStreams.join(
                Iterables.transform(
                    files,
                    new Function<File, InputSupplier<InputStreamReader>>()
                    {
                      @Override
                      public InputSupplier<InputStreamReader> apply(final File input)
                      {
                        return new InputSupplier<InputStreamReader>()
                        {
                          @Override
                          public InputStreamReader getInput() throws IOException
                          {
                            InputStream baseStream = new FileInputStream(input);
                            if (input.getName().endsWith(".gz")) {
                              baseStream = new GZIPInputStream(baseStream);
                            }

                            return new InputStreamReader(baseStream, Charsets.UTF_8);
                          }
                        };
                      }
                    }
                )
            ).getInput()
        );
      }
    };
  }
}
