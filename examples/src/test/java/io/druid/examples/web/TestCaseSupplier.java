/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
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

package io.druid.examples.web;

import com.google.common.io.InputSupplier;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;

public class TestCaseSupplier implements InputSupplier<BufferedReader>
{
  private final String testString;

  public TestCaseSupplier(String testString)
  {
    this.testString = testString;
  }

  @Override
  public BufferedReader getInput() throws IOException
  {
    return new BufferedReader(new StringReader(testString));
  }
}
