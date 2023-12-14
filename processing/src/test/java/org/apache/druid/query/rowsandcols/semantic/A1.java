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

package org.apache.druid.query.rowsandcols.semantic;

import org.apache.druid.query.operator.window.WindowFrame;

import java.util.ArrayList;

public class A1
{

  enum X
  {
    A
    {
      @Override
      int q()
      {
        return 11;
      }
    },
    B
    {
      @Override
      int q()
      {
        return 0;
      }
    };

    abstract int q();
  }

  public static void main(final String[] args)
  {
    System.out.println(new ArrayList(111).size());
    if (WindowFrame.PeerType.ROWS == WindowFrame.PeerType.ROWS) {
      System.out.println("EEE");
    } else {
      System.out.println("Asd");
    }

    if (true) {
      System.out.println("EEE1");
    } else {
      System.out.println("Asd1");
    }

    if (true) {
      System.out.println("EEE1");
    } else {
      System.out.println("Asd1");
    }
    System.out.println("EEE1");

    final int q = 33;
    System.out.println(q);

    final int r = X.A.q();
    System.out.println(r);
  }

  int get(final int k)
  {
    return k;

  }

}
