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

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.ParseContext;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class ASD
{

  public static void main(String[] args) throws InterruptedException, ExecutionException, TimeoutException, Exception
  {

    ListeningExecutorService executor = MoreExecutors.newDirectExecutorService();

    System.out.println(System.currentTimeMillis());
    ListenableFuture<?> f = executor.submit(new X());
    System.out.println(System.currentTimeMillis());
    Object res = f.get(1, TimeUnit.MILLISECONDS);
    System.out.println(f);
    System.out.println(res);




    for(int i=0;i<10;i++) {
    System.out.println("_ "+System.currentTimeMillis());
    DocumentBuilderFactory aa = DocumentBuilderFactory.newInstance();
    System.out.println(System.currentTimeMillis());
    DocumentBuilder a = aa.newDocumentBuilder();
    System.out.println(System.currentTimeMillis());
    }

    JsonPath p=null;
    DocumentContext a = p.parse("asd");
    Configuration cfg=Configuration.defaultConfiguration();
    byte[] bytes=null;
    ParseContext aa = JsonPath.using(Configuration.defaultConfiguration());
    aa.parseUtf8(bytes);
//    a.



  }

  static class X implements Runnable
  {

    @Override
    public void run()
    {
      try {
        Thread.sleep(1000);
      }
      catch (InterruptedException e) {

        throw new RuntimeException();

      }

    }
  }

}
