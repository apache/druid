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

package org.apache.druid.testing.embedded.server;

import org.junit.jupiter.api.Test;
import org.w3c.dom.Document;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathFactory;

import java.io.ByteArrayInputStream;
import java.io.StringWriter;

public class Snippet
{
  @Test
  public void asd() throws Exception {
    DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
    Document d=builder.parse(new ByteArrayInputStream("<x><asd>xx</asd><xxx>y</xxx></x>".getBytes()));
    XPath xf= XPathFactory.newInstance().newXPath();
    XPathExpression aa = xf.compile("/x/asd");
    String res = aa.evaluate(d);
    System.out.println(res);

    StringWriter writer = new StringWriter();
    Transformer transformer = TransformerFactory.newInstance().newTransformer();
    transformer.transform(new DOMSource(d), new StreamResult(writer));
    String xml = writer.toString();
    System.out.println(xml);
    System.out.println(d.getChildNodes());


//    Node n = d.getParentNode();
//    XPath xf= XPathFactory.newInstance().newXPath();
//    XPathExpression aa = xf.compile("asd");
//    x
//
//    d.eval
//    d.getElementsByTagName(null)
//    n.get
//    String path = ".asd";
//    String input="{asd:1,xxx:32}";
//    ParseContext a = JsonPath.compile(path).using(Configuration.defaultConfiguration());
//
//    Object u = JsonPath.compile(path).read(input);
//    Object v = a.parseUtf8(input.getBytes());
//
//    System.out.println(u);
//    System.out.println(v);
//
//    "asd".charAt(0)
  }

}

