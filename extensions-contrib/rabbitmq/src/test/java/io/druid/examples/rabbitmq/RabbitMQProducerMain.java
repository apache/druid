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

package io.druid.examples.rabbitmq;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import io.druid.java.util.common.StringUtils;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Random;

/**
 *
 */
public class RabbitMQProducerMain
{
  public static void main(String[] args)
      throws Exception
  {
    // We use a List to keep track of option insertion order. See below.
    final List<Option> optionList = new ArrayList<Option>();

    optionList.add(OptionBuilder.withLongOpt("help")
        .withDescription("display this help message")
        .create("h"));
    optionList.add(OptionBuilder.withLongOpt("hostname")
        .hasArg()
        .withDescription("the hostname of the AMQP broker [defaults to AMQP library default]")
        .create("b"));
    optionList.add(OptionBuilder.withLongOpt("port")
        .hasArg()
        .withDescription("the port of the AMQP broker [defaults to AMQP library default]")
        .create("n"));
    optionList.add(OptionBuilder.withLongOpt("username")
        .hasArg()
        .withDescription("username to connect to the AMQP broker [defaults to AMQP library default]")
        .create("u"));
    optionList.add(OptionBuilder.withLongOpt("password")
        .hasArg()
        .withDescription("password to connect to the AMQP broker [defaults to AMQP library default]")
        .create("p"));
    optionList.add(OptionBuilder.withLongOpt("vhost")
        .hasArg()
        .withDescription("name of virtual host on the AMQP broker [defaults to AMQP library default]")
        .create("v"));
    optionList.add(OptionBuilder.withLongOpt("exchange")
        .isRequired()
        .hasArg()
        .withDescription("name of the AMQP exchange [required - no default]")
        .create("e"));
    optionList.add(OptionBuilder.withLongOpt("key")
        .hasArg()
        .withDescription("the routing key to use when sending messages [default: 'default.routing.key']")
        .create("k"));
    optionList.add(OptionBuilder.withLongOpt("type")
        .hasArg()
        .withDescription("the type of exchange to create [default: 'topic']")
        .create("t"));
    optionList.add(OptionBuilder.withLongOpt("durable")
        .withDescription("if set, a durable exchange will be declared [default: not set]")
        .create("d"));
    optionList.add(OptionBuilder.withLongOpt("autodelete")
        .withDescription("if set, an auto-delete exchange will be declared [default: not set]")
        .create("a"));
    optionList.add(OptionBuilder.withLongOpt("single")
        .withDescription("if set, only a single message will be sent [default: not set]")
        .create("s"));
    optionList.add(OptionBuilder.withLongOpt("start")
        .hasArg()
        .withDescription("time to use to start sending messages from [default: 2010-01-01T00:00:00]")
        .create());
    optionList.add(OptionBuilder.withLongOpt("stop")
        .hasArg()
        .withDescription("time to use to send messages until (format: '2013-07-18T23:45:59') [default: current time]")
        .create());
    optionList.add(OptionBuilder.withLongOpt("interval")
        .hasArg()
        .withDescription("the interval to add to the timestamp between messages in seconds [default: 10]")
        .create());
    optionList.add(OptionBuilder.withLongOpt("delay")
        .hasArg()
        .withDescription("the delay between sending messages in milliseconds [default: 100]")
        .create());

    // An extremely silly hack to maintain the above order in the help formatting.
    HelpFormatter formatter = new HelpFormatter();
    // Add a comparator to the HelpFormatter using the ArrayList above to sort by insertion order.
    //noinspection ComparatorCombinators -- don't replace with comparingInt() to preserve comments
    formatter.setOptionComparator((o1, o2) -> {
      // I know this isn't fast, but who cares! The list is short.
      //noinspection SuspiciousMethodCalls
      return Integer.compare(optionList.indexOf(o1), optionList.indexOf(o2));
    });

    // Now we can add all the options to an Options instance. This is dumb!
    Options options = new Options();
    for (Option option : optionList) {
      options.addOption(option);
    }

    CommandLine cmd = null;

    try{
      cmd = new BasicParser().parse(options, args);

    }
    catch(ParseException e){
      formatter.printHelp("RabbitMQProducerMain", e.getMessage(), options, null);
      System.exit(1);
    }

    if(cmd.hasOption("h")) {
      formatter.printHelp("RabbitMQProducerMain", options);
      System.exit(2);
    }

    ConnectionFactory factory = new ConnectionFactory();

    if(cmd.hasOption("b")){
      factory.setHost(cmd.getOptionValue("b"));
    }
    if(cmd.hasOption("u")){
      factory.setUsername(cmd.getOptionValue("u"));
    }
    if(cmd.hasOption("p")){
      factory.setPassword(cmd.getOptionValue("p"));
    }
    if(cmd.hasOption("v")){
      factory.setVirtualHost(cmd.getOptionValue("v"));
    }
    if(cmd.hasOption("n")){
      factory.setPort(Integer.parseInt(cmd.getOptionValue("n")));
    }

    String exchange = cmd.getOptionValue("e");
    String routingKey = "default.routing.key";
    if(cmd.hasOption("k")){
      routingKey = cmd.getOptionValue("k");
    }

    boolean durable = cmd.hasOption("d");
    boolean autoDelete = cmd.hasOption("a");
    String type = cmd.getOptionValue("t", "topic");
    boolean single = cmd.hasOption("single");
    int interval = Integer.parseInt(cmd.getOptionValue("interval", "10"));
    int delay = Integer.parseInt(cmd.getOptionValue("delay", "100"));

    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss", Locale.ENGLISH);
    Date stop = sdf.parse(cmd.getOptionValue("stop", sdf.format(new Date())));

    Random r = new Random();
    Calendar timer = Calendar.getInstance(Locale.ENGLISH);
    timer.setTime(sdf.parse(cmd.getOptionValue("start", "2010-01-01T00:00:00")));

    String msg_template = "{\"utcdt\": \"%s\", \"wp\": %d, \"gender\": \"%s\", \"age\": %d}";

    Connection connection = factory.newConnection();
    Channel channel = connection.createChannel();

    channel.exchangeDeclare(exchange, type, durable, autoDelete, null);

    do {
      int wp = (10 + r.nextInt(90)) * 100;
      String gender = r.nextBoolean() ? "male" : "female";
      int age = 20 + r.nextInt(70);

      String line = StringUtils.format(msg_template, sdf.format(timer.getTime()), wp, gender, age);

      channel.basicPublish(exchange, routingKey, null, StringUtils.toUtf8(line));

      System.out.println("Sent message: " + line);

      timer.add(Calendar.SECOND, interval);

      Thread.sleep(delay);
    } while((!single && stop.after(timer.getTime())));

    connection.close();
  }

}
