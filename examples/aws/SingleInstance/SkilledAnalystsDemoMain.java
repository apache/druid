package com.skilledanalysts.druid.demo;

import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.ServletHolder;

import com.metamx.common.lifecycle.Lifecycle;
import com.metamx.common.logger.Logger;
import com.metamx.druid.http.BrokerMain;
import com.metamx.druid.http.BrokerNode;
import com.metamx.druid.log.LogLevelAdjuster;
import com.skilledanalysts.druid.demo.http.DemoServlet;

public class SkilledAnalystsDemoMain {

	private static final Logger log = new Logger(BrokerMain.class);

	public static void main(String[] args) throws Exception
	{
		LogLevelAdjuster.register();

		Lifecycle lifecycle = new Lifecycle();

		BrokerNode bulderNode =  BrokerNode.builder().build();
		lifecycle.addManagedInstance(
				bulderNode
				);

		try {
			final Context root = new Context(bulderNode.getServer(), "/druid/v3", Context.SESSIONS);
			root.addServlet(new ServletHolder(new DemoServlet()), "/demoServlet");
			lifecycle.start();
		}
		catch (Throwable t) {
			log.info(t, "Throwable caught at startup, committing seppuku");
			System.exit(2);
		}
		lifecycle.join();
	}
}
