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

package com.metamx.druid.utils;

import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.collect.Sets;
import com.google.common.io.Closeables;
import com.metamx.common.config.Config;
import com.metamx.druid.initialization.ZkPathsConfig;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.skife.config.ConfigurationObjectFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.util.Properties;
import java.util.Set;

/**
 * Set up the shared Druid ensemble space.
 * This affects the Zookeeper which holds common properties, and znode paths for coordination,
 * and also performs metadata table creation in the database (MySQL).
 * By storing ensemble-wide properties in zookeeper, cluster administration is simplified.
 * Each service instance can also have local property overrides in the file runtime.properties
 * located in the classpath.
 * <p>
 * The design rules are noted here with rationale
 * </p>
 * <p/>
 * <pre>
 * Design Rule Notes:
 * (a) Properties set on the commandline of services take precedence over  runtime.properties which
 *       takes precedence over properties stored in zookeeper.
 *
 *       Rationale:  organizing principle.
 *
 * (a) Services load properties on startup only.
 *
 *       Rationale: stepwise changes are safer and easier to manage.
 *
 * (b) Only DruidSetup creates properties and znode paths (zpaths) on zookeeper and no other tool or service
 *       will make ensemble-wide settings automatically.
 *
 *       Rationale: one place for this logic, under manual control, and avoid accidental
 *       namespace/partition creation.
 *
 * (c) DruidSetup creates reasonable zpaths but supports overrides to enable tactical
 *   version transitions (just in case).  If zpaths are overridden, then they must all be
 *   overridden together since they are not independent.
 *
 *       Rationale:  convention beats configuration most of the time; sometimes configuration is needed
 *       negotiate unusual cases.
 *
 * (d) Properties settings stored on zookeeper are not cumulative; previous properties are removed before
 *   new ones are stored.
 *       Rationale:  Keep the operations at the granularity of a file of properties, avoid
 *       dependence on order of setup operations, enable dumping of current settings.
 * </pre>
 *
 * @author pbaclace
 */
public class DruidSetup
{
  private final static String MODIFIED_PROP = "__MODIFIED";
  private final static Set<String> IGNORED_PROPS = Sets.newHashSet(MODIFIED_PROP);

  public static void main(final String[] args)
  {
    CuratorFramework curator = null;

    try {
      if (args.length < 2 || args.length > 3) {
        printUsage();
        System.exit(1);
      }
      String cmd = args[0];
      if ("dump".equals(cmd) && args.length == 3) {
        final String zkConnect = args[1];
        curator = connectToZK(zkConnect);
        curator.start();
        String zpathBase = args[2];
        dumpFromZk(curator, zkConnect, zpathBase, System.out);
      } else if ("put".equals(cmd) && args.length == 3) {
        final String zkConnect = args[1];
        curator = connectToZK(zkConnect);
        curator.start();
        final String pfile = args[2];
        putToZk(curator, pfile);
      } else {
        printUsage();
        System.exit(1);
      }
    }
    finally {
      Closeables.closeQuietly(curator);
    }
  }

  /**
   * Load properties from local file, validate and tweak.
   * <p/>
   * This can only be used for setup, not service run time because of some assembly here.
   *
   * @param pfile path to runtime.properties file to be read.
   */
  private static Properties loadProperties(String pfile)
  {
    InputStream is = null;
    try {
      is = new FileInputStream(pfile);
    }
    catch (FileNotFoundException e) {
      System.err.println("File not found: " + pfile);
      System.err.println("No changes made.");
      System.exit(4);
    }

    try {
      Properties props = new Properties();
      props.load(new InputStreamReader(is, Charsets.UTF_8));
      return props;
    }
    catch (IOException e) {
      throw reportErrorAndExit(pfile, e);
    }
    finally {
      Closeables.closeQuietly(is);
    }
  }

  /**
   * @param curator  zookeeper client.
   * @param zPathBase znode base path.
   * @param zkConnect ZK coordinates in the form host1:port1[,host2:port2[, ...]]
   * @param out
   */
  private static void dumpFromZk(CuratorFramework curator, String zkConnect, final String zPathBase, PrintStream out)
  {
    ZkPathsConfig config = new ZkPathsConfig()
    {
      @Override
      public String getZkBasePath()
      {
        return zPathBase;
      }
    };

    try {
      if (curator.checkExists().forPath(config.getPropertiesPath()) != null) {
        byte[] data = curator.getData().forPath(config.getPropertiesPath());
        Properties currProps = new Properties();
        currProps.load(new InputStreamReader(new ByteArrayInputStream(data), Charsets.UTF_8));

        if (! currProps.isEmpty()) {
          out.printf("# Begin Properties Listing for zpath[%s]%n", config.getPropertiesPath());
          try {
            currProps.store(new OutputStreamWriter(out, Charsets.UTF_8), "Druid");
          }
          catch (IOException ignored) {
          }
          out.printf("# End Properties for zkConnect[%s] zpath[%s]%n", zkConnect, config.getPropertiesPath());
        }
        else {
          out.printf("# Properties at zpath[%s] empty.%n", config.getPropertiesPath());
        }
      }
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  private static void putToZk(CuratorFramework curator, String pfile)
  {
    final Properties props = loadProperties(pfile);
    ConfigurationObjectFactory configFactory = Config.createFactory(props);
    final ZkPathsConfig zkPaths = configFactory.build(ZkPathsConfig.class);

    createZNodes(curator, zkPaths, System.out);
    updatePropertiesZK(curator, zkPaths, props, System.out);
  }

  /**
   * @param curator  zookeeper client.
   * @param zkPaths znode base path.
   * @param props     the properties to store.
   * @param out       the PrintStream for human readable update summary (usually System.out).
   */
  private static void updatePropertiesZK(CuratorFramework curator, ZkPathsConfig zkPaths, Properties props, PrintStream out)
  {
    Properties currProps = new Properties();
    try {
      if (curator.checkExists().forPath(zkPaths.getPropertiesPath()) != null) {
        final byte[] data = curator.getData().forPath(zkPaths.getPropertiesPath());
        currProps.load(new InputStreamReader(new ByteArrayInputStream(data), Charsets.UTF_8));
      }
      boolean propsDiffer = false;
      if (currProps.isEmpty()) {
        out.println("No properties currently stored in zk");
        propsDiffer = true;
      } else { // determine whether anything is different
        int countNew = 0;
        int countDiffer = 0;
        int countRemoved = 0;
        int countNoChange = 0;
        StringBuilder changes = new StringBuilder(1024);
        for (String pname : props.stringPropertyNames()) {
          if (IGNORED_PROPS.contains(pname)) {
            continue; // ignore meta props, if any
          }
          final String pvalue = props.getProperty(pname);
          final String pvalueCurr = currProps.getProperty(pname);
          if (pvalueCurr == null) {
            countNew++;
          } else {
            if (pvalueCurr.equals(pvalue)) {
              countNoChange++;
            } else {
              countDiffer++;
              changes.append(String.format("CHANGED[%s]: PREV=%s --- NOW=%s%n", pname, pvalueCurr, pvalue));
            }
          }
        }
        for (String pname : currProps.stringPropertyNames()) {
          if (IGNORED_PROPS.contains(pname)) {
            continue; // ignore meta props, if any
          }
          if (props.getProperty(pname) == null) {
            countRemoved++;
            changes.append(String.format("REMOVED: %s=%s%n", pname, currProps.getProperty(pname)));
          }
        }
        if (countNew + countRemoved + countDiffer > 0) {
          out.printf(
              "Properties differ: %,d new,  %,d changed, %,d removed, %,d unchanged, previously updated %s%n",
              countNew, countDiffer, countRemoved, countNoChange, currProps.getProperty(MODIFIED_PROP)
          );
          out.println(changes);
          propsDiffer = true;
        } else {
          out.printf("Current properties identical to file given, %,d total properties set.%n", countNoChange);
        }
      }
      if (propsDiffer) {
        ByteArrayOutputStream propsBytes = new ByteArrayOutputStream();
        props.store(new OutputStreamWriter(propsBytes, Charsets.UTF_8), "Common Druid properties");

        if (currProps.isEmpty()) {
          curator.setData().forPath(zkPaths.getPropertiesPath(), propsBytes.toByteArray());
        }
        else {
          curator.create().forPath(zkPaths.getPropertiesPath(), propsBytes.toByteArray());
        }
        out.printf("Properties updated, %,d total properties set.%n", props.size());
      }
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * @param curator  zookeeper client.
   * @param zkPaths znode base path.
   * @param out       the PrintStream for human readable update summary.
   */
  private static void createZNodes(CuratorFramework curator, ZkPathsConfig zkPaths, PrintStream out)
  {
    createPath(curator, zkPaths.getAnnouncementsPath(), out);
    createPath(curator, zkPaths.getMasterPath(), out);
    createPath(curator, zkPaths.getLoadQueuePath(), out);
    createPath(curator, zkPaths.getServedSegmentsPath(), out);
    createPath(curator, zkPaths.getPropertiesPath(), out);
  }

  private static void createPath(CuratorFramework curator, String thePath, PrintStream out)
  {
    try {
      if (curator.checkExists().forPath(thePath) != null) {
        out.printf("Path[%s] exists already%n", thePath);
      } else {
        out.printf("Creating ZK path[%s]%n", thePath);
        curator.create().creatingParentsIfNeeded().forPath(thePath);
      }
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  private static RuntimeException reportErrorAndExit(String pfile, IOException ioe)
  {
    System.err.println("Could not read file: " + pfile);
    System.err.println("  because of: " + ioe);
    System.err.println("No changes made.");
    System.exit(4);

    return new RuntimeException();
  }

  private static CuratorFramework connectToZK(String zkConnect)
  {
    return CuratorFrameworkFactory.builder()
                                  .connectString(zkConnect)
                                  .retryPolicy(new RetryOneTime(5000))
                                  .build();
  }

  /**
   * Print usage to stdout.
   */
  private static void printUsage()
  {
    System.out.println(
        "Usage: <java invocation> CMD [args]\n"
        + "  Where CMD is a particular command:\n"
        + "  CMD choices:\n"
        + "    dump zkConnect baseZkPath    # dump info from zk at given coordinates\n"
        + "    put zkConnect  propfile      # store paths and propfile into zk at given coordinates\n"
        + "  args:\n"
        + "    zkConnect:  ZK coordinates in the form host1:port1[,host2:port2[, ...]]\n"
        + "    baseZkPath:  like /druid or /mydruid etc. to uniquely identify a Druid ensemble\n"
        + "                   and should be equal to property druid.zk.paths.base\n"
        + "    propfile:  Java properties file with common properties for all services in ensemble\n"
        + "  Notes:\n"
        + "    dump command makes no modifications and shows zk properties at baseZkPath.\n"
        + "    put command can safely be invoked more than once, will not disturb existing queues,\n"
        + "              and properties are not cumulative.\n"
        + "    A zookeeper can service more than one Druid ensemble if baseZkPath is distinct.\n"
        + "    Druid services only load properties during process startup.\n"
        + "    Properties defined on a service command line take precedence over the runtime.properties\n"
        + "              file which takes precedence over properties stored in zookeeper.\n"
        + ""
    );
  }
}
