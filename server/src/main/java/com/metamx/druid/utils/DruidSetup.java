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

import com.google.common.io.Closeables;
import com.metamx.common.logger.Logger;
import com.metamx.druid.db.DatabaseRuleManager;
import com.metamx.druid.db.DbConnector;
import com.metamx.druid.db.DbConnectorConfig;
import com.metamx.druid.initialization.Initialization;
import com.metamx.druid.jackson.DefaultObjectMapper;
import com.metamx.druid.zk.PropertiesZkSerializer;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;

import java.io.*;
import java.util.Properties;

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
  private static final Logger log = new Logger(DruidSetup.class);

  public static void main(final String[] args)
  {
    ZkClient zkClient = null;

    if (args.length < 2 || args.length > 3) {
      printUsage();
      System.exit(1);
    }
    String cmd = args[0];
    if ("dump".equals(cmd) && args.length == 3) {
      final String zkConnect = args[1];
      zkClient = connectToZK(zkConnect);
      String zpathBase = args[2];
      dumpFromZk(zkClient, zpathBase, zkConnect, System.out);
    } else if ("put".equals(cmd) && args.length == 3) {
      final String zkConnect = args[1];
      zkClient = connectToZK(zkConnect);
      final String pfile = args[2];
      putToZk(zkClient, pfile);
    } else if ("dbprep".equals(cmd) && args.length == 2) {
      final String pfile = args[1];
      prepDB(pfile);
    } else {
      printUsage();
      System.exit(1);
    }

    if (zkClient != null) {
      zkClient.close();
    }
  }

  /**
   * Load properties from local file, validate and tweak.
   * <p/>
   * This can only be used for setup, not service run time because of some assembly here.
   *
   * @param pfile path to runtime.properties file to be read.
   * @param props Properties object to fill, props like druid.zk.paths.*Path will always be set after
   *              this method either because the input file has them set (overrides) or because prop
   *              druid.zk.paths.base was used as a prefix to construct the default zpaths;
   *              druid.zk.paths.base will be set iff there is a single base for all zpaths
   */
  private static void loadProperties(String pfile, Properties props)
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
    catch (IOException ioe) {
      reportErrorAndExit(pfile, ioe);
    }
    try {
      props.load(is);
    }
    catch (IOException e) {
      reportErrorAndExit(pfile, e);
    }
    finally {
      Closeables.closeQuietly(is);
    }

    if (!Initialization.validateResolveProps(props)) { // bail, errors have been emitted
      System.exit(9);
    }

    //  emit effective zpaths to be used
    System.out.println("Effective zpath properties:");
    for (String pname : Initialization.SUB_PATH_PROPS) {
      System.out.println("  " + pname + "=" + props.getProperty(pname));
    }
    System.out.println(
        "  " + "druid.zk.paths.propertiesPath" + "=" +
        props.getProperty("druid.zk.paths.propertiesPath")
    );

  }

  /**
   * @param zkClient  zookeeper client.
   * @param zpathBase znode base path.
   * @param zkConnect ZK coordinates in the form host1:port1[,host2:port2[, ...]]
   * @param out
   */
  private static void dumpFromZk(ZkClient zkClient, String zpathBase, String zkConnect, PrintStream out)
  {
    final String propPath = Initialization.makePropPath(zpathBase);
    if (zkClient.exists(propPath)) {
      Properties currProps = zkClient.readData(propPath, true);
      if (currProps != null) {
        out.println("# Begin Properties Listing for zkConnect=" + zkConnect + " zpath=" + propPath);
        try {
          currProps.store(out, "Druid");
        }
        catch (IOException ignored) {
        }
        out.println("# End Properties Listing for zkConnect=" + zkConnect + " zpath=" + propPath);
        out.println("# NOTE:  properties like druid.zk.paths.*Path are always stored in zookeeper in absolute form.");
        out.println();
      }
    }
    //out.println("Zookeeper znodes and zpaths for " + zkConnect + " (showing all zpaths)");
    // list all znodes
    //   (not ideal since recursive listing starts at / instead of at baseZkPath)
    //zkClient.showFolders(out);
  }

  /**
   * @param zkClient zookeeper client.
   * @param pfile
   */
  private static void putToZk(ZkClient zkClient, String pfile)
  {
    Properties props = new Properties();
    loadProperties(pfile, props);
    String zpathBase = props.getProperty("druid.zk.paths.base");

    // create znodes first
    //
    createZNodes(zkClient, zpathBase, System.out);

    // put props
    //
    updatePropertiesZK(zkClient, zpathBase, props, System.out);
  }

  /**
   * @param zkClient  zookeeper client.
   * @param zpathBase znode base path.
   * @param props     the properties to store.
   * @param out       the PrintStream for human readable update summary (usually System.out).
   */
  private static void updatePropertiesZK(ZkClient zkClient, String zpathBase, Properties props, PrintStream out)
  {
    final String propPathOverride = props.getProperty("druid.zk.paths.propertiesPath");
    final String propPathConstructed = Initialization.makePropPath(zpathBase);
    final String propPath = (propPathOverride != null) ? propPathOverride : propPathConstructed;
    Properties currProps = null;
    if (zkClient.exists(propPath)) {
      currProps = zkClient.readData(propPath, true);
    }
    boolean propsDiffer = false;
    if (currProps == null) {
      out.println("No properties currently stored in zk");
      propsDiffer = true;
    } else { // determine whether anything is different
      int countNew = 0;
      int countDiffer = 0;
      int countRemoved = 0;
      int countNoChange = 0;
      String currMetaPropVal = "";
      StringBuilder changes = new StringBuilder(1024);
      for (String pname : props.stringPropertyNames()) {
        if (pname.equals(PropertiesZkSerializer.META_PROP)) {
          continue; // ignore meta prop datestamp, if any
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
            changes.append("CHANGED: ").append(pname).append("=  PREV=").append(pvalueCurr)
                   .append("   NOW=").append(pvalue).append("\n");
          }
        }
      }
      for (String pname : currProps.stringPropertyNames()) {
        if (pname.equals(PropertiesZkSerializer.META_PROP)) {
          currMetaPropVal = currProps.getProperty(pname);
          continue; // ignore meta prop datestamp
        }
        if (props.getProperty(pname) == null) {
          countRemoved++;
          changes.append("REMOVED: ").append(pname).append("=").append(currProps.getProperty(pname)).append("\n");
        }
      }
      if (countNew + countRemoved + countDiffer > 0) {
        out.println(
            "Current properties differ: "
            + countNew + " new,  "
            + countDiffer + " different values, "
            + countRemoved + " removed, "
            + countNoChange + " unchanged, "
            + currMetaPropVal + " previously updated"
        );
        out.println(changes);
        propsDiffer = true;
      } else {
        out.println("Current properties identical to file given, entry count=" + countNoChange);
      }
    }
    if (propsDiffer) {
      if (currProps != null) {
        zkClient.delete(propPath);
      }
      // update zookeeper
      zkClient.createPersistent(propPath, props);
      out.println("Properties updated, entry count=" + props.size());
    }
  }

  /**
   * @param zkClient  zookeeper client.
   * @param zpathBase znode base path.
   * @param out       the PrintStream for human readable update summary.
   */
  private static void createZNodes(ZkClient zkClient, String zpathBase, PrintStream out)
  {
    zkClient.createPersistent(zpathBase, true);
    for (String subPath : Initialization.SUB_PATHS) {
      final String thePath = String.format("%s/%s", zpathBase, subPath);
      if (zkClient.exists(thePath)) {
        out.printf("Path[%s] exists already%n", thePath);
      } else {
        out.printf("Creating ZK path[%s]%n", thePath);
        zkClient.createPersistent(thePath, true);
      }
    }
  }

  private static void reportErrorAndExit(String pfile, IOException ioe)
  {
    System.err.println("Could not read file: " + pfile);
    System.err.println("  because of: " + ioe);
    System.err.println("No changes made.");
    System.exit(4);
  }

  private static ZkClient connectToZK(String zkConnect)
  {
    return new ZkClient(
        new ZkConnection(zkConnect),
        Integer.MAX_VALUE,
        new PropertiesZkSerializer()
    );
  }

  /**
   * Connect to db and create table, if it does not exist.
   * NOTE: Connection failure only shows up in log output.
   *
   * @param pfile path to properties file to use.
   */
  private static void prepDB(final String pfile)
  {
    Properties tmp_props = new Properties();
    loadProperties(pfile, tmp_props);
    final String tableName = tmp_props.getProperty("druid.database.segmentTable", "prod_segments");
    final String ruleTableName = tmp_props.getProperty("druid.database.ruleTable", "prod_rules");

    final String dbConnectionUrl = tmp_props.getProperty("druid.database.connectURI");
    final String username = tmp_props.getProperty("druid.database.user");
    final String password = tmp_props.getProperty("druid.database.password");
    final String defaultDatasource = tmp_props.getProperty("druid.database.defaultDatasource", "_default");

    //
    //   validation
    //
    if (tableName.length() == 0 || !Character.isLetter(tableName.charAt(0))) {
      throw new RuntimeException("poorly formed property druid.database.segmentTable=" + tableName);
    }
    if (ruleTableName.length() == 0 || !Character.isLetter(ruleTableName.charAt(0))) {
      throw new RuntimeException("poorly formed property druid.database.ruleTable=" + ruleTableName);
    }
    if (username == null || username.length() == 0) {
      throw new RuntimeException("poorly formed property druid.database.user=" + username);
    }
    if (password == null || password.length() == 0) {
      throw new RuntimeException("poorly formed property druid.database.password=" + password);
    }
    if (dbConnectionUrl == null || dbConnectionUrl.length() == 0) {
      throw new RuntimeException("poorly formed property druid.database.connectURI=" + dbConnectionUrl);
    }

    final DbConnectorConfig config = new DbConnectorConfig()
    {
      @Override
      public String getDatabaseConnectURI()
      {
        return dbConnectionUrl;
      }

      @Override
      public String getDatabaseUser()
      {
        return username;
      }

      @Override
      public String getDatabasePassword()
      {
        return password;
      }

      @Override
      public String getSegmentTable()
      {
        return tableName;
      }
    };

    DbConnector dbConnector = new DbConnector(config);

    DbConnector.createSegmentTable(dbConnector.getDBI(), tableName);
    DbConnector.createRuleTable(dbConnector.getDBI(), ruleTableName);
    DatabaseRuleManager.createDefaultRule(
        dbConnector.getDBI(),
        ruleTableName,
        defaultDatasource,
        new DefaultObjectMapper()
    );
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
        + "    dbprep propfile              # create metadata table in db\n"
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
