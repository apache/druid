package com.metamx.druid.master.rules;

import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.metamx.common.MapUtils;
import com.metamx.druid.master.DruidMasterHelper;
import com.metamx.druid.master.DruidMasterRuntimeParams;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.FoldController;
import org.skife.jdbi.v2.Folder3;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.HandleCallback;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

/**
 */
public class DruidMasterRuleMaker implements DruidMasterHelper
{
  private final DruidMasterRuleMakerConfig config;
  private final ObjectMapper jsonMapper;
  private final DBI dbi;

  public DruidMasterRuleMaker(DruidMasterRuleMakerConfig config, ObjectMapper jsonMapper, DBI dbi)
  {
    this.config = config;
    this.jsonMapper = jsonMapper;
    this.dbi = dbi;
  }

  @Override
  public DruidMasterRuntimeParams run(DruidMasterRuntimeParams params)
  {
    Map<String, List<Rule>> assignmentRules = dbi.withHandle(
        new HandleCallback<Map<String, List<Rule>>>()
        {
          @Override
          public Map<String, List<Rule>> withHandle(Handle handle) throws Exception
          {
            return handle.createQuery(
                String.format("SELECT dataSource, payload FROM %s", config.getRuleTable())
            ).fold(
                Maps.<String, List<Rule>>newHashMap(),
                new Folder3<Map<String, List<Rule>>, Map<String, Object>>()
                {
                  @Override
                  public Map<String, List<Rule>> fold(
                      Map<String, List<Rule>> retVal,
                      Map<String, Object> stringObjectMap,
                      FoldController foldController,
                      StatementContext statementContext
                  ) throws SQLException
                  {

                    try {
                      String dataSource = MapUtils.getString(stringObjectMap, "dataSource");
                      List<Rule> rules = jsonMapper.readValue(
                          MapUtils.getString(stringObjectMap, "payload"), new TypeReference<List<Rule>>()
                      {
                      }
                      );
                      retVal.put(dataSource, rules);
                      return retVal;
                    }
                    catch (Exception e) {
                      throw Throwables.propagate(e);
                    }
                  }
                }
            );
          }
        }
    );

    return params.buildFromExisting()
                 .withRuleMap(
                     new RuleMap(
                         assignmentRules,
                         assignmentRules.get(config.getDefaultDatasource())
                     )
                 )
                 .build();
  }
}
