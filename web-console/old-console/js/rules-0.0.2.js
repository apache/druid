/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

var tiers = [];
var defaultDatasource = "_default";

var ruleTypes = [
  "loadByInterval",
  "loadByPeriod",
  "dropByInterval",
  "dropByPeriod",
  "dropBeforeByPeriod",
  "loadForever",
  "dropForever",
  "JSON"
];

function makeRuleDiv(rule) {
  var retVal = "<div class='rule'>";

  if (rule === null || typeof rule === "undefined") {
    retVal += makeRuleComponents("") + makeRuleBody();
  } else {
    retVal += makeRuleComponents(rule.type) + makeRuleBody(rule);
  }

  retVal += "</div>";
  return retVal;
}

function makeRuleComponents(type) {
  var components = "<button type='button' class='delete_rule'>X</button>";
  components +=
  "<span class='rule_dropdown'>" +
    "<span class='rule_label'>type</span>" +
      "<select class='rule_dropdown_types'>" +
        "<option></option>";

  $.each(ruleTypes, function(index, ruleType) {
    if (ruleType === type) {
      components += "<option selected='selected' value='" + ruleType + "'>" + ruleType + "</option>";
    } else {
      components += "<option value='" + ruleType + "'>" + ruleType + "</option>";
    }
  });

  components +=
    "</select>" +
  "</span>";

  return components;
}

function makeRuleBody(rule) {
  var retVal = "<span class='rule_body'>";
  if (rule !== null && typeof rule !== "undefined") {
    switch (rule.type) {
      case "loadByInterval":
        retVal += makeLoadByInterval(rule);
        break;
      case "loadByPeriod":
        retVal += makeLoadByPeriod(rule);
        break;
      case "loadForever":
        retVal += makeLoadForever(rule);
        break;
      case "dropByInterval":
        retVal += makeDropByInterval(rule);
        break;
      case "dropByPeriod":
        retVal += makeDropByPeriod(rule);
        break;
      case "dropBeforeByPeriod":
        retVal += makeDropBeforeByPeriod(rule);
        break;
      case "dropForever":
        retVal += "";
        break;
      case "JSON":
        retVal += makeJSON();
        break;
      default:
        break;
    }
  }
  retVal += "</span>";
  return retVal;
}

function makeLoadByInterval(rule) {
  var retVal = "";
  retVal += "<span class='rule_label'>interval</span><input type='text' class='long_text' name='interval' " + "value='" + rule.interval + "'/>";
  retVal += "<button type='button' class='add_tier'>Add Another Tier</button>";
  if (rule.tieredReplicants === undefined) {
    retVal += makeTierLoad(null, 0);
  }
  for (var tier in rule.tieredReplicants) {
    retVal += makeTierLoad(tier, rule.tieredReplicants[tier]);
  }
  return retVal;
}

function makeLoadByPeriod(rule) {
  var retVal = "";
  retVal += "<span class='rule_label'>period</span><input type='text' name='period' " + "value='" + rule.period + "'/>";
  retVal += "<span class='rule_label'>includeFuture</span><input type='text' name='includeFuture' " + "value='" + true + "'/>";
  retVal += "<button type='button' class='add_tier'>Add Another Tier</button>";
  if (rule.tieredReplicants === undefined) {
    retVal += makeTierLoad(null, 0);
  }
  for (var tier in rule.tieredReplicants) {
    retVal += makeTierLoad(tier, rule.tieredReplicants[tier]);
  }
  return retVal;
}

function makeLoadForever(rule) {
  var retVal = "";
  retVal += "<button type='button' class='add_tier'>Add Another Tier</button>";
  if (rule.tieredReplicants === undefined) {
    retVal += makeTierLoad(null, 0);
  }
  for (var tier in rule.tieredReplicants) {
    retVal += makeTierLoad(tier, rule.tieredReplicants[tier]);
  }
  return retVal;
}

function makeTierLoad(tier, val) {
  return "<div class='rule_tier'>" +
         "<span class='rule_label'>replicants</span><input type='text' class='short_text' name='replicants' " + "value='" + val + "'/>" +
                      makeTiersDropdown(tier) +
         "</div>";
}

function makeDropByInterval(rule) {
  return "<span class='rule_label'>interval</span><input type='text' name='interval' " + "value='" + rule.interval + "'/>";
}

function makeDropByPeriod(rule) {
  var retVal = "";
  retVal += "<span class='rule_label'>period</span><input type='text' name='period' " + "value='" + rule.period + "'/>";
  retVal += "<span class='rule_label'>includeFuture</span><input type='text' name='includeFuture' " + "value='" + true + "'/>";
  return retVal;
}

function makeDropBeforeByPeriod(rule) {
  return "<span class='rule_label'>period</span><input type='text' name='period' " + "value='" + rule.period + "'/>";
}

function makeJSON() {
  return "<span class='rule_label'>JSON</span><input type='text' class='very_long_text' name='JSON'/>";
}

function makeTiersDropdown(selTier) {
  var retVal = "<span class='rule_label'>tier</span><select class='tiers' name='tier'>"

  if ($.inArray(selTier, tiers) == -1) {
    tiers.push(selTier);
  }

  $.each(tiers, function(index, tier) {
    if (selTier === tier) {
      retVal += "<option selected='selected' value='" + tier + "'>" + tier + "</option>";
    } else {
      retVal += "<option value='" + tier + "'>" + tier + "</option>";
    }
  });

  retVal += "</select>";
  return retVal;
}

function getRules() {
  var selected = $('#datasources option:selected').text();
  if (selected !== "") {
    $.getJSON("/druid/coordinator/v1/rules/" + selected, function(data) {
      $('#rules_list').empty();
      if (!$.isEmptyObject(data)) {
        $.each(data, function(index, rule) {
          $('#rules_list').append(makeRuleDiv(rule));
        });
      }
    });
  }
}

function domToRule(domRule) {
  var ruleType = $($(domRule).find(".rule_dropdown_types:first")).val();
  var inputs = $($(domRule).find(".rule_body:first")).children("input");

  // Special case for free form JSON
  if (ruleType === "JSON") {
    return $.parseJSON($(inputs.children("input:first")).val());
  }

  var rule = {};
  rule.type = ruleType;
  $.each(inputs, function(index, input) {
    var name = $(input).attr("name");
    rule[name] = $(input).val();
  });

  var theTiers = $($(domRule).find(".rule_body:first")).children(".rule_tier");

  var tieredReplicants = {};
  $.each(theTiers, function(index, theTier) {
    var tierName = $(theTier).find("select").val();
    var replicants = $(theTier).find("[name=replicants]").val();
    tieredReplicants[tierName] = replicants;
  });
  rule.tieredReplicants = tieredReplicants;

  return rule;
}

$(document).ready(function() {
  $("button").button();

  $("#rules_list").sortable();

  $("#error_dialog").dialog({
      autoOpen: false,
      modal:true,
      resizeable: false,
      buttons: {
        Ok : function() {
          $(this).dialog("close");
        }
      }
  });

  $("#cancel_dialog").dialog({
      autoOpen: false,
      modal:true,
      resizeable: false,
      buttons: {
        Yes : function() {
          getRules();
          $(this).dialog("close");
        },
        No: function() {
          $(this).dialog("close");
        }
      }
  });

  $("#update_dialog").dialog({
    autoOpen: false,
    modal:true,
    resizeable: false,
    buttons: {
      Yes : function() {
        var rules = [];
        $.each($("#rules_list").children(), function(index, domRule) {
          rules.push(domToRule(domRule));
        });

        var selected = $('#datasources option:selected').text();
        $.ajax({
          type: 'POST',
          url:'/druid/coordinator/v1/rules/' + selected,
          data: JSON.stringify(rules),
          contentType:"application/json; charset=utf-8",
          dataType:"text",
          error: function(xhr, status, error) {
            $("#update_dialog").dialog("close");
            $("#error_dialog").html(xhr.responseText);
            $("#error_dialog").dialog("open");
          },
          success: function(data, status, xhr) {
            $("#update_dialog").dialog("close");
          }
        });
      },
      Cancel: function() {
        $(this).dialog("close");
      }
    }
  });

  $.getJSON("/druid/coordinator/v1/tiers", function(theTiers) {
      tiers = theTiers;
  });

  $.getJSON("/druid/coordinator/v1/metadata/datasources", function(data) {
    $.each(data, function(index, datasource) {
      $('#datasources').append($('<option></option>').val(datasource).text(datasource));
    });
    $('#datasources').append($('<option></option>').val(defaultDatasource).text(defaultDatasource));
  });

  $("#datasources").change(function(event) {
    getRules();
    $("#rules").show();
  });

  $(document).on("change", '.rule_dropdown_types', null, function(event) {
    var newRule = {
      "type" : $(event.target).val()
    };
    var ruleBody = $(event.target).parent('.rule_dropdown').next('.rule_body');
    ruleBody.replaceWith(makeRuleBody(newRule));
  });

  $(document).on("click", '.delete_rule', null, function(event) {
    $(event.target).parent(".rule").remove();
  });

  $(document).on("click", '.add_tier', null, function(event) {
    $(event.target).parent().append(makeTierLoad(null, 0));
  });

  $("#create_new_rule").click(function (event) {
    $('#rules_list').prepend(makeRuleDiv());
  });

  $("#cancel").click(function() {
    $("#cancel_dialog").dialog("open");
  });

  $('#update').click(function (){
    $("#update_dialog").dialog("open")
  });
});
