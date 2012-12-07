var tiers = [];
var defaultDatasource = "_default";

var ruleTypes = [
  "loadByInterval",
  "loadByPeriod",
  "dropByInterval",
  "dropByPeriod",
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
      case "dropByInterval":
        retVal += makeDropByInterval(rule);
        break;
      case "dropByPeriod":
        retVal += makeDropByPeriod(rule);
        break;
      case "JSON":
        retVal += makeJSON();
        break;
      default:
        break;
    }
  }
  retVal += "</span>"
  return retVal;
}

function makeLoadByInterval(rule) {
   return "<span class='rule_label'>interval</span><input type='text' class='long_text' name='interval' " + "value='" + rule.interval + "'/>" +
          "<span class='rule_label'>replicants</span><input type='text' class='short_text' name='replicants' " + "value='" + rule.replicants + "'/>" +
          makeTiersDropdown(rule)
   ;
}

function makeLoadByPeriod(rule) {
  return "<span class='rule_label'>period</span><input type='text' name='period' " + "value='" + rule.period + "'/>" +
         "<span class='rule_label'>replicants</span><input type='text' class='short_text' name='replicants' " + "value='" + rule.replicants + "'/>" +
         makeTiersDropdown(rule)
  ;
}

function makeDropByInterval(rule) {
   return "<span class='rule_label'>interval</span><input type='text' name='interval' " + "value='" + rule.interval + "'/>";
}

function makeDropByPeriod(rule) {
   return "<span class='rule_label'>period</span><input type='text' name='period' " + "value='" + rule.period + "'/>";
}

function makeJSON() {
  return "<span class='rule_label'>JSON</span><input type='text' class='very_long_text' name='JSON'/>";
}

function makeTiersDropdown(rule) {
  var retVal = "<span class='rule_label'>tier</span><select class='tiers' name='tier'>"

  $.each(tiers, function(index, tier) {
    if (rule.tier === tier) {
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
    $.getJSON("/info/rules/" + selected, function(data) {
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
  var inputs = $($(domRule).find(".rule_body:first")).children(":not(span)");

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
          url:'/info/rules/' + selected,
          data: JSON.stringify(rules),
          contentType:"application/json; charset=utf-8",
          dataType:"json",
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

  $.getJSON("/info/tiers", function(theTiers) {
      tiers = theTiers;
  });

  $.getJSON("/info/db/datasources", function(data) {
    $.each(data, function(index, datasource) {
      $('#datasources').append($('<option></option>').attr("value", datasource).text(datasource));
    });
    $('#datasources').append($('<option></option>').attr("value", defaultDatasource).text(defaultDatasource));
  });

  $("#datasources").change(function(event) {
    getRules();
    $("#rules").show();
  });

  $(".rule_dropdown_types").live("change", function(event) {
    var newRule = {
      "type" : $(event.target).val()
    };
    var ruleBody = $(event.target).parent('.rule_dropdown').next('.rule_body');
    ruleBody.replaceWith(makeRuleBody(newRule));
  });

  $(".delete_rule").live("click", function(event) {
    $(event.target).parent(".rule").remove();
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