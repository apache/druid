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

var configs = [];

function makeConfigDiv(key, value) {
  var retVal = "<div class='config'>";

  retVal += "<span class='config_label'>" + key + "</span>";
  retVal += "<input type='text' class='value' name='value' value='" + value + "'/>";

  retVal += "</div>";
  return retVal;
}

function domToConfig(configDiv) {
  var retVal = {};

  retVal.key = $($(configDiv).find(".config_label")).text();
  retVal.value = $($(configDiv).find(".value")).val();

  return retVal;
}

function getConfigs() {
  $.getJSON("/druid/coordinator/v1/config", function(data) {
    $('#config_list').empty();

    $.each(data, function (key, value) {
      $('#config_list').append(makeConfigDiv(key, value));
    });
  });
}

$(document).ready(function() {
  $("button").button();

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
          getConfigs();
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
        var configs = {};
        $.each($("#config_list").children(), function(index, configDiv) {
          var config = domToConfig(configDiv);
          configs[config.key] = config.value;
        });

        $.ajax({
          type: 'POST',
          url:'/druid/coordinator/v1/config',
          data: JSON.stringify(configs),
          contentType:"application/json; charset=utf-8",
          dataType:"text",
          error: function(xhr, status, error) {
            $("#update_dialog").dialog("close");
            $("#error_dialog").html(xhr.responseText);
            $("#error_dialog").dialog("open");
          },
          success: function(data, status, xhr) {
            getConfigs();
            $("#update_dialog").dialog("close");
          }
        });
      },
      Cancel: function() {
        $(this).dialog("close");
      }
    }
  });

  getConfigs();

  $("#cancel").click(function() {
    $("#cancel_dialog").dialog("open");
  });

  $('#update').click(function (){
    $("#update_dialog").dialog("open")
  });
});