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

  $("#confirm_dialog").dialog({
      autoOpen: false,
      modal:true,
      resizeable: false,
      buttons: {
        Yes : function() {
          var selected = $('#datasources option:selected').text();
          var interval = $('#interval').val();
          $.ajax({
            type: 'DELETE',
            url:'/druid/coordinator/v1/datasources/' + selected +'?kill=true&interval=' + interval,
            contentType:"application/json; charset=utf-8",
            dataType:"text",
            error: function(xhr, status, error) {
              $("#confirm_dialog").dialog("close");
              $("#error_dialog").html(xhr.responseText);
              $("#error_dialog").dialog("open");
            },
            success: function(data, status, xhr) {
              $("#confirm_dialog").dialog("close");
            }
          });
        },
        Cancel: function() {
          $(this).dialog("close");
        }
      }
  });

  $.getJSON("/druid/coordinator/v1/metadata/datasources?includeDisabled", function(data) {
    $.each(data, function(index, datasource) {
      $('#datasources').append($('<option></option>').val(datasource).text(datasource));
    });
  });

  $("#confirm").click(function() {
    $("#confirm_dialog").dialog("open");
  });
});
