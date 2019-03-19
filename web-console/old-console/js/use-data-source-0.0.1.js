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

  $("#use_dialog").dialog({
      autoOpen: false,
      modal:true,
      resizeable: false,
      buttons: {
        Yes : function() {
          var selected = $('#data_sources option:selected').text();
          $.ajax({
            type: 'POST',
            url:'/druid/coordinator/v1/datasources/' + selected,
            data: JSON.stringify(selected),
            contentType:"application/json; charset=utf-8",
            dataType:"text",
            error: function(xhr, status, error) {
              $("#use_dialog").dialog("close");
              $("#error_dialog").html(xhr.responseText);
              $("#error_dialog").dialog("open");
            },
            success: function(data, status, xhr) {
              $("#use_dialog").dialog("close");
            }
          });
        },
        Cancel: function() {
          $(this).dialog("close");
        }
      }
  });

  $("#unuse_dialog").dialog({
    autoOpen: false,
    modal:true,
    resizeable: false,
    buttons: {
      Yes : function() {
        var selected = $('#data_sources option:selected').text();
        $.ajax({
          type: 'DELETE',
          url:'/druid/coordinator/v1/datasources/' + selected,
          data: JSON.stringify(selected),
          contentType:"application/json; charset=utf-8",
          dataType:"text",
          error: function(xhr, status, error) {
            $("#unuse_dialog").dialog("close");
            $("#error_dialog").html(xhr.responseText);
            $("#error_dialog").dialog("open");
          },
          success: function(data, status, xhr) {
            $("#unuse_dialog").dialog("close");
          }
        });
      },
      Cancel: function() {
        $(this).dialog("close");
      }
    }
  });

  $.getJSON("/druid/coordinator/v1/metadata/datasources", function(used_data_sources) {
    $.each(used_data_sources, function(index, data_source) {
      $('#used_data_sources').append($('<li>' + data_source + '</li>'));
    });

    $.getJSON("/druid/coordinator/v1/metadata/datasources?includeUnused", function(all_data_sources) {
      var unused_data_sources = _.difference(all_data_sources, used_data_sources);
      $.each(unused_data_sources, function(index, data_source) {
        $('#unused_data_sources').append($('<li>' + data_source + '</li>'));
      });
      $.each(all_data_sources, function(index, data_source) {
        $('#data_sources').append($('<option></option>').val(data_source).text(data_source));
      });
    });
  });

  $("#use").click(function() {
    $("#use_dialog").dialog("open");
  });

  $("#unuse").click(function() {
    $("#unuse_dialog").dialog("open");
  });
});
