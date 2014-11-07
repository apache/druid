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

  $("#enable_dialog").dialog({
      autoOpen: false,
      modal:true,
      resizeable: false,
      buttons: {
        Yes : function() {
          var selected = $('#datasources option:selected').text();
          $.ajax({
            type: 'POST',
            url:'/druid/coordinator/v1/datasources/' + selected,
            data: JSON.stringify(selected),
            contentType:"application/json; charset=utf-8",
            dataType:"text",
            error: function(xhr, status, error) {
              $("#enable_dialog").dialog("close");
              $("#error_dialog").html(xhr.responseText);
              $("#error_dialog").dialog("open");
            },
            success: function(data, status, xhr) {
              $("#enable_dialog").dialog("close");
            }
          });
        },
        Cancel: function() {
          $(this).dialog("close");
        }
      }
  });

  $("#disable_dialog").dialog({
    autoOpen: false,
    modal:true,
    resizeable: false,
    buttons: {
      Yes : function() {
        var selected = $('#datasources option:selected').text();
        $.ajax({
          type: 'DELETE',
          url:'/druid/coordinator/v1/datasources/' + selected,
          data: JSON.stringify(selected),
          contentType:"application/json; charset=utf-8",
          dataType:"text",
          error: function(xhr, status, error) {
            $("#disable_dialog").dialog("close");
            $("#error_dialog").html(xhr.responseText);
            $("#error_dialog").dialog("open");
          },
          success: function(data, status, xhr) {
            $("#disable_dialog").dialog("close");
          }
        });
      },
      Cancel: function() {
        $(this).dialog("close");
      }
    }
  });

  $.getJSON("/druid/coordinator/v1/metadata/datasources", function(enabled_datasources) {
    $.each(enabled_datasources, function(index, datasource) {
      $('#enabled_datasources').append($('<li>' + datasource + '</li>'));
    });

    $.getJSON("/druid/coordinator/v1/metadata/datasources?includeDisabled", function(db_datasources) {
      var disabled_datasources = _.difference(db_datasources, enabled_datasources);
      $.each(disabled_datasources, function(index, datasource) {
        $('#disabled_datasources').append($('<li>' + datasource + '</li>'));
      });
      $.each(db_datasources, function(index, datasource) {
        $('#datasources').append($('<option></option>').val(datasource).text(datasource));
      });
    });
  });

  $("#enable").click(function() {
    $("#enable_dialog").dialog("open");
  });

  $('#disable').click(function (){
    $("#disable_dialog").dialog("open")
  });
});
