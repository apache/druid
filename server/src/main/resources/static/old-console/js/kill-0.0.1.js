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
