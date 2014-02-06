// requires jQuery, druidTable, tablehelper

$(document).ready(function() {

  var basePath = "/druid/coordinator/v1/";
  var type = $('#select_type').val() + '';
  var view = $('#select_view').val() + '';

  function handleTable(dontDisplay)
  {
    $.get(basePath + type + '?full', function(data) {
      buildTable(data, $('#result_table'), dontDisplay);

      $('.loading').hide();
      $('#table_wrapper').show();
    });
  }

  function handleList(hasSegments)
  {
    $('#selected_data').html('');
    $.get(basePath + type, function(data) {
      var appendStr = '<ul>';

      _.each(data, function(entry) {
        entry += '';
        
        appendStr += '<li class="has_raw';
        if (hasSegments === true) {
          appendStr += ' has_segments';
        }
        appendStr += '"><a class="val">' + entry + '</a><span class="links"></span></li>';
      });
      appendStr += '</ul>';
      $('#selected_data').html(appendStr);

      $('.val').click(function(event) {
        var el = $(event.target);
        var links = el.siblings('.links');
        var linksStr = "";
        var rawJsonPath = type + "/" + el.text();
        $('.links').empty();

        if (hasSegments) {
          type += "/" + el.text() + "/segments";
          linksStr += '[<a class="segments_table">View Segments Table</a>]';
          linksStr += '[<a class="segments_list">View Segments List</a>]';
          linksStr += '<a target="_blank" href="' + basePath + type + '?full">[View Segment Raw JSON]</a>';
        }
        linksStr += '<a target="_blank" href="' + basePath + rawJsonPath + '">[Raw JSON]</a>';
        links.html(linksStr);

        $('.segments_table').click(function() {
          resetViews();
          handleTable();
        })

        $('.segments_list').click(function() {
          resetViews();
          handleList(false);
        })
      });

      $('.loading').hide();
      $('#selected_data').show();
    });
  }

  function handleRaw() {
    var htmlStr = '<a target="_blank" href="' + basePath + type + '?full' + '">Go to Raw JSON</a>';
    $('#selected_data').html(htmlStr);

    $('.loading').hide();
    $('#selected_data').show();
  }

  $('#view_button').click(function() {
    type = $('#select_type').val() + '';
    view = $('#select_view').val() + '';

    resetViews();

    switch (view) {
    case "table":
      var dontDisplay;
      switch (type) {
        case "servers":
        case "datasources":
          dontDisplay = ["segments"];
          break;
        case "segments":
          dontDisplay = ["shardSpec"];
          break;
      }

      handleTable(dontDisplay);
      break;
    case "list":
      handleList(type.indexOf("segments") == -1);
      break;
    case "raw":
      handleRaw();
      break;
    }
  });

  function resetViews() {
    $('.loading').show();
    $('#selected_data').hide();
    $('#table_wrapper').hide();
  }

});