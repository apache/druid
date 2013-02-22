$(document).ready(function() {
  $.getJSON("/mmx/merger/v1/runningTasks", function(data) {
    $('#pendingTasks').append('<p>' + JSON.stringify(data) + '</p>');
  });

  $.getJSON("/mmx/merger/v1/pendingTasks", function(data) {
    $('#runningTasks').append('<p>' + JSON.stringify(data) + '</p>');
  });

  $.getJSON("/mmx/merger/v1/workers", function(data) {
    $('#workers').append('<p>' + JSON.stringify(data) + '</p>');
  });

  $.getJSON("/mmx/merger/v1/scaling", function(data) {
    $('#events').append('<p>' + JSON.stringify(data) + '</p>');
  });
});