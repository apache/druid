$(function () {
    $.get('../master/config', function (data) {
        document.getElementById("millis").value=data["millisToWaitBeforeDeleting"];
        document.getElementById("mergeBytes").value = data["mergeBytesLimit"];
        document.getElementById("mergeSegments").value = data["mergeSegmentsLimit"];
        document.getElementById("maxSegments").value = data["maxSegmentsToMove"];
    });

    $("#submit").click( function ()
    {
        values = {};
        list = $('form').serializeArray();
        for (var i=0;i< list.length;i++)
        {
            values[list[i]["name"]]=list[i]["value"];
        }
        $.ajax({
            url:'../master/config',
            type:"POST",
            data: JSON.stringify(values),
            contentType:"application/json; charset=utf-8",
            dataType:"json"
        });
    });
});
