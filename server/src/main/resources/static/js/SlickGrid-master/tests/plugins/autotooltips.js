(function($) {

  var grid,         // The SlickGrid instance
      cols = [      // The column definitions
        { name: "Short",  field: "short",   width: 100 },
        { name: "Medium", field: "medium",  width: 100 },
        { name: "Long",   field: "long",    width: 100 },
        { name: "Mixed",  field: "mixed",   width: 100 },
        { name: "Long header creates tooltip",         field: "header",        width: 50 },
        { name: "Long header with predefined tooltip", field: "tooltipHeader", width: 50, tooltip: "Already have a tooltip!" }
      ],
      data            = [], // The grid data
      LONG_VALUE      = "looooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooong",
      MEDIUM_VALUE    = "mediummmmmmm"
      SHORT_VALUE     = "short",
      TOOLTIP         = "tooltip",
      TRUNCATED_VALUE = LONG_VALUE.substr(0, 17) + "...",
      $container      = $("#container");
  
  // Create data
  for (var i = 0; i < 10; i++) {
    data.push({
      "id":             "row" + i,
      "short":          SHORT_VALUE,
      "medium":         MEDIUM_VALUE,
      "long":           LONG_VALUE,
      "mixed":          ( i % 2 == 0 ? SHORT_VALUE : LONG_VALUE ),
      "header":         i,
      "tooltipHeader":  i
    });
  }
  
  function setupGrid(pluginOptions) {
    $('<div id="grid" />').appendTo($container);
    grid = new Slick.Grid("#grid", data, cols);
    grid.registerPlugin(new Slick.AutoTooltips(pluginOptions));
    grid.render();
  }
  
  function teardownGrid() {
    $container.empty();
  }
  
  function getCell(columnIndex) {
    return $("#grid .slick-cell.l" + columnIndex).eq(0);
  }
  
  function getHeaderCell(columnIndex) {
    return $("#grid .slick-header-column").eq(columnIndex);
  }
  
  module("plugins - autotooltips - defaults", {
    setup: function () {
      setupGrid({});
    },
    teardown: teardownGrid
  });
  
  test("title is empty when cell text has enough room", function () {
    var $cell = getCell(0);       // Grab a cell
    $cell.trigger("mouseenter");  // Trigger hover on a cell in grid
    
    strictEqual($cell.attr("title"), "", "title is not present");
  });
  
  test("title is present when cell text is cut off", function () {
    var $cell = getCell(2);       // Grab a cell
    $cell.trigger("mouseenter");  // Trigger hover on a cell in grid
    
    strictEqual($cell.attr("title"), LONG_VALUE, "title equals cell text");
  });
  
  module("plugins - autotooltips - header", {
    setup: function () {
      setupGrid({ enableForHeaderCells: true });
    },
    teardown: teardownGrid
  });
  
  test("title is empty when header column has enough width", function () {
    var $headerCell = getHeaderCell(0); // Grab the requested header cell
    $headerCell.trigger("mouseenter");  // Trigger hover on a header cell
    
    strictEqual($headerCell.attr("title"), "", "title is not present");
  });
  
  test("title is present when header column is cut off", function () {
    var $headerCell = getHeaderCell(4); // Grab the requested header cell
    $headerCell.trigger("mouseenter");  // Trigger hover on a header cell
    
    strictEqual($headerCell.attr("title"), "Long header creates tooltip", "title equals column name");
  });
  
  test("title is not overridden when header column has pre-defined tooltip", function() {
    var $headerCell = getHeaderCell(5); // Grab the requested header cell
    $headerCell.trigger("mouseenter");  // Trigger hover on a header cell
    
    strictEqual($headerCell.attr("title"), "Long header with predefined tooltip", "title is not overridden");
  });
  
  // ******************************** //
  // Tests for maximum tooltip length //
  // ******************************** //
  
  module("plugins - autotooltips - max tooltip", {
    setup: function () {
      setupGrid({ maxToolTipLength: 20 });
    },
    teardown: teardownGrid
  });
  
  test("title is empty when cell text has enough room", function () {
    var $cell = getCell(0);       // Grab a cell
    $cell.trigger("mouseenter");  // Trigger hover on a cell in grid
    
    strictEqual($cell.attr("title"), "", "title is not present");
  });
  
  test("title is present and not truncated when cell text is cut off but not too long", function () {
    var $cell = getCell(1);       // Grab a cell
    $cell.trigger("mouseenter");  // Trigger hover on a cell in grid
    
    strictEqual($cell.attr("title"), MEDIUM_VALUE, "title equals truncated text");
  });
  
  test("title is present and truncated when cell text is cut off and too long", function () {
    var $cell = getCell(2);       // Grab a cell
    $cell.trigger("mouseenter");  // Trigger hover on a cell in grid
    
    strictEqual($cell.attr("title"), TRUNCATED_VALUE, "title equals truncated text");
  });

})(jQuery);