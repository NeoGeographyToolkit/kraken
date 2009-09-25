// Info about Seadragon AJAX tile layout: 
//  http://gasi.ch/blog/inside-deep-zoom-2/
//  http://msdn.microsoft.com/en-us/library/cc645077(VS.95).aspx

var PRECISION = 2;      // number of decimal places            
var viewer = null;

Seadragon.Config.zoomPerClick = 1.0;
            
function init() {
    document.getElementById("container").innerHTML = "";    // for CMS
    
    viewer = new Seadragon.Viewer("container");
    viewer.openDzi("/static/cache/2009_5_29/chicagoland.dzi");
    
    viewer.addEventListener("open", showViewport);
    viewer.addEventListener("animation", showViewport);
    
    Seadragon.Utils.addEvent(viewer.elmt, "mousemove", mouseMove);
    //    Seadragon.Utils.addEvent(viewer.elmt, "mousedown", mouseClick);
}

function mousePixelToImagePixel(pixel) {
    var dims = viewer.source.dimensions;
    var normalized_point = viewer.viewport.pointFromPixel(pixel);
    var point = new Seadragon.Point(dims.x * normalized_point.x,
				    dims.y * normalized_point.y);
    return point;
}

            
function mouseMove(event) {
    // getMousePosition() returns position relative to page, while we want theo
    // position relative to the viewer element. so subtract the difference.
    var pixel = Seadragon.Utils.getMousePosition(event).minus(
		Seadragon.Utils.getElementPosition(viewer.elmt));
    if (!viewer.isOpen()) { return; }

    var point = mousePixelToImagePixel(pixel);

    var dims = viewer.source.dimensions;
    if (point.x >= 0.0 && point.y >= 0.0 && 
	point.x < dims.x && point.y < dims.y) 
	document.getElementById("imagePixels").innerHTML = toString(point, true);
    else 
	document.getElementById("imagePixels").innerHTML = "";
}

function mouseClick(event) {
    // getMousePosition() returns position relative to page, while we want the
    // position relative to the viewer element. so subtract the difference.
    var pixel = Seadragon.Utils.getMousePosition(event).minus(
		Seadragon.Utils.getElementPosition(viewer.elmt));
    
    if (!viewer.isOpen()) { return; }
    
    var point = mousePixelToImagePixel(pixel);

    var dims = viewer.source.dimensions;
    if (point.x >= 0.0 && point.y >= 0.0 && 
	point.x < dims.x && point.y < dims.y)
	document.getElementById("imagePixels").innerHTML = toString(point, true);
    else 
	document.getElementById("imagePixels").innerHTML = "";
}

function showViewport(viewer) {
    if (!viewer.isOpen()) {
	return;
    }
    
    var sizePoints = viewer.viewport.getBounds().getSize();
    var sizePixels = viewer.viewport.getContainerSize();
    // or  = viewer.viewport.deltaPixelsFromPoints(sizePoints);
    
    document.getElementById("viewportSizePoints").innerHTML = toString(sizePoints, false);
    document.getElementById("viewportSizePixels").innerHTML = toString(sizePixels, false);
}

function toString(point, useParens) {
    var x = point.x;
    var y = point.y;
    
    if (x % 1 || y % 1) {           // if not an integer,
	x = x.toFixed(PRECISION);   // then restrict number of
	y = y.toFixed(PRECISION);   // decimal places
    }
    
    if (useParens) {
	return "(" + x + ", " + y + ")";
    } else {
	return x + " x " + y;
    }
}

Seadragon.Utils.addEvent(window, "load", init);