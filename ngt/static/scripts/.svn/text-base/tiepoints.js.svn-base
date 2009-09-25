
// Create the XML-RPC object, with the radius function accessible
mars = XmlRpc.getObject("/z/geo/moon", ["radius"]);

get_moon_radius = function (latitude,longitude) {
    // Call the service to obtain the radius at that point
    radius = moon.radius(latitude,longitude);
    
    // Update the page text with the returned value
    $('radius').value = radius;
}
       
// ---------------
function load() {
    google.earth.createInstance("map3d", initCallback, failureCallback);
}

google.load("earth", "1");

var ge = null;
var crossOverlay = null;

// KML variables
var named_kmls = $H({
	'CTX Footprints': 'http://pirlwww.lpl.arizona.edu/~rbeyer/kml/CTX_PDS.kmz', 
	'MSL Landing Sites':'http://alderaan.arc.nasa.gov/anne/MSL_Landing_Sites-2.kml',
	'MOLA':'http://alderaan.arc.nasa.gov/mbroxton/mars_overlay/mola_colorized/mola_colorized.kml',
	'THEMIS':'http://onmars.jpl.nasa.gov/THEMIS.kml',
	'MDIM':'http://byss.arc.nasa.gov/maps/mars/visible_kml/visible_kml.kml',
    });
var kml_load_name = '';
var named_kml_objs = new Hash;
var named_kml_chks = new Hash;

var xy_changed = false;

// ---------------

function init() {
    google.earth.createInstance("map3d", initCallback, failureCallback);
}

function initCallback(object) {
    ge = object;
    ge.getWindow().setVisibility(true);
    var cam = ge.getView().copyAsCamera(ge.ALTITUDE_ABSOLUTE);
    cam.setAltitude(12000000);
    ge.getView().setAbstractView(cam);
    ge.getNavigationControl().setVisibility(ge.VISIBILITY_SHOW);
    document.getElementById('geplugin_version').innerHTML = ge.getPluginVersion();
    show_crosshairs(true);

    // Setup named_kml stuff
    named_kml_chks.set('CTX Footprints', $('ctx_footprints'));
    named_kml_chks.set('MSL Landing Sites', $('msl_landing_sites'));
    named_kml_chks.set('MOLA', $('MOLA'));
    named_kml_chks.set('THEMIS', $('THEMIS'));
    named_kml_chks.set('MDIM', $('MDIM'));

    show_named_kml('MSL Landing Sites', true);
    show_named_kml('CTX Footprints', false);
    show_named_kml('MOLA', false);
    show_named_kml('THEMIS', true);
    show_named_kml('MDIM', false);

    // Add crosshairs to gigapan browser
    //debug("showCrosshairs(1)");
    //flashProxy.call('showCrosshairs', 1);
    
    // Create tie point list.  The $("") part is taking advantage of 
    // functionality from the 'prototype' framework
    tp_list = new TiepointList($("tiepoint_list_div"), 
			       tiepoint_list_event_handler);
    
    // Setup interface for not having current tiepoint
    ui_setup_no_active_tiepoint();
    
    // Setup first pano as default
    panos.each(function(pair) {
	    if(curr_pano_name == '' || curr_pano_id == -1) {
		curr_pano_name = pair.value.name;
		curr_pano_id   = pair.key;
	    }
	});
    
    ui_select_new_pano(curr_pano_id);
}

function failureCallback(object) {
}

