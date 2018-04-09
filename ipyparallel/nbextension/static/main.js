// Copyright (c) IPython Development Team.
// Distributed under the terms of the Modified BSD License.

define(function(require) {
    var $ = require('jquery');
    var IPython = require('base/js/namespace');
    var clusterlist = require('./clusterlist');
    
    var cluster_html = $([
'<div id="ipyclusters" class="tab-pane">',
'  <div id="cluster_toolbar" class="row">',
'    <div class="col-xs-8 no-padding">',
'      <span id="cluster_list_info">IPython parallel computing clusters</span>',
'    </div>',
'    <div class="col-xs-4 no-padding tree-buttons">',
'      <span id="cluster_buttons" class="pull-right">',
'      <button id="refresh_cluster_list" title="Refresh cluster list" class="btn btn-default btn-xs"><i class="fa fa-refresh"></i></button>',
'      </span>',
'    </div>',
'  </div>',
'  <div id="cluster_list">',
'    <div id="cluster_list_header" class="row list_header">',
'      <div class="profile_col col-xs-4">profile</div>',
'      <div class="status_col col-xs-3">status</div>',
'      <div class="engines_col col-xs-3" title="Enter the number of engines to start or empty for default"># of engines</div>',
'      <div class="action_col col-xs-2">action</div>',
'    </div>',
'  </div>',
'</div>'
    ].join('\n'));
    
    function load() {
        if (!IPython.notebook_list) return;
        var base_url = IPython.notebook_list.base_url;
        // hide the deprecated clusters tab
        $("#tabs").find('[href="#clusters"]').hide();
        $('head').append(
            $('<link>')
            .attr('rel', 'stylesheet')
            .attr('type', 'text/css')
            .attr('href', base_url + 'nbextensions/ipyparallel/clusterlist.css')
        );
        $(".tab-content").append(cluster_html);
        $("#tabs").append(
            $('<li>')
            .append(
                $('<a>')
                .attr('href', '#ipyclusters')
                .attr('data-toggle', 'tab')
                .text('IPython Clusters')
                .click(function (e) {
                    window.history.pushState(null, null, '#ipyclusters');
                })
            )
        );
        var cluster_list = new clusterlist.ClusterList('#cluster_list', {
            base_url: IPython.notebook_list.base_url,
            notebook_path: IPython.notebook_list.notebook_path,
        });
        cluster_list.load_list();
    }
    return {
        load_ipython_extension: load
    };
});
