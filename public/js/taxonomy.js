var taxonomy = (function($) {

	var call_api = function(method, params, success) {
		$.post('/api/' + method, params, function(data) {
			data = JSON.parse(data);
			if(data.status === 'ok') {
				success(data.response);
			} else {
				console.log("Error calling API");
				console.log(data);
			}
		});
	};

	var add_cell = function(row, content, cls) {
		if(content === null) {
			content = '';
		}
		row.append($("<td>").html(content).addClass(cls));
	};
	var add_cell_attr = function(row, obj, attr, prefix) {
		add_cell(row, obj[attr], prefix + '-' + attr);
	};

	var render_taxon = function(taxon, place) {
		var row = $("<tr>").addClass("row-taxon").addClass("rank-" + taxon.rank);
		['rank', 'age', 'valid_name', 'comments'].forEach(function(attr) {
			add_cell_attr(row, taxon, attr, 'taxon');
		});
		var names = $("<table>").addClass("names-table");
		taxon.names.forEach(function(name) {
			var row = $("<tr>").addClass("row-name").addClass("status-" + name.status);
			['group', 'original_name', 'base_name', 'authority', 'year', 'page_described',
				'original_citation', 'nomenclature_comments', 'taxonomy_comments',
				'other_comments'].forEach(function(attr) {
				add_cell_attr(row, name, attr, 'name');
			});
			// TODO: types
			names.append(row);
		});
		row.append($("<td>").append(names).addClass('taxon-names'));
		place.append(row);
		taxon.children.forEach(function(child) {
			render_taxon(child, place);
		});
	};

	var render_taxonomy = function(taxon, place) {
		var table = $("<table>").addClass('taxonomy-table');
		render_taxon(taxon, table);
		place.append(table);
	};

	return {
		call_api: call_api,
		render_taxonomy: render_taxonomy,
	}
})(jQuery);
