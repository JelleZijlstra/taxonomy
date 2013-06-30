var taxonomy = (function($) {

	// translate constants into slightly more useful form
	var cons = {};
	$.each(constants, function(group, data) {
		cons[group] = [];
		data.forEach(function(d) {
			cons[group][d.value] = d;
		});
	});

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
		row.append($("<div>").html(content).addClass(cls).addClass('taxonomy-cell'));
	};
	var add_cell_attr = function(row, obj, attr, prefix) {
		add_cell(row, obj[attr], prefix + '-' + attr);
	};
	var add_cell_constant = function(row, obj, attr, prefix) {
		var val = obj[attr + '_numeric'];
		var name = cons[attr][val].abbreviation;
		row.append($("<div>").html(name).addClass(prefix + '-' + attr)
			.addClass('taxonomy-cell')).attr('data-value', val);
	};

	var render_taxon = function(taxon, place) {
		var div = $("<div>").addClass("container-taxon").attr('data-id', taxon.id);
		var row = $("<div>").addClass("row-taxon").addClass("rank-" + taxon.rank).attr('data-id', taxon.id);
		add_cell_constant(row, taxon, 'rank', 'taxon');
		add_cell_constant(row, taxon, 'age', 'taxon');
		add_cell_attr(row, taxon, 'valid_name', 'taxon');
		add_cell_attr(row, taxon, 'comments', 'taxon');
		var names = $("<div>").addClass("names-table");
		taxon.names.forEach(function(name) {
			var row = $("<div>").addClass("row-name").addClass("status-" + name.status)
				.attr('data-id', name.id);
			add_cell_constant(row, name, 'group', 'name');
			['original_name', 'base_name', 'authority', 'year', 'page_described',
				'original_citation', 'nomenclature_comments', 'taxonomy_comments',
				'other_comments'].forEach(function(attr) {
				add_cell_attr(row, name, attr, 'name');
			});
			// TODO: types
			names.append(row);
		});
		row.append($("<div>").append(names).addClass('taxon-names'));
		div.append($("<div>").addClass('row-taxon-outer').append(row));
		var children = $("<div>").addClass("children-taxon");
		taxon.children.forEach(function(child) {
			render_taxon(child, children);
		});
		div.append(children);
		place.append(div);
	};

	var get_id = function(elt, table) {
		return $(elt).closest('.row-' + table).attr('data-id');
	};

	var make_text_editable = function(place, field, table) {
		place.find('.' + table + '-' + field).attr('contenteditable', 'true').attr('spellcheck', 'false').blur(function() {
			var new_text = $(this).text();
			var id = get_id(this, table);
			var data = {};
			data[field] = new_text;
			changes.push({'table': table, 'kind': 'update', 'id': id, 'data': data});
		});
	};

	var make_dropdown_editable = function(field, table) {
		var vals = cons[field];
		var items = {};
		$.each(vals, function(val, data) {
			if(data === undefined) {
				return;
			}
			items[data.name] = {
				name: data.name,
				callback: function() {
					var id = get_id(this, table);
					var changed_data = {};
					changed_data[field] = data.value;
					changes.push({'table': table, 'kind': 'update', 'id': id, 'data': changed_data});
					$(this).text(data.abbreviation);
				}
			};
		});
		$.contextMenu({
			selector: '.' + table + '-' + field,
			trigger: 'hover',
			items: items
		});
	};

	var render_taxonomy = function(taxon, place) {
		var table = $("<div>").addClass('taxonomy-table');
		render_taxon(taxon, table);
		// turn on editing
		make_text_editable(table, 'valid_name', 'taxon');
		make_text_editable(table, 'comments', 'taxon');
		make_text_editable(table, 'original_name', 'name');
		make_text_editable(table, 'base_name', 'name');
		make_text_editable(table, 'authority', 'name');
		make_text_editable(table, 'year', 'name');
		make_text_editable(table, 'page_described', 'name');
		make_text_editable(table, 'original_citation', 'name');
		make_text_editable(table, 'nomenclature_comments', 'name');
		make_text_editable(table, 'taxonomy_comments', 'name');
		make_text_editable(table, 'other_comments', 'name');
		make_dropdown_editable('rank', 'taxon');
		make_dropdown_editable('age', 'taxon');
		make_dropdown_editable('group', 'name')

		// table.find('.taxonomy-cell:not(.taxon-rank)').attr('contenteditable', 'true').attr('spellcheck', 'false').blur(function() {
		// 	// changed text
		// 	console.log($(this).text());
		// });
		// $(".name-group").on('focus', function() {

		// });
		var $elt = $(".taxon-rank").first();
		uiTools.dropdown({selector: ".taxon-rank", text: 'order', options: ['family', 'genus'], callback: function() { console.log(arguments); } });

		// save table
		place.append(table);
	};

	var changes = [];

	var save_now = function() {
		var current_changes = changes;
		changes = [];
		if(current_changes.length !== 0) {
			// TODO: catch errors
			call_api('edit', {'changes': JSON.stringify(current_changes)}, function() {});
		}
	}

	// save every five minutes
	window.setInterval(save_now, 300000);

	return {
		call_api: call_api,
		render_taxonomy: render_taxonomy,
		save_now: save_now,
	}
})(jQuery);
