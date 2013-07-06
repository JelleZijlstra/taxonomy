var taxonomy = (function($) {
	// module globals
	var cons = {};
	var dfs = {};
	var changes = [];

	// translate constants into slightly more useful form
	$.each(constants, function(group, data) {
		cons[group] = [];
		data.forEach(function(d) {
			cons[group][d.value] = d;
			dfs[d.constant] = d.value;
		});
	});

	var array_find = function(array, predicate) {
		for(var i = 0; i < array.length; i++) {
			if(predicate(array[i])) {
				return array[i];
			}
		}
	};

	var group_of_rank = function(rank) {
		switch(rank) {
			case dfs.SUBSPECIES: case dfs.SPECIES: case dfs.SPECIES_GROUP:
				return dfs.GROUP_SPECIES;
			case dfs.SUBGENUS: case dfs.GENUS:
				return dfs.GROUP_GENUS;
			case dfs.SUBTRIBE: case dfs.TRIBE: case dfs.SUBFAMILY: case dfs.FAMILY: case dfs.SUPERFAMILY:
				return dfs.GROUP_FAMILY;
			default:
				return dfs.GROUP_HIGH;
		}
	}

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
			.addClass('taxonomy-cell').attr('data-value', val));
	};
	var add_cell_options = function(row, prefix) {
		row.append($("<div>").text("+").addClass(prefix + '-options').addClass('taxonomy-cell'));
	};

	var render_taxon = function(taxon, place) {
		var div = $("<div>").addClass("container-taxon").attr('data-id', taxon.id);
		var row = $("<div>").addClass("row-taxon").addClass("rank-" + taxon.rank).attr('data-id', taxon.id);
		add_cell_options(row, 'taxon');
		add_cell_constant(row, taxon, 'rank', 'taxon');
		add_cell_constant(row, taxon, 'age', 'taxon');
		add_cell_attr(row, taxon, 'valid_name', 'taxon');
		add_cell_attr(row, taxon, 'comments', 'taxon');
		var names = $("<div>").addClass("names-table");
		taxon.names.forEach(function(name) {
			var row = $("<div>").addClass("row-name").addClass("status-" + name.status)
				.attr('data-id', name.id);
			add_cell_options(row, 'name');
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
	var get_numeric = function($row, table, attr) {
		return parseInt($row.find('.' + table + '-' + attr).attr('data-value'), 10);
	}

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
					$(this).attr('data-value', data.value);
				}
			};
		});
		$.contextMenu({
			selector: '.' + table + '-' + field,
			trigger: 'hover',
			autoHide: true,
			items: items
		});
	};

	var add_child = function(id, $place) {
		var $row = $place.find('.row-taxon').first();
		var name = $row.find('.taxon-valid_name').text();
		var parent_rank = get_numeric($row, 'taxon', 'rank');
		if(parent_rank === dfs.SUBSPECIES) {
			uiTools.alert({title: "Cannot add child", text: "Subspecies cannot have children"});
		}
		uiTools.ask({
			title: 'Add new taxon and name',
			text: 'Parent: ' + name,
			fields: [
				// Only ask for name - everything else can be added and fixed manually
				{"name": "valid_name", "type": "text", "label": "Name"}
			],
			callback: function(data) {
				// determine data
				var valid_name = data.valid_name;
				var base_name = valid_name.replace(/^.* /, '');
				// guess rank
				var rank = parent_rank - 5;
				if(parent_rank === dfs.SUBGENUS) {
					rank = dfs.SPECIES;
				}
				if(parent_rank === dfs.GENUS) {
					if(valid_name.indexOf("(") === -1 && valid_name.indexOf(" ") !== -1) {
						rank = dfs.GENUS;
					}
				}
				if(parent_rank <= dfs.FAMILY && parent_rank >= dfs.SUBTRIBE) {
					rank = dfs.GENUS;
				}
				if(rank !== dfs.GENUS) {
					if(valid_name.match(/oidea$/)) {
						rank = dfs.SUPERFAMILY;
					} else if(valid_name.match(/idae$/)) {
						rank = dfs.FAMILY;
					} else if(valid_name.match(/inae$/)) {
						rank = dfs.SUBFAMILY;
					} else if(valid_name.match(/ini$/)) {
						rank = dfs.TRIBE;
					} else if(valid_name.match(/ina$/)) {
						rank = dfs.SUBTRIBE;
					}
				}
				var paras = {
					valid_name: valid_name,
					base_name: base_name,
					group: group_of_rank(rank),
					rank: rank,
					age: get_numeric($row, 'taxon', 'age'),
					parent: id
				};
				changes.push({'kind': 'create_pair', 'data': paras});
				save_now(function(results) {
					var txn = array_find(results, function(val) {
						return val.kind === 'create_pair' && val.valid_name === valid_name;
					});
					
				});
			}
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
		make_dropdown_editable('group', 'name');

		// options for taxon
		$.contextMenu({
			selector: ".taxon-options",
			autoHide: true,
			trigger: 'hover',
			items: {
				'add child': {
					name: 'add child',
					callback: function() {
						var id = get_id(this, 'taxon');
						add_child(id, $(this).closest('.container-taxon'));
					},
				}
			}
		});

		// save table
		place.append(table);
	};

	var save_now = function(callback) {
		var current_changes = changes;
		changes = [];
		if(current_changes.length !== 0) {
			// TODO: catch errors
			call_api('edit', {'changes': JSON.stringify(current_changes)}, function(results) {
				if(callback) {
					callback(results);
				}
			});
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
