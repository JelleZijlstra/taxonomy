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

	// Count the number of times that substr occurs in str
	var count_substr = function(str, substr) {
		// http://stackoverflow.com/questions/881085/count-the-number-of-occurences-of-a-character-in-a-string-in-javascript
		var regex = new RegExp(substr, "g");
		var match = str.match(regex);
		if(match) {
			return match.length;
		} else {
			return 0;
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


	var get_id = function(elt, table) {
		return $(elt).closest('.row-' + table).attr('data-id');
	};
	var get_numeric = function($row, table, attr) {
		return parseInt($row.find('.' + table + '-' + attr).attr('data-value'), 10);
	};

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

	var add_name = function(list, name) {
		var row = $("<div>").addClass("row-name").addClass("status-" + name.status)
			.attr('data-id', name.id);
		add_cell_options(row, 'name');
		add_cell_constant(row, name, 'status', 'name');
		add_cell_constant(row, name, 'group', 'name');
		['original_name', 'root_name', 'authority', 'year', 'page_described',
			'original_citation', 'nomenclature_comments', 'taxonomy_comments',
			'other_comments'].forEach(function(attr) {
			add_cell_attr(row, name, attr, 'name');
		});
		// TODO: types
		list.append(row);
		return row;
	};
	var add_new_name = function(list, name) {
		var row = add_name(list, name);
		reload_editing(row);
	};

	var render_taxon = function(taxon, place, before) {
		var div = $("<div>").addClass("container-taxon").attr('data-id', taxon.id);
		var row = $("<div>").addClass("row-taxon").addClass("rank-" + taxon.rank.replace(' ', '_')).attr('data-id', taxon.id);
		add_cell_options(row, 'taxon');
		add_cell_constant(row, taxon, 'rank', 'taxon');
		add_cell_constant(row, taxon, 'age', 'taxon');
		add_cell_attr(row, taxon, 'valid_name', 'taxon');
		add_cell_attr(row, taxon, 'comments', 'taxon');
		var names = $("<div>").addClass("names-table");
		taxon.names.forEach(function(name) {
			add_name(names, name);
		});
		row.append($("<div>").append(names).addClass('taxon-names'));
		div.append($("<div>").addClass('row-taxon-outer').append(row));
		var children = $("<div>").addClass("children-taxon");
		taxon.children.forEach(function(child) {
			render_taxon(child, children);
		});
		div.append(children);
		if(before) {
			place.before(div);
		} else {
			place.append(div);
		}
		return div;
	};
	var render_new_taxon = function(taxon, place, before) {
		var div = render_taxon(taxon, place, before);
		reload_editing(div);
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
				var root_name = valid_name.replace(/^.* /, '');
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
				var spaces = count_substr(valid_name, " ");
				if(spaces === 2) {
					rank = dfs.SUBSPECIES;
				} else if(spaces === 1) {
					rank = dfs.SPECIES;
				}
				var paras = {
					valid_name: valid_name,
					root_name: root_name,
					group: group_of_rank(rank),
					rank: rank,
					age: get_numeric($row, 'taxon', 'age'),
					parent: id
				};
				changes.push({'kind': 'create_pair', 'data': paras});
				save_now(function(results) {
					var txn = array_find(results, function(val) {
						console.log(val);
						return val.kind === 'create_pair' && val.valid_name === valid_name;
					}).taxon;
					console.log(txn);

					// find right place to insert
					var children = $place.find('.children-taxon');
					var place = null;
					children.children().each(function() {
						var child_rank = $(this).find('.taxon-rank').first().attr('data-value');
						if(child_rank < rank) {
							return true;
						} else if(child_rank > rank) {
							place = $(this);
							return false;
						} else {
							var child_name = $(this).find('.taxon-valid_name').first().text();
							if(valid_name <= child_name) {
								place = $(this);
								return false;
							} else {
								return true;
							}
						}
					});
					if(place == null) {
						render_new_taxon(txn, children);
					} else {
						render_new_taxon(txn, place, true);
					}
				});
			}
		});
	};
	var add_synonym = function(id, $row) {
		var valid_name = $row.find('.taxon-valid_name').text();
		var name_row = $row.find('.row-name').first();
		uiTools.ask({
			title: 'Add new synonym',
			text: 'Valid name: ' + valid_name,
			fields: [
				{"name": "root_name", "type": "text", "label": "Root name"}
			],
			callback: function(data) {
				var root_name = data.root_name;
				var nm_data = {
					root_name: root_name,
					group: get_numeric(name_row, 'name', 'group'),
					status: dfs.STATUS_SYNONYM,
					taxon: id,
				};
				changes.push({kind: 'create', data: nm_data, table: 'name'});
				save_now(function(data) {
					var nm_obj = array_find(data, function(entry) {
						return entry.kind === 'create' && entry.root_name === root_name;
					}).name;
					add_new_name($row.find('.names-table'), nm_obj);
				});
			}
		});
	};
	var change_parent = function(id, $place) {
		var my_name = $place.find('.taxon-valid_name').first().text();
		get_name({
			table: 'taxon',
			text: 'Give new parent for: ' + my_name,
			success: function(new_parent) {
				var $np = $(".container-taxon[data-id=" + new_parent.id + "]");
				$np.find(".children-taxon").first().append($place);
				changes.push({kind: 'update', id: id, data: {parent: new_parent.id}, table: 'taxon'})
			}
		});
	};

	var get_name = function(paras) {
		uiTools.ask({
			title: 'Enter a name',
			text: paras.text,
			fields: [
				{name: "name", type: "text", text: "Name"},
				{name: "id", type: "hidden", text: ""},
			],
			callback: function(data) {
				var name = data.name;
				call_api('find_taxon', {valid_name: name}, function(data) {
					if(data.length === 0) {
						// invalid taxon
					} else {
						paras.success(data[0]);
					}
				})
			},
		});
	};

	var reload_editing = function(table) {
		make_text_editable(table, 'valid_name', 'taxon');
		make_text_editable(table, 'comments', 'taxon');
		make_text_editable(table, 'original_name', 'name');
		make_text_editable(table, 'root_name', 'name');
		make_text_editable(table, 'authority', 'name');
		make_text_editable(table, 'year', 'name');
		make_text_editable(table, 'page_described', 'name');
		make_text_editable(table, 'original_citation', 'name');
		make_text_editable(table, 'nomenclature_comments', 'name');
		make_text_editable(table, 'taxonomy_comments', 'name');
		make_text_editable(table, 'other_comments', 'name');
	};

	var render_taxonomy = function(taxon, place) {
		var table = $("<div>").addClass('taxonomy-table');
		render_taxon(taxon, table);

		// turn on editing
		reload_editing(table);
		make_dropdown_editable('rank', 'taxon');
		make_dropdown_editable('age', 'taxon');
		make_dropdown_editable('group', 'name');
		make_dropdown_editable('status', 'name');

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
				},
				'add synonym': {
					name: 'add synonym',
					callback: function() {
						var id = get_id(this, 'taxon');
						add_synonym(id, $(this).closest('.row-taxon'));
					}
				},
				'change parent': {
					name: 'change parent',
					callback: function() {
						change_parent(get_id(this, 'taxon'), $(this).closest('.container-taxon'));
					}
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

	$(window).bind('unload', function() { console.log("I was called"); save_now(); });
	window.onbeforeunload = function() { console.log("I was called"); save_now(); };
	window.addEventListener('unload', function() { console.log("I was called"); save_now(); });

	return {
		call_api: call_api,
		render_taxonomy: render_taxonomy,
		save_now: save_now,
	}
})(jQuery);
