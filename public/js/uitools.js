var uiTools = {
	/*
	 * Give a menu with a number of options in buttons. Parameters (all mandatory):
	 * - title: Title of the form
	 * - text: Text displayed in the form
	 * - options: Array of strings that become button labels
	 * - callback: Function executed when the form completes, with the name of the button selected as the argument.
	 */
	menu: function(paras) {
		var $dialog = $("<div>").addClass('uitools-dialog').attr('title', paras.title);
		var $p = $('<p>').addClass('uiTools-menu-line').html(paras.text);
		$dialog.append($p);
		function callback(name) {
			return function() {
				$dialog.dialog('close');
				paras.callback(name);
			}
		}
		var buttons = {};
		paras.options.forEach(function(button) {
			buttons[button] = callback(button);
		});
		$dialog.dialog({
			resizable: false,
			modal: true,
			buttons: buttons
		});
	},
	/*
	 * Display an alert box with parameters:
	 * - title: Title of the box
	 * - text: Text of the box.
	 */
	alert: function(paras) {
		var $dialog = $("<div>").addClass('uitools-dialog').attr('title', paras.title).html(paras.text);
		$dialog.dialog();
	},
	/*
	 * Select the div[data-role="page"] with the given id, hiding all others.
	 */
	selectPage: function(id) {
		$('div[data-role="page"]').hide();
		$('#' + id).show();
	},
	/*
	 * Ask the user for a set of data in a form. Parameters:
	 * - title: Title of the form.
	 * - text: Text displayed above the form.
	 * - fields: Array of fields to use. Each entry should be an object with keys name, type, and text.
	 * - callback: Callback executed with as its argument an object where the keys are the names of the fields and the values are the values entered.
	 */
	ask: function(paras) {
		var $dialog = $("<div>").attr('title', paras.title).addClass('uitools-form uitools-dialog');
		$dialog.append($("<p>").html(paras.text));
		var $fieldset = $("<fieldset>");
		paras.fields.forEach(function(field) {
			var $label = $("<label>").attr('for', field.name).html(field.text);
			var $input = $("<input>").attr('name', field.name).attr('id', field.name).attr('type', field.type).addClass("ui-widget-content ui-corner-all");
			$fieldset.append($label).append($input);
		});
		$dialog.append($("<form>").append($fieldset));

		$dialog.dialog({
			resizable: false,
			modal: true,
			buttons: {
				Save: function() {
					var data = {};
					paras.fields.forEach(function(field) {
						data[field.name] = $dialog.find('#' + field.name).val();
					});
					$dialog.dialog('close');
					paras.callback(data);
				}
			}
		});
	},
	/*
	 * Create a dropdown menu that pops up when an element is clicked. Parameters:
	 * - selector: Selector for elements to create the dropdown on
	 * - text: Text displayed on the element
	 * - options: Options to display when the element is clicked
	 * - callback: Function that gets fired when an element is selected
	 */
	dropdown: function(paras) {
		var items = {};
		paras.options.forEach(function(option) {
			items[option] = {name: option};
		});
		$.contextMenu({
			selector: paras.selector,
			trigger: 'left',
			items: items,
			callback: paras.callback
		});
	},
	/*
	 * Close any open dialogs created by uiTools.
	 */
	closeAllDialogs: function() {
		$('.uitools-dialog').dialog('close');
	}
};
