#include "filters.h"

#include "yaml-cpp/yaml.h"
#include "to_string.h"

using namespace std;

void load_filter_scalar(const string &table_name, TableFilter &table_filter, const YAML::Node &node) {
	string action(node.as<string>());

	if (action == "clear") {
		table_filter.where_conditions = "false";
	} else {
		throw filter_definition_error("Don't know how to '" + action + "' table '" + table_name + "'");
	}
}

void load_filter_columns(const string &table_name, TableFilter &table_filter, const YAML::Node &node) {
	for (YAML::const_iterator column_it = node.begin(); column_it != node.end(); ++column_it) {
		string column_name(column_it->first.as<string>());

		if (column_it->second.Type() == YAML::NodeType::Null) {
			table_filter.filter_expressions[column_name] = "NULL";
		} else {
			table_filter.filter_expressions[column_name] = column_it->second.as<string>();
		}
	}
}

void load_filter_rows(const string &table_name, TableFilter &table_filter, const YAML::Node &node) {
	if (table_filter.where_conditions.empty()) { // actually currently always true, since you can't combine 'clear' and 'only'
		table_filter.where_conditions = node.as<string>();
	} else {
		table_filter.where_conditions += " AND (";
		table_filter.where_conditions += node.as<string>();
		table_filter.where_conditions += ")";
	}
}

void load_filter_map(const string &table_name, TableFilter &table_filter, const YAML::Node &node) {
	for (YAML::const_iterator action_it = node.begin(); action_it != node.end(); ++action_it) {
		if (action_it->first.as<string>() == "replace") {
			load_filter_columns(table_name, table_filter, action_it->second);

		} else if (action_it->first.as<string>() == "only") {
			load_filter_rows(table_name, table_filter, action_it->second);

		} else {
			throw filter_definition_error("Don't how to filter table '" + table_name + "'; action given: " + to_string(action_it->first));
		}
	}
}

TableFilters load_filters(const string &filters_file) {
	TableFilters table_filters;

	if (filters_file.empty()) {
		return table_filters;
	}

	YAML::Node config(YAML::LoadFile(filters_file));

	for (YAML::const_iterator table_it = config.begin(); table_it != config.end(); ++table_it) {
		string table_name(table_it->first.as<string>());
		TableFilter &table_filter(table_filters[table_name]);

		switch (table_it->second.Type()) {
			case YAML::NodeType::Scalar:
				load_filter_scalar(table_name, table_filter, table_it->second);
				break;

			case YAML::NodeType::Map:
				load_filter_map(table_name, table_filter, table_it->second);
				break;

			default:
				throw filter_definition_error("Don't how to filter table '" + table_name + "'; action given: " + to_string(table_it->second));
		}
	}

	return table_filters;
}

void apply_filter(Table &table, const TableFilter &table_filter) {
	table.where_conditions = table_filter.where_conditions;

	map<string, Column*> columns_by_name;
	for (Column &column : table.columns) {
		columns_by_name[column.name] = &column;
	}

	for (auto const &it : table_filter.filter_expressions) {
		const string &column_name(it.first);
		const string &filter_expression(it.second);

		Column *column = columns_by_name[column_name];
		if (!column) throw filter_definition_error("Can't find column '" + column_name + "' to filter in table '" + table.name + "'");

		column->filter_expression = filter_expression;
	}

	for (size_t index : table.primary_key_columns) {
		if (!table.columns[index].filter_expression.empty()) {
			throw filter_definition_error("Can't replace values in column '" + table.columns[index].name + "' table '" + table.name + "' because it is used in the primary key");
		}
	}
}

void apply_filters(const TableFilters &table_filters, Tables &tables) {
	map<string, Table*> tables_by_id;

	for (Table &table : tables) {
		tables_by_id[table.name] = &table;
	}

	for (auto const &it : table_filters) {
		const string &table_name(it.first);
		const TableFilter &table_filter(it.second);

		Table *table = tables_by_id[table_name];
		if (!table) throw filter_definition_error("Filtered table '" + table_name + "' not found");

		apply_filter(*table, table_filter);
	}
}
