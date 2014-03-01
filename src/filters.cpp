#include "filters.h"

#include "yaml-cpp/yaml.h"
#include "to_string.h"

using namespace std;

void load_filter_scalar(Table &table, const YAML::Node &node) {
	string action(node.as<string>());

	if (action == "clear") {
		table.where_conditions = "false";
	} else {
		throw runtime_error("Don't know how to '" + action + "' table '" + table.name + "'");
	}
}

void load_filter_columns(Table &table, const YAML::Node &node) {
	map<string, Column*> columns_by_name;
	for (Column &column : table.columns) {
		columns_by_name[column.name] = &column;
	}

	for (YAML::const_iterator column_it = node.begin(); column_it != node.end(); ++column_it) {
		string column_name(column_it->first.as<string>());
		Column *column = columns_by_name[column_name];
		if (!column) throw runtime_error("Can't find column '" + column_name + "' to filter in table '" + table.name + "'");

		if (column_it->second.Type() == YAML::NodeType::Null) {
			column->filter_expression = "NULL";
		} else {
			column->filter_expression = column_it->second.as<string>();
		}
	}
}

void load_filter_rows(Table &table, const YAML::Node &node) {
	table.where_conditions += table.where_conditions.empty() ? "(" : " AND (";
	table.where_conditions += node.as<string>();
	table.where_conditions += ")";
}

void load_filter_map(Table &table, const YAML::Node &node) {
	for (YAML::const_iterator action_it = node.begin(); action_it != node.end(); ++action_it) {
		if (action_it->first.as<string>() == "replace") {
			load_filter_columns(table, action_it->second);

		} else if (action_it->first.as<string>() == "only") {
			load_filter_rows(table, action_it->second);

		} else {
			throw runtime_error("Don't how to filter table '" + table.name + "'; action given: " + to_string(action_it->first));
		}
	}
}

void load_filters(const char *filters_file, map<string, Table*> &tables_by_name) {
	YAML::Node config(YAML::LoadFile(filters_file));

	for (YAML::const_iterator table_it = config.begin(); table_it != config.end(); ++table_it) {
		string table_name(table_it->first.as<string>());
		Table *table = tables_by_name[table_name];
		if (!table) throw runtime_error("Filtered table '" + table_name + "' not found");

		switch (table_it->second.Type()) {
			case YAML::NodeType::Scalar:
				load_filter_scalar(*table, table_it->second);
				break;

			case YAML::NodeType::Map:
				load_filter_map(*table, table_it->second);
				break;

			default:
				throw runtime_error("Don't how to filter table '" + table_name + "'; action given: " + to_string(table_it->second));
		}
	}
}
