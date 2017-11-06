#include "filters.h"

#include "yaml-cpp/yaml.h"
#include "to_string.h"

using namespace std;

void load_filter_scalar(const string &table_name, TableFilter &table_filter, const YAML::Node &node) {
	string action(node.as<string>());

	if (action == "clear") {
		table_filter.where_conditions = "false";
	} else {
		throw runtime_error("Don't know how to '" + action + "' table '" + table_name + "'");
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
	table_filter.where_conditions += table_filter.where_conditions.empty() ? "(" : " AND (";
	table_filter.where_conditions += node.as<string>();
	table_filter.where_conditions += ")";
}

void load_filter_map(const string &table_name, TableFilter &table_filter, const YAML::Node &node) {
	for (YAML::const_iterator action_it = node.begin(); action_it != node.end(); ++action_it) {
		if (action_it->first.as<string>() == "replace") {
			load_filter_columns(table_name, table_filter, action_it->second);

		} else if (action_it->first.as<string>() == "only") {
			load_filter_rows(table_name, table_filter, action_it->second);

		} else {
			throw runtime_error("Don't how to filter table '" + table_name + "'; action given: " + to_string(action_it->first));
		}
	}
}

TableFilters load_filters(const string &filters_file) {
	YAML::Node config(YAML::LoadFile(filters_file));
	TableFilters table_filters;

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
				throw runtime_error("Don't how to filter table '" + table_name + "'; action given: " + to_string(table_it->second));
		}
	}

	return table_filters;
}
