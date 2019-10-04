#ifndef COLUMN_TYPES_H
#define COLUMN_TYPES_H

#include <string>

using namespace std;

// we represent the column types in memory using integer constants so that we can efficiently use
// them in switch statements in relatively frequent calls, but we serialize the string format. as a
// result, we can freely change the integer constants as required to make logic easier. we don't really
// interpret the bitpatterns, but have grouped them for convenience and the compiler might choose to.
enum class ColumnType {
	unknown                 = 0x00000000,
	mysql_specific          = 0x00000001,
	postgresql_specific     = 0x00000002,

	binary                  = 0x00000100,
	binary_varbinary        = 0x00000101,
	binary_fixed            = 0x00000102,
	text                    = 0x00000200,
	text_varchar            = 0x00000201,
	text_fixed              = 0x00000202,
	json                    = 0x00000300,
	json_binary             = 0x00000301,
	uuid                    = 0x00000400,
	date                    = 0x00000500,
	time                    = 0x00000501,
	time_tz                 = 0x00000502,
	datetime                = 0x00000503,
	datetime_tz             = 0x00000504,
	datetime_mysqltimestamp = 0x00000505,
	spatial                 = 0x00000600,
	spatial_geography       = 0x00000601,
	enumeration             = 0x00000700,

	boolean                 = 0x40000000,
	sint_8b                 = 0x40000100,
	sint_16b                = 0x40000101,
	sint_24b                = 0x40000102,
	sint_32b                = 0x40000103,
	sint_64b                = 0x40000104,
	uint_8b                 = 0x40000201,
	uint_16b                = 0x40000202,
	uint_24b                = 0x40000203,
	uint_32b                = 0x40000204,
	uint_64b                = 0x40000205,
	float_64b               = 0x40000300,
	float_32b               = 0x40000301,
	decimal                 = 0x40000400,

	integer_min             = 0x40000100,
	integer_max             = 0x400002ff,
	non_quoted_literals_min = 0x40000000,
	non_quoted_literals_max = 0x7fffffff,
};

const map<ColumnType, string> ColumnTypeNames{
	{ColumnType::unknown, "unknown"},
	{ColumnType::mysql_specific, "mysql_specific"},
	{ColumnType::postgresql_specific, "postgresql_specific"},

	{ColumnType::binary, "binary"},
	{ColumnType::binary_varbinary, "binary.varbinary"},
	{ColumnType::binary_fixed, "binary.fixed"},
	{ColumnType::text, "text"},
	{ColumnType::text_varchar, "text.varchar"},
	{ColumnType::text_fixed, "text.fixed"},
	{ColumnType::json, "json"},
	{ColumnType::json_binary, "json.binary"},
	{ColumnType::uuid, "uuid"},
	{ColumnType::date, "date"},
	{ColumnType::time, "time"},
	{ColumnType::time_tz, "time.tz"},
	{ColumnType::datetime, "datetime"},
	{ColumnType::datetime_tz, "datetime.tz"},
	{ColumnType::datetime_mysqltimestamp, "datetime.mysqltimestamp"},
	{ColumnType::spatial, "spatial"},
	{ColumnType::spatial_geography, "spatial.geography"},
	{ColumnType::enumeration, "enum"},

	{ColumnType::boolean, "boolean"},
	{ColumnType::sint_8b, "integer.8bit"},
	{ColumnType::sint_16b, "integer.16bit"},
	{ColumnType::sint_24b, "integer.24bit"},
	{ColumnType::sint_32b, "integer"},
	{ColumnType::sint_64b, "integer.64bit"},
	{ColumnType::uint_8b, "integer.unsigned.8bit"},
	{ColumnType::uint_16b, "integer.unsigned.16bit"},
	{ColumnType::uint_24b, "integer.unsigned.24bit"},
	{ColumnType::uint_32b, "integer.unsigned"},
	{ColumnType::uint_64b, "integer.unsigned.64bit"},
	{ColumnType::float_64b, "float"},
	{ColumnType::float_32b, "float.32bit"},
	{ColumnType::decimal, "decimal"},
};

const map<string, ColumnType> ColumnTypesByName{
	{"unknown", ColumnType::unknown},
	{"mysql_specific", ColumnType::mysql_specific},
	{"postgresql_specific", ColumnType::postgresql_specific},

	{"binary", ColumnType::binary},
	{"binary.varbinary", ColumnType::binary_varbinary},
	{"binary.fixed", ColumnType::binary_fixed},
	{"text", ColumnType::text},
	{"text.varchar", ColumnType::text_varchar},
	{"text.fixed", ColumnType::text_fixed},
	{"json", ColumnType::json},
	{"json.binary", ColumnType::json_binary},
	{"uuid", ColumnType::uuid},
	{"date", ColumnType::date},
	{"time", ColumnType::time},
	{"time.tz", ColumnType::time_tz},
	{"datetime", ColumnType::datetime},
	{"datetime.tz", ColumnType::datetime_tz},
	{"datetime.mysqltimestamp", ColumnType::datetime_mysqltimestamp},
	{"spatial", ColumnType::spatial},
	{"spatial.geography", ColumnType::spatial_geography},
	{"enum", ColumnType::enumeration},

	{"boolean", ColumnType::boolean},
	{"integer.8bit", ColumnType::sint_8b},
	{"integer.16bit", ColumnType::sint_16b},
	{"integer.24bit", ColumnType::sint_24b},
	{"integer", ColumnType::sint_32b},
	{"integer.64bit", ColumnType::sint_64b},
	{"integer.unsigned.8bit", ColumnType::uint_8b},
	{"integer.unsigned.16bit", ColumnType::uint_16b},
	{"integer.unsigned.24bit", ColumnType::uint_24b},
	{"integer.unsigned", ColumnType::uint_32b},
	{"integer.unsigned.64bit", ColumnType::uint_64b},
	{"float", ColumnType::float_64b},
	{"float.32bit", ColumnType::float_32b},
	{"decimal", ColumnType::decimal},
};

#endif
