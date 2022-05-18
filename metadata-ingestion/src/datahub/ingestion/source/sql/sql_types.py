import re
from typing import Any, Dict, ValuesView

from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    ArrayType,
    BooleanType,
    BytesType,
    DateType,
    EnumType,
    NullType,
    NumberType,
    RecordType,
    StringType,
    TimeType,
    UnionType,
)

# these can be obtained by running `select format_type(oid, null),* from pg_type;`
# we've omitted the types without a meaningful DataHub type (e.g. postgres-specific types, index vectors, etc.)
# (run `\copy (select format_type(oid, null),* from pg_type) to 'pg_type.csv' csv header;` to get a CSV)

# we map from format_type since this is what dbt uses
# see https://github.com/fishtown-analytics/dbt/blob/master/plugins/postgres/dbt/include/postgres/macros/catalog.sql#L22

# see https://www.npgsql.org/dev/types.html for helpful type annotations
POSTGRES_TYPES_MAP: Dict[str, Any] = {
    "boolean": BooleanType,
    "bytea": BytesType,
    '"char"': StringType,
    "name": None,  # for system identifiers
    "bigint": NumberType,
    "smallint": NumberType,
    "int2vector": NumberType,  # for indexing
    "integer": NumberType,
    "regproc": None,  # object identifier
    "text": StringType,
    "oid": None,  # object identifier
    "tid": None,  # object identifier
    "xid": None,  # object identifier
    "cid": None,  # object identifier
    "oidvector": None,  # object identifier
    "json": RecordType,
    "xml": RecordType,
    "xid8": None,  # object identifier
    "point": None,  # 2D point
    "lseg": None,  # line segment
    "path": None,  # path of points
    "box": None,  # a pair of corner points
    "polygon": None,  # closed set of points
    "line": None,  # infinite line
    "real": NumberType,
    "double precision": NumberType,
    "unknown": None,
    "circle": None,  # circle with center and radius
    "money": NumberType,
    "macaddr": None,  # MAC address
    "inet": None,  # IPv4 or IPv6 host address
    "cidr": None,  # IPv4 or IPv6 network specification
    "macaddr8": None,  # MAC address
    "aclitem": None,  # system info
    "character": StringType,
    "character varying": StringType,
    "date": DateType,
    "time without time zone": TimeType,
    "timestamp without time zone": TimeType,
    "timestamp with time zone": TimeType,
    "interval": None,
    "time with time zone": TimeType,
    "bit": BytesType,
    "bit varying": BytesType,
    "numeric": NumberType,
    "refcursor": None,
    "regprocedure": None,
    "regoper": None,
    "regoperator": None,
    "regclass": None,
    "regcollation": None,
    "regtype": None,
    "regrole": None,
    "regnamespace": None,
    "uuid": StringType,
    "pg_lsn": None,
    "tsvector": None,  # text search vector
    "gtsvector": None,  # GiST for tsvector. Probably internal type.
    "tsquery": None,  # text search query tree
    "regconfig": None,
    "regdictionary": None,
    "jsonb": BytesType,
    "jsonpath": None,  # path to property in a JSON doc
    "txid_snapshot": None,
    "pg_snapshot": None,
    "int4range": None,  # don't have support for ranges yet
    "numrange": None,
    "tsrange": None,
    "tstzrange": None,
    "daterange": None,
    "int8range": None,
    "record": RecordType,
    "record[]": ArrayType,
    "cstring": None,
    '"any"': UnionType,
    "anyarray": ArrayType,
    "void": NullType,
    "trigger": None,
    "event_trigger": None,
    "language_handler": None,
    "internal": None,
    "anyelement": None,
    "anynonarray": None,
    "anyenum": EnumType,
    "fdw_handler": None,
    "index_am_handler": None,
    "tsm_handler": None,
    "table_am_handler": None,
    "anyrange": None,
    "anycompatible": None,
    "anycompatiblearray": None,
    "anycompatiblenonarray": None,
    "anycompatiblerange": None,
    "boolean[]": ArrayType,
    "bytea[]": ArrayType,
    '"char"[]': ArrayType,
    "name[]": ArrayType,
    "bigint[]": ArrayType,
    "smallint[]": ArrayType,
    "int2vector[]": ArrayType,
    "integer[]": ArrayType,
    "regproc[]": ArrayType,
    "text[]": ArrayType,
    "oid[]": ArrayType,
    "tid[]": ArrayType,
    "xid[]": ArrayType,
    "cid[]": ArrayType,
    "oidvector[]": ArrayType,
    "json[]": ArrayType,
    "xml[]": ArrayType,
    "xid8[]": ValuesView,
    "point[]": ArrayType,
    "lseg[]": ArrayType,
    "path[]": ArrayType,
    "box[]": ArrayType,
    "polygon[]": ArrayType,
    "line[]": ArrayType,
    "real[]": ArrayType,
    "double precision[]": ArrayType,
    "circle[]": ArrayType,
    "money[]": ArrayType,
    "macaddr[]": ArrayType,
    "inet[]": ArrayType,
    "cidr[]": ArrayType,
    "macaddr8[]": ArrayType,
    "aclitem[]": ArrayType,
    "character[]": ArrayType,
    "character varying[]": ArrayType,
    "date[]": ArrayType,
    "time without time zone[]": ArrayType,
    "timestamp without time zone[]": ArrayType,
    "timestamp with time zone[]": ArrayType,
    "interval[]": ArrayType,
    "time with time zone[]": ArrayType,
    "bit[]": ArrayType,
    "bit varying[]": ArrayType,
    "numeric[]": ArrayType,
    "refcursor[]": ArrayType,
    "regprocedure[]": ArrayType,
    "regoper[]": ArrayType,
    "regoperator[]": ArrayType,
    "regclass[]": ArrayType,
    "regcollation[]": ArrayType,
    "regtype[]": ArrayType,
    "regrole[]": ArrayType,
    "regnamespace[]": ArrayType,
    "uuid[]": ArrayType,
    "pg_lsn[]": ArrayType,
    "tsvector[]": ArrayType,
    "gtsvector[]": ArrayType,
    "tsquery[]": ArrayType,
    "regconfig[]": ArrayType,
    "regdictionary[]": ArrayType,
    "jsonb[]": ArrayType,
    "jsonpath[]": ArrayType,
    "txid_snapshot[]": ArrayType,
    "pg_snapshot[]": ArrayType,
    "int4range[]": ArrayType,
    "numrange[]": ArrayType,
    "tsrange[]": ArrayType,
    "tstzrange[]": ArrayType,
    "daterange[]": ArrayType,
    "int8range[]": ArrayType,
    "cstring[]": ArrayType,
}

# Postgres types with modifiers (identifiable by non-empty typmodin and typmodout columns)
POSTGRES_MODIFIED_TYPES = {
    "character varying",
    "character varying[]",
    "bit varying",
    "bit varying[]",
    "time with time zone",
    "time with time zone[]",
    "time without time zone",
    "time without time zone[]",
    "timestamp with time zone",
    "timestamp with time zone[]",
    "timestamp without time zone",
    "timestamp without time zone[]",
    "numeric",
    "numeric[]",
    "interval",
    "interval[]",
    "character",
    "character[]",
    "bit",
    "bit[]",
}


def resolve_postgres_modified_type(type_string: str) -> Any:

    if type_string.endswith("[]"):
        return ArrayType

    for modified_type_base in POSTGRES_MODIFIED_TYPES:
        if re.match(rf"{re.escape(modified_type_base)}\([0-9,]+\)", type_string):
            return POSTGRES_TYPES_MAP[modified_type_base]

    return None


# see https://docs.snowflake.com/en/sql-reference/intro-summary-data-types.html
SNOWFLAKE_TYPES_MAP: Dict[str, Any] = {
    "NUMBER": NumberType,
    "DECIMAL": NumberType,
    "NUMERIC": NumberType,
    "INT": NumberType,
    "INTEGER": NumberType,
    "BIGINT": NumberType,
    "SMALLINT": NumberType,
    "FLOAT": NumberType,
    "FLOAT4": NumberType,
    "FLOAT8": NumberType,
    "DOUBLE": NumberType,
    "DOUBLE PRECISION": NumberType,
    "REAL": NumberType,
    "VARCHAR": StringType,
    "CHAR": StringType,
    "CHARACTER": StringType,
    "STRING": StringType,
    "TEXT": StringType,
    "BINARY": BytesType,
    "VARBINARY": BytesType,
    "BOOLEAN": BooleanType,
    "DATE": DateType,
    "DATETIME": DateType,
    "TIME": TimeType,
    "TIMESTAMP": TimeType,
    "TIMESTAMP_LTZ": TimeType,
    "TIMESTAMP_NTZ": TimeType,
    "TIMESTAMP_TZ": TimeType,
    "VARIANT": RecordType,
    "OBJECT": RecordType,
    "ARRAY": ArrayType,
    "GEOGRAPHY": None,
}

# see https://github.com/googleapis/python-bigquery-sqlalchemy/blob/main/sqlalchemy_bigquery/_types.py#L32
BIGQUERY_TYPES_MAP: Dict[str, Any] = {
    "STRING": StringType,
    "BOOL": BooleanType,
    "BOOLEAN": BooleanType,
    "INT64": NumberType,
    "INTEGER": NumberType,
    "FLOAT64": NumberType,
    "FLOAT": NumberType,
    "TIMESTAMP": TimeType,
    "DATETIME": DateType,
    "DATE": DateType,
    "BYTES": BytesType,
    "TIME": TimeType,
    "RECORD": RecordType,
    "NUMERIC": NumberType,
    "BIGNUMERIC": NumberType,
    "GEOGRAPHY": None,
}

# see https://spark.apache.org/docs/latest/sql-ref-datatypes.html
SPARK_SQL_TYPES_MAP: Dict[str, Any] = {
    "boolean": BooleanType,
    "byte": NumberType,
    "tinyint": NumberType,
    "short": NumberType,
    "smallint": NumberType,
    "int": NumberType,
    "integer": NumberType,
    "long": NumberType,
    "bigint": NumberType,
    "float": NumberType,
    "real": NumberType,
    "double": NumberType,
    "date": DateType,
    "timestamp": TimeType,
    "string": StringType,
    "binary": BytesType,
    "decimal": NumberType,
    "dec": NumberType,
    "numeric": NumberType,
    "array": ArrayType,
    "struct": RecordType,
    "map": RecordType,
}
