#!/usr/bin/env python

import collections
import os

import psycopg2
from psycopg2.extras import RealDictCursor

from utils import Jinja2

sql_conn = psycopg2.connect(os.getenv("DATABASE_DSN"))
ps_cursor = sql_conn.cursor(cursor_factory=RealDictCursor)


def get_field(obj):
    if obj["data_type"] == "integer":
        line = f"{obj['column_name']} = fields.IntField("
    elif obj["data_type"] == "numeric":
        line = f"{obj['column_name']} = fields.DecimalField(max_digits={obj['numeric_precision']}, decimal_places={obj['numeric_scale']}, "
    elif obj["data_type"] == "timestamp with time zone":
        line = f"{obj['column_name']} = fields.DateTimeField("
    elif obj["data_type"] == "date":
        line = f"{obj['column_name']} = fields.DateField("
    elif obj["data_type"] == "text":
        line = f"{obj['column_name']} = fields.TextField("
    elif obj["data_type"] == "character varying":
        line = f"{obj['column_name']} = fields.CharField(max_length={obj['character_maximum_length']}, "
    else:
        return f"{obj['column_name']} = # VALIDATE_BY_HAND"

    if obj["is_nullable"]:
        line = f"{line}null=True"
    else:
        line = f"{line}null=False"

    line = f"{line})"

    if str(obj["column_name"]).endswith("_id"):
        line = f"{line} # CHECK_RELATIONSHIP"

    line = f"{line}"
    return line


def inspect_table(table_name, obj):
    Jinja2.set_template("tortoise.py.jinja2")

    class_name = "".join(word.title() for word in table_name.split("_"))
    fields = []

    for row in obj:
        fields.append(get_field(row))

    with open(f"output/{table_name}.py", "w") as f:
        f.write(
            Jinja2.render(table_name=table_name, class_name=class_name, fields=fields)
        )


def main():
    catalog = os.getenv("DB_CATALOG")
    schema = os.getenv("DB_SCHEMA")
    ps_cursor.execute(
        f"""
        SELECT *
        FROM information_schema.COLUMNS
        WHERE table_catalog = '{catalog}'
        AND table_schema = '{schema}'
        AND column_name != 'id'
        ORDER BY table_name, column_name;
    """
    )
    rows = ps_cursor.fetchall()
    grouped = collections.defaultdict(list)

    for row in rows:
        grouped[row["table_name"]].append(row)

    for key, values in grouped.items():
        inspect_table(key, values)


if __name__ == "__main__":
    main()
