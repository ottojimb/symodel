#!/usr/bin/env python

import collections
import os

import psycopg2
from psycopg2.extras import RealDictCursor

from utils import Jinja2

sql_conn = psycopg2.connect(os.getenv("DATABASE_DSN"))
ps_cursor = sql_conn.cursor(cursor_factory=RealDictCursor)


def get_relationship(table_row, foreign_keys):
    field = foreign_keys[table_row["column_name"]]
    field_name = str(field["column_name"]).replace("_id", "")
    class_name = "".join(word.title() for word in str(field["foreign_table_name"]).split("_"))
    return f'{field_name} = fields.ForeignKeyField("models.{class_name}", related_name="{field["table_name"]}")'


def get_data_type(table_row):
    if table_row["data_type"] == "integer":
        render_line = f"{table_row['column_name']} = fields.IntField("
    elif table_row["data_type"] == "numeric":
        render_line = f"{table_row['column_name']} = fields.DecimalField(max_digits={table_row['numeric_precision']}, decimal_places={table_row['numeric_scale']}, "
    elif table_row["data_type"] == "timestamp with time zone":
        render_line = f"{table_row['column_name']} = fields.DateTimeField("
    elif table_row["data_type"] == "date":
        render_line = f"{table_row['column_name']} = fields.DateField("
    elif table_row["data_type"] == "text":
        render_line = f"{table_row['column_name']} = fields.TextField("
    elif table_row["data_type"] == "character varying":
        render_line = f"{table_row['column_name']} = fields.CharField(max_length={table_row['character_maximum_length']}, "
    else:
        return f"{table_row['column_name']} = fields.ToValidate()  # VALIDATE_BY_HAND"

    if table_row["is_nullable"]:
        render_line = f"{render_line}null=True"
    else:
        render_line = f"{render_line}null=False"

    return f"{render_line})"


def get_field(table_row, foreign_keys):
    if table_row["column_name"] in foreign_keys.keys():
        return get_relationship(table_row, foreign_keys)

    line = get_data_type(table_row)

    if str(table_row["column_name"]).endswith("_id"):
        line = f"{line}  # CHECK_RELATIONSHIP"

    return line


def inspect_table(table_name, table_description, foreign_keys):
    Jinja2.set_template("tortoise.py.jinja2")

    class_name = "".join(word.title() for word in table_name.split("_"))
    fields = []

    for table_row in table_description:
        fields.append(get_field(table_row, foreign_keys))

    with open(f"output/{table_name}.py", "w") as f:
        f.write(
            Jinja2.render(table_name=table_name, class_name=class_name, fields=fields)
        )


def get_fields():
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
    grouped_fields = collections.defaultdict(list)

    for row in rows:
        grouped_fields[row["table_name"]].append(row)

    return grouped_fields


def get_foreign_keys(key):
    ps_cursor.execute(
        f"""
            SELECT
                tc.table_name, kcu.column_name,
                ccu.table_name AS foreign_table_name,
                ccu.column_name AS foreign_column_name
            FROM
                information_schema.table_constraints AS tc
                JOIN information_schema.key_column_usage 
                    AS kcu ON tc.constraint_name = kcu.constraint_name
                JOIN information_schema.constraint_column_usage 
                    AS ccu ON ccu.constraint_name = tc.constraint_name
            WHERE constraint_type = 'FOREIGN KEY'
            AND tc.table_name = '{key}'
            ORDER BY tc.table_name, kcu.column_name;
        """
    )
    rows = ps_cursor.fetchall()
    grouped_foreign = dict()

    for row in rows:
        grouped_foreign[row["column_name"]] = row

    return grouped_foreign


def main():
    grouped_fields = get_fields()

    for table_name, table_configuration in grouped_fields.items():
        inspect_table(table_name, table_configuration, get_foreign_keys(table_name))


if __name__ == "__main__":
    main()
