import importlib.util

from cerberus import Validator
from docutils import nodes

# from docutils.parsers.rst import Directive
from sphinx.util.docutils import SphinxDirective


class CerberusSchemaDirective(SphinxDirective):
    has_content = True
    required_arguments = 1
    optional_arguments = 3
    final_argument_whitespace = True
    option_spec = {
        "validator": str,
        "module": str,
        "schema": str,
    }

    def run(self):
        schema_title = self.arguments[0]
        validator_class = self.options.get("validator", Validator)
        module_name = self.options.get("module")
        schema = self.options.get("schema")

        if not module_name:
            return [
                nodes.error(
                    None,
                    nodes.title(text=schema_title),
                    nodes.paragraph(text="No module specified"),
                )
            ]

        try:
            module = importlib.import_module(module_name)
            schema = getattr(module, schema)
            print(f"Schema: {schema}")

        except ImportError as e:
            return [
                nodes.error(
                    None,
                    nodes.title(text=schema_title),
                    nodes.paragraph(text=f"Error importing module: {str(e)}"),
                )
            ]
        except AttributeError as e:
            return [
                nodes.error(
                    None,
                    nodes.title(text=schema_title),
                    nodes.paragraph(text=f"Error getting schema: {str(e)}"),
                )
            ]

        if isinstance(validator_class, str):
            # import:
            spec = importlib.util.find_spec(validator_class)
            module = importlib.util.module_from_spec(spec)
            validator_class = spec.loader.exec_module(module)

        try:
            validator_class(schema)
        except Exception as e:
            return [
                nodes.error(
                    None,
                    nodes.title(text=schema_title),
                    nodes.paragraph(text=f"Error in schema: {str(e)}"),
                    nodes.literal_block(text=schema),
                )
            ]

        table = nodes.table()
        tgroup = nodes.tgroup(cols=5)
        table += tgroup

        for _ in range(5):
            tgroup += nodes.colspec(colwidth=1)

        thead = nodes.thead()
        tgroup += thead
        row = nodes.row()
        thead += row
        row += nodes.entry("", nodes.paragraph(text="Field"))
        row += nodes.entry("", nodes.paragraph(text="Type"))
        row += nodes.entry("", nodes.paragraph(text="Required"))
        row += nodes.entry("", nodes.paragraph(text="Default"))
        row += nodes.entry("", nodes.paragraph(text="Description"))

        tbody = nodes.tbody()
        tgroup += tbody

        def add_schema_to_table(schema, tbody, parent_key="", level=0):
            if isinstance(schema, dict):
                for key, value in schema.items():
                    add_field_to_table(key, value, tbody, parent_key, level)
            elif isinstance(schema, list):
                # For list schemas, we'll show the structure of the first item
                if schema:
                    add_field_to_table("item", schema[0], tbody, parent_key, level)

        def add_field_to_table(key, value, tbody, parent_key="", level=0):
            full_key = f"{parent_key}.{key}" if parent_key else key
            print(f"Adding field {full_key} to table")
            row = nodes.row()
            tbody += row

            # Field name
            field_name = full_key
            row += nodes.entry("", nodes.paragraph(text=field_name))

            # Type and structure
            field_type = get_field_type(value)
            row += nodes.entry("", nodes.paragraph(text=field_type))

            # Required
            required = "Required" if isinstance(value, dict) and value.get("required", False) else "Optional"
            row += nodes.entry("", nodes.paragraph(text=required))

            default_value = get_default(value)
            row += nodes.entry("", nodes.paragraph(text=default_value))

            # Constraints and description
            description = get_field_description(value)
            row += nodes.entry("", nodes.paragraph(text=description))

            # Recursive handling of nested structures
            if isinstance(value, dict):
                if "schema" in value:
                    nested_schema = value["schema"]
                    if isinstance(nested_schema, dict):
                        if nested_schema.get("type") == "dict":
                            add_schema_to_table(
                                nested_schema.get("schema", {}),
                                tbody,
                                full_key,
                                level + 1,
                            )
                        else:
                            add_schema_to_table(nested_schema, tbody, full_key, level + 1)
                    elif isinstance(nested_schema, list):
                        add_schema_to_table(nested_schema, tbody, full_key, level + 1)
                elif value.get("type") == "dict":
                    add_schema_to_table(value.get("schema", {}), tbody, full_key, level + 1)

        def get_default(value):
            if not isinstance(value, dict):
                return ""

            if "default" in value:
                return value["default"]
            return ""

        def get_field_type(value):
            if isinstance(value, dict):
                base_type = value.get("type", "dict")
                if base_type == "list":
                    if isinstance(value.get("schema"), dict):
                        return f"list of {get_field_type(value['schema'])}"
                    elif isinstance(value.get("schema"), list):
                        return "list of multiple types"
                    else:
                        return "list"
                return base_type
            elif isinstance(value, str):
                return value
            else:
                return str(type(value).__name__)

        def get_field_description(value):
            if not isinstance(value, dict):
                return ""

            description = value.get("help", "")
            constraints = []
            if "allowed" in value:
                constraints.append(f"Allowed: {', '.join(map(str, value['allowed']))}")
            if "excludes" in value:
                constraints.append(f"Excludes: {value['excludes']}")
            if "is_qualname" in value:
                constraints.append("Must be a valid Python qualname")
            if constraints:
                description += " (" + "; ".join(constraints) + ")"
            return description

        add_schema_to_table(schema, tbody)

        return [nodes.title(text=schema_title), table]


def setup(app):
    app.add_directive("cerberus-schema", CerberusSchemaDirective)

    return {
        "version": "0.1",
        "parallel_read_safe": True,
        "parallel_write_safe": True,
    }
