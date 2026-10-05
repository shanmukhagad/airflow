"""Sphinx extension to auto-generate RBAC reference table from BaseAuthManager."""

from __future__ import annotations

import inspect
from typing import TYPE_CHECKING, Any

from docutils import nodes
from docutils.parsers.rst import Directive

# Airflow Core's BaseAuthManager import
from airflow.api_fastapi.auth.managers.base_auth_manager import BaseAuthManager

if TYPE_CHECKING:
    from sphinx.application import Sphinx


class RBACRefDirective(Directive):
    """Custom Sphinx directive (`.. rbac-ref::`) to render RBAC permissions table."""

    has_content = False

    def run(self) -> list[nodes.Node]:
        # 1. Extract all `is_authorized_*` methods from BaseAuthManager.
        # FIX: use inspect.isroutine so that class methods can be accurately catched
        auth_methods = [
            (name, func)
            for name, func in inspect.getmembers(BaseAuthManager, predicate=inspect.isroutine)
            if name.startswith("is_authorized_")
        ]

        # Methods ko alphabetical order mein sort karna
        auth_methods.sort(key=lambda x: x[0])

        if not auth_methods:
            # Fallback warning if no methods found
            error_node = nodes.warning()
            error_node += nodes.paragraph(text="No RBAC methods found on BaseAuthManager.")
            return [error_node]

        # 2. Table structure create karna (docutils nodes)
        table = nodes.table()
        tgroup = nodes.tgroup(cols=3)
        table += tgroup

        # Table Column Widths define karna
        tgroup += nodes.colspec(colwidth=30)  # Method Name
        tgroup += nodes.colspec(colwidth=25)  # Resource Scope / Parameters
        tgroup += nodes.colspec(colwidth=45)  # Description / Docstring

        # Table Header Row
        thead = nodes.thead()
        tgroup += thead
        header_row = nodes.row()
        thead += header_row

        headers = ["Permission Method", "Action / Resource Parameters", "Description"]
        for header_text in headers:
            entry = nodes.entry()
            entry += nodes.paragraph(text=header_text)
            header_row += entry

        # Table Body
        tbody = nodes.tbody()
        tgroup += tbody

        # 3. Convert each method details to table rows
        for method_name, method_func in auth_methods:
            row = nodes.row()

            # Column 1: Method Name (with Code formatting)
            col1 = nodes.entry()
            col1 += nodes.literal(text=method_name)
            row += col1

            # Column 2: extract parameters & Type hints 
            col2 = nodes.entry()
            try:
                sig = inspect.signature(method_func)
                params = []
                for param in sig.parameters.values():
                    if param.name == "self":
                        continue
                    if param.annotation != inspect.Parameter.empty:
                        ann = getattr(param.annotation, "__name__", str(param.annotation))
                        params.append(f"{param.name}: {ann}")
                    else:
                        params.append(param.name)
                param_text = ", ".join(params) if params else "None"
            except Exception:
                param_text = "N/A"

            col2 += nodes.paragraph(text=param_text)
            row += col2

            # Column 3: Docstring Clean and Format
            col3 = nodes.entry()
            doc = inspect.getdoc(method_func) or "No description available."
            # Summary first extract the paragraph
            doc_summary = doc.split("\n\n")[0].replace("\n", " ")
            col3 += nodes.paragraph(text=doc_summary)
            row += col3

            tbody += row

        return [table]


def setup(app: Sphinx) -> dict[str, Any]:
    """Register directive with Sphinx engine."""
    app.add_directive("rbac-ref", RBACRefDirective)

    return {
        "version": "0.1",
        "parallel_read_safe": True,
        "parallel_write_safe": True,
    }