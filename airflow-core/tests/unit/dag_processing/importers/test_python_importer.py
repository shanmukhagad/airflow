from __future__ import annotations

import zipfile
from pathlib import Path

from airflow.dag_processing.importers.python_importer import (
    PythonDagImporter,
    SegfaultImportError,
    parse_dag_file,
)


def test_python_importer_handles_sigsegv(mocker):
    mock_record_error = mocker.patch(
        target="airflow.dag_processing.importers.python_importer.record_import_error"
    )

    mocker.patch(
        target="airflow.dag_processing.importers.python_importer._execute_import",
        side_effect=SegfaultImportError("Received SIGSEGV signal while importing DAG file."),
    )

    result = parse_dag_file("/tmp/fake_dag.py")

    assert result is None
    mock_record_error.assert_called_once_with(
        "/tmp/fake_dag.py",
        "Received SIGSEGV signal while importing DAG file.",
    )


def test_list_dag_files_ignores_non_zip_archives(tmp_path):
    dags_dir = tmp_path / "dags"
    dags_dir.mkdir()

    valid_py = dags_dir / "valid_dag.py"
    valid_py.write_text("from airflow import DAG\ndag = DAG('valid_py')\n")

    valid_zip = dags_dir / "valid_bundle.zip"
    with zipfile.ZipFile(valid_zip, "w") as zf:
        zf.writestr("bundled_dag.py", "from airflow import DAG\ndag = DAG('valid_zip')\n")

    valid_upper_zip = dags_dir / "upper_bundle.ZIP"
    with zipfile.ZipFile(valid_upper_zip, "w") as zf:
        zf.writestr("upper_dag.py", "from airflow import DAG\ndag = DAG('upper_zip')\n")

    non_zip_archives = [
        "spark_job.jar",
        "architecture.docx",
        "metrics.xlsx",
        "presentation.pptx",
        "dependency.whl",
    ]
    for filename in non_zip_archives:
        archive_path = dags_dir / filename
        with zipfile.ZipFile(archive_path, "w") as zf:
            zf.writestr(
                "content.xml",
                "from airflow import DAG\ndag = DAG('ignored_archive')\n",
            )

    importer = PythonDagImporter()
    discovered = {Path(p).name for p in importer.list_dag_files(str(dags_dir), safe_mode=False)}

    assert "valid_dag.py" in discovered
    assert "valid_bundle.zip" in discovered
    assert "upper_bundle.ZIP" in discovered
    for filename in non_zip_archives:
        assert filename not in discovered
