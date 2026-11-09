from airflow.dag_processing.importers.python_importer import ( 
    parse_dag_file, 
    SegfaultImportError 
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