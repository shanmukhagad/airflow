 .. Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

 ..   http://www.apache.org/licenses/LICENSE-2.0

 .. Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

.. _concepts:debugging:

Debugging Airflow Dags
======================

Testing Dags with dag.test()
-----------------------------

To debug Dags in an IDE, you can set up the ``dag.test`` command in your Dag file and run through your Dag in a single
serialized python process.

This approach can be used with any supported database (including a local SQLite database) and will
*fail fast* as all tasks run in a single process.

``dag.test()`` executes a real Dag run, so it has two prerequisites:

* an initialized metadata database (``airflow db migrate``), and
* a Dag that Airflow can serialize, which means the Dag must be defined in a file inside a
  configured :doc:`Dag bundle </administration-and-deployment/dag-bundles>` — by default, your
  Dags folder. ``dag.test()`` re-parses the bundle and serializes the Dag for you.

A Dag built in memory — for instance inside a pytest function, in a file outside your Dags folder —
is not in any bundle, so nothing gets serialized and the call fails with
``Cannot create DagRun for DAG <dag_id> because the dag is not serialized``. To test operator
behaviour without a Dag run at all, call the operator directly as described in
:ref:`Unit tests <best_practices:unit_tests>`.

To set up ``dag.test``, add these two lines to the bottom of your Dag file:

.. code-block:: python

  if __name__ == "__main__":
      dag.test()

and that's it! You can add optional arguments to fine tune the testing but otherwise you can run or debug Dags as
needed. Here are some examples of arguments:

* ``execution_date`` if you want to test argument-specific Dag runs
* ``use_executor`` if you want to test the Dag using an executor. By default ``dag.test`` runs the Dag without an
  executor, it just runs all the tasks locally.
  By providing this argument, the Dag is executed using the executor configured in the Airflow environment.

Conditionally skipping tasks
----------------------------

If you don't wish to execute some subset of tasks in your local environment (e.g. dependency check sensors or cleanup steps),
you can automatically mark them successful supplying a pattern matching their ``task_id`` in the ``mark_success_pattern`` argument.

In the following example, testing the Dag won't wait for either of the upstream Dags to complete. Instead, testing data
is manually ingested. The cleanup step is also skipped, making the intermediate csv is available for inspection.

.. code-block:: python

  with DAG("example_dag", default_args=default_args) as dag:
      sensor = ExternalTaskSensor(task_id="wait_for_ingestion_dag", external_dag_id="ingest_raw_data")
      sensor2 = ExternalTaskSensor(task_id="wait_for_dim_dag", external_dag_id="ingest_dim")
      collect_stats = PythonOperator(task_id="extract_stats_csv", python_callable=extract_stats_csv)
      # ... run other tasks
      cleanup = PythonOperator(task_id="cleanup", python_callable=Path.unlink, op_args=[collect_stats.output])

      [sensor, sensor2] >> collect_stats >> cleanup

  if __name__ == "__main__":
      ingest_testing_data()
      run = dag.test(mark_success_pattern="wait_for_.*|cleanup")
      print(f"Intermediate csv: {run.get_task_instance('collect_stats').xcom_pull(task_id='collect_stats')}")


Debugging Airflow Dags on the command line
==========================================

With the same two line addition as mentioned in the above section, you can now easily debug a Dag using pdb as well.
Run ``python -m pdb <path to Dag file>.py`` for an interactive debugging experience on the command line.

.. code-block:: bash

  [Breeze:3.10.19] root@ef2c84ad4856:/opt/airflow# python -m pdb providers/standard/src/airflow/providers/standard/example_dags/example_bash_operator.py
  > /opt/airflow/providers/standard/src/airflow/providers/standard/example_dags/example_bash_operator.py(18)<module>()
  -> """Example Dag demonstrating the usage of the BashOperator."""
  (Pdb) b 45
  Breakpoint 1 at /opt/airflow/providers/standard/src/airflow/providers/standard/example_dags/example_bash_operator.py:45
  (Pdb) c
  > /opt/airflow/providers/standard/src/airflow/providers/standard/example_dags/example_bash_operator.py(45)<module>()
  -> task_id="run_after_loop",
  (Pdb) run_this_last
  <Task(EmptyOperator): run_this_last>

**IDE setup steps:**

1. Add ``main`` block at the end of your Dag file to make it runnable.

.. code-block:: python

  if __name__ == "__main__":
      dag.test()

2. Run / debug the Dag file.


Real-World Debugging Strategies
================================

While CLI commands like ``dag.test()`` are useful during initial development, diagnosing issues in deployed environments requires analyzing runtime state, task logs, and environment configurations.

Filtering Task Logs by Severity
-------------------------------

Task execution logs in the Airflow UI can be verbose. When a task fails, routine ``INFO`` messages can obscure the underlying stack trace.

1. Navigate to the **Grid View** or **Graph View** and select the failed task instance.
2. Click the **Logs** tab.
3. Use the search bar in the log viewer to filter logs by severity or keywords (e.g., search for ``ERROR`` or ``WARNING``):
   
   * **``ERROR`` / ``CRITICAL``:** Isolates explicit Python exceptions, syntax errors, and fatal job terminations.
   * **``WARNING``:** Highlights non-fatal execution warnings, such as deprecation notices or retry attempts.

Log Keyword Searching
---------------------

To quickly locate intermittent errors in long-running tasks, use the log search field to query specific runtime indicators. 

Common search patterns include:

* **Exception Types:** Search for ``Traceback``, ``KeyError``, ``TypeError``, or ``DatabaseError``.
* **HTTP / API Failures:** Search for status codes such as ``401``, ``403``, ``500``, or network keywords like ``ConnectionRefusedError`` and ``Timeout``.
* **Resource Limits:** Search for ``Out of Memory``, ``OOMKilled``, or SIGKILL signals (``Exit code 137``).

Inspecting XCom Payloads
------------------------

Tasks often fail downstream because upstream tasks pushed malformed or missing XCom data. You can verify the exact data passed between tasks directly in the UI.

1. Navigate to the failed or upstream task instance in the Airflow UI.
2. Open the **XCom** section or tab to inspect the key-value pairs to confirm the data format matches downstream expectations.

Alternatively, pull and inspect XCom data safely inside a task using Python:

.. code-block:: python

    @task
    def process_data(**context):
        # Retrieve XCom value pushed by 'extract_data' task
        raw_payload = context["task_instance"].xcom_pull(task_ids="extract_data")
        
        # Log specific keys/structure rather than full raw payload to avoid log bloat and leak risks
        if isinstance(raw_payload, dict):
            print(f"DEBUG: Received XCom payload keys: {list(raw_payload.keys())}")


Validating Connections & Variables
----------------------------------

Missing or incorrectly named Airflow Connections and Variables are a frequent cause of ``AirflowException`` or connection timeouts at runtime.

* **Connections:** Navigate to **Admin -> Connections** to verify that the target Connection ID matches the ``conn_id`` declared in your operator (e.g., ``postgres_default``). Use the **Test Connection** button to validate network accessibility and credentials.
* **Variables:** Navigate to **Admin -> Variables** to ensure required runtime parameters exist. Require explicit configuration in code to fail fast if a variable is missing:

.. code-block:: python

    from airflow.exceptions import AirflowException
    from airflow.models import Variable

    # Fail explicitly if configuration is missing to avoid unintended fallbacks
    try:
        api_endpoint = Variable.get("API_ENDPOINT")
    except KeyError:
        raise AirflowException("Required Variable 'API_ENDPOINT' is missing from Airflow environment.")


Analyzing Historical Task Runs
------------------------------

Recurring or seasonal failures often point to external system bottlenecks rather than DAG code bugs.

1. Open the **Grid View** to view execution history across past DAG runs.
2. Check the duration chart to identify tasks with sudden spikes in execution time (e.g., database lock contention).
3. Review the **Gantt Chart** or **Details** tab to check task attempt counts and verify if task retries (``retries=3``) are triggering repeatedly due to transient network drops.