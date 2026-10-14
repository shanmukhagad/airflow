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

.. _troubleshooting:

Troubleshooting
===============

Obscure task failures
^^^^^^^^^^^^^^^^^^^^^

.. _troubleshooting-task-state-changed-externally:

Task state changed externally
-----------------------------

There are many potential causes for a task's state to be changed by a component other than the executor, which might cause some confusion when reviewing task instance or scheduler logs.

What this message means
~~~~~~~~~~~~~~~~~~~~~~~

A task's state can be updated by different Airflow components or by external actions. If the state reported by the executor differs from the state stored for the task instance, Airflow may log a state mismatch.

For example, you might see a message similar to::

    Executor <executor> reported that the task instance <ti> finished with state <executor_state>, but the task instance's state attribute is <ti_state>.

Check the logs around the time of the state change to determine what caused it.

How to investigate
~~~~~~~~~~~~~~~~~~

Start by checking the task and scheduler logs around the time the state changed. If the cause is not clear, also check the worker or executor logs and the logs of the infrastructure running the worker.

1. Check the task logs. If no task logs are available, the task may never have started or the worker may have stopped before producing logs.
2. Check the scheduler logs around the same timestamp to see whether the scheduler changed the task state.
3. Check the worker or executor logs to determine whether the task was received and started.
4. Check infrastructure logs for events such as out-of-memory kills, pod or container eviction, worker restarts, or other failures.
5. Check whether a user or external process changed the task state through the Airflow UI or REST API.

Below are some example scenarios that could cause a task's state to change by a component other than the executor:

- If a task's Dag failed to parse on the worker, the scheduler may mark the task as failed. If confirmed, consider increasing :ref:`core.dagbag_import_timeout <config:core__dagbag_import_timeout>` and :ref:`dag_processor.dag_file_processor_timeout <config:dag_processor__dag_file_processor_timeout>`.
- The scheduler will mark a task as failed if the task has been queued for longer than :ref:`scheduler.task_queued_timeout <config:scheduler__task_queued_timeout>`.
- If a :ref:`task instance's heartbeat times out <concepts:task-instance-heartbeat-timeout>`, it will be marked failed by the scheduler.
- A user marked the task as successful or failed in the Airflow UI.
- An external script or process used the :doc:`Airflow REST API <stable-rest-api-ref>` to change the state of a task.

.. _troubleshooting-task-stuck-queued:

Task stuck in queued state
--------------------------

A task remains in the ``queued`` state while it is waiting to be picked up and executed by a worker. If a task stays queued for longer than :ref:`scheduler.task_queued_timeout <config:scheduler__task_queued_timeout>`, the scheduler may retry or fail the task.

Common causes include:

- Insufficient worker capacity to execute the queued tasks.
- The executor is unable to dispatch the task to a worker.
- The queued timeout is too short for the workload and available capacity.

To troubleshoot a task that is stuck in the queued state:

1. Check the scheduler logs for messages about the task and whether it was dispatched.
2. Check that workers are running and accepting new tasks.
3. Check the executor configuration and logs for dispatch or communication failures.
4. If the system is healthy but tasks regularly exceed the timeout, consider increasing :ref:`scheduler.task_queued_timeout <config:scheduler__task_queued_timeout>` or adding worker capacity.

.. _troubleshooting-task-stuck-running:

Task stuck in running state
---------------------------

A task may remain in the ``running`` state even though it is no longer making progress. If a task instance stops sending heartbeats, the scheduler can detect the heartbeat timeout and fail or reschedule the task.

Common causes include:

- The worker running the task was killed because of an out-of-memory condition.
- The worker stopped responding because it was restarted, evicted, or scaled down.
- A worker liveness check failed and the worker was restarted.

To troubleshoot a task that is stuck in the running state:

1. Check the task logs to determine whether the task stopped making progress unexpectedly.
2. Check worker and infrastructure logs around the same timestamp for OOM kills, restarts, evictions, or liveness probe failures.
3. Check whether the worker responsible for the task is still running and healthy.
4. If the worker appears healthy but heartbeat timeouts continue, review :ref:`scheduler.task_instance_heartbeat_timeout <config:scheduler__task_instance_heartbeat_timeout>`.

.. _troubleshooting-process-terminated-by-signal:

Process terminated by signal
----------------------------

Sometimes, Airflow or some adjacent system will kill a task instance's ``TaskRunner``, causing the task instance to fail.

Below we discuss a few common cases.

Dag run timeout
"""""""""""""""

A dag run timeout can be specified by ``dagrun_timeout`` in the dag's definition.
The task process would likely be killed with SIGTERM (exit code -15).

.. _troubleshooting-oom:

Out of memory error (OOM)
"""""""""""""""""""""""""

When a task process consumes too much memory for a worker, the best case scenario is it is killed
with SIGKILL (exit code -9). Depending on configuration and infrastructure, it is also
possible that the whole worker will be killed due to OOM and then the tasks would be marked as
failed after failing to heartbeat.

Lingering task supervisor processes
-----------------------------------

Under very high concurrency the socket handlers inside the task supervisor may
miss the final EOF events from the task process. When this occurs the supervisor
believes sockets are still open and will not exit. The
:ref:`workers.socket_cleanup_timeout <config:workers__socket_cleanup_timeout>` option controls how long the supervisor
waits after the task finishes before force-closing any remaining sockets. If you
observe leftover ``supervisor`` processes, consider increasing this delay.
