from airflow.listeners import hookimpl


@hookimpl
def on_task_instance_failed(previous_state, task_instance, error):
    print(
        f"[LISTENER] FAILED HOOK task={task_instance.task_id} "
        f"try={task_instance.try_number} state={getattr(task_instance, 'state', 'n/a')}",
        flush=True,
    )


@hookimpl
def on_task_instance_success(previous_state, task_instance):
    print(
        f"[LISTENER] SUCCESS HOOK task={task_instance.task_id} "
        f"try={task_instance.try_number} state={getattr(task_instance, 'state', 'n/a')}",
        flush=True,
    )


@hookimpl
def on_task_instance_up_for_retry(previous_state, task_instance, error):
    print(
        f"[LISTENER] UP_FOR_RETRY HOOK task={task_instance.task_id} "
        f"try={task_instance.try_number} state={getattr(task_instance, 'state', 'n/a')}",
        flush=True,
    )