import collections
import logging
import queue
import random

log = logging.getLogger(__name__)
log.info("Interchange task dispatch started")


def naive_interchange_task_dispatch(
    interesting_managers,
    pending_task_queue,
    ready_manager_queue,
    scheduler_mode="hard",
    cold_routing=False,
):
    """
    This is an initial task dispatching algorithm for interchange.
    It returns a dictionary, whose key is manager, and the value is the list of tasks
    to be sent to manager, and the total number of dispatched tasks.
    """
    if scheduler_mode == "hard":
        return dispatch(
            interesting_managers,
            pending_task_queue,
            ready_manager_queue,
            scheduler_mode="hard",
        )

    elif scheduler_mode == "soft":
        task_dispatch, dispatched_tasks = {}, 0
        loops = ["warm"] if not cold_routing else ["warm", "cold"]
        for loop in loops:
            task_dispatch, dispatched_tasks = dispatch(
                interesting_managers,
                pending_task_queue,
                ready_manager_queue,
                scheduler_mode="soft",
                loop=loop,
                task_dispatch=task_dispatch,
                dispatched_tasks=dispatched_tasks,
            )
        return task_dispatch, dispatched_tasks


def dispatch(
    interesting_managers,
    pending_task_queue,
    ready_manager_queue,
    scheduler_mode="hard",
    loop="warm",
    task_dispatch=None,
    dispatched_tasks=0,
):
    """
    This is the core task dispatching algorithm for interchange.
    The algorithm depends on the scheduler mode and which loop.
    """
    if not task_dispatch:
        task_dispatch = {}
    if interesting_managers:
        shuffled_managers = list(interesting_managers)
        random.shuffle(shuffled_managers)
        for manager in shuffled_managers:
            tasks_inflight = ready_manager_queue[manager]["total_tasks"]
            real_capacity = min(
                ready_manager_queue[manager]["free_capacity"]["total_workers"],
                ready_manager_queue[manager]["max_worker_count"] - tasks_inflight,
            )
            if real_capacity and ready_manager_queue[manager]["active"]:
                if scheduler_mode == "hard":
                    tasks, tids = get_tasks_hard(
                        pending_task_queue, ready_manager_queue[manager], real_capacity
                    )
                else:
                    tasks, tids = get_tasks_soft(
                        pending_task_queue,
                        ready_manager_queue[manager],
                        real_capacity,
                        loop=loop,
                    )
                log.debug(f"[MAIN] Get tasks {tasks} from queue")
                if tasks:
                    for task_type in tids:
                        # This line is a set update, not dict update
                        ready_manager_queue[manager]["tasks"][task_type].update(
                            tids[task_type]
                        )
                    log.debug(
                        "[MAIN] The tasks on manager {} is {}".format(
                            manager, ready_manager_queue[manager]["tasks"]
                        )
                    )
                    ready_manager_queue[manager]["total_tasks"] += len(tasks)
                    if manager not in task_dispatch:
                        task_dispatch[manager] = []
                    task_dispatch[manager] += tasks
                    dispatched_tasks += len(tasks)
                    log.debug(f"[MAIN] Assigned tasks {tids} to manager {manager}")
                if ready_manager_queue[manager]["free_capacity"]["total_workers"] > 0:
                    log.debug(
                        "[MAIN] Manager {} still has free_capacity {}".format(
                            manager,
                            ready_manager_queue[manager]["free_capacity"][
                                "total_workers"
                            ],
                        )
                    )
                else:
                    log.debug(f"[MAIN] Manager {manager} is now saturated")
                    interesting_managers.remove(manager)
            else:
                interesting_managers.remove(manager)

    log.debug(
        "The task dispatch of {} loop is {}, in total {} tasks".format(
            loop, task_dispatch, dispatched_tasks
        )
    )
    return task_dispatch, dispatched_tasks


def get_tasks_hard(pending_task_queue, manager_ads, real_capacity):
    tasks = []
    tids = collections.defaultdict(set)
    task_type = manager_ads["worker_type"]
    if not task_type:
        log.warning(
            "Using hard scheduler mode but with manager worker type unset. "
            "Use soft scheduler mode. Set this in the config."
        )
        return tasks, tids
    if task_type not in pending_task_queue:
        log.debug(f"No task of type {task_type}. Exiting task fetching.")
        return tasks, tids

    # dispatch tasks of available types on manager
    if task_type in manager_ads["free_capacity"]["free"]:
        while manager_ads["free_capacity"]["free"][task_type] > 0 and real_capacity > 0:
            try:
                x = pending_task_queue[task_type].get(block=False)
            except queue.Empty:
                break
            else:
                log.debug(f"Get task {x}")
                tasks.append(x)
                tids[task_type].add(x["task_id"])
                manager_ads["free_capacity"]["free"][task_type] -= 1
                manager_ads["free_capacity"]["total_workers"] -= 1
                real_capacity -= 1

    # dispatch tasks to unused slots based on the manager type
    log.debug("Second round of task fetching in hard mode")
    while manager_ads["free_capacity"]["free"]["unused"] > 0 and real_capacity > 0:
        try:
            x = pending_task_queue[task_type].get(block=False)
        except queue.Empty:
            break
        else:
            log.debug(f"Get task {x}")
            tasks.append(x)
            tids[task_type].add(x["task_id"])
            manager_ads["free_capacity"]["free"]["unused"] -= 1
            manager_ads["free_capacity"]["total_workers"] -= 1
            real_capacity -= 1
    return tasks, tids


def get_tasks_soft(pending_task_queue, manager_ads, real_capacity, loop="warm"):
    tasks = []
    tids = collections.defaultdict(set)

    # Warm routing to dispatch tasks
    if loop == "warm":
        for task_type in manager_ads["free_capacity"]["free"]:
            # Dispatch tasks that are of the available container types on the manager
            if task_type != "unused":
                type_inflight = len(manager_ads["tasks"].get(task_type, set()))
                type_capacity = min(
                    manager_ads["free_capacity"]["free"][task_type],
                    manager_ads["free_capacity"]["total"][task_type] - type_inflight,
                )
                while (
                    manager_ads["free_capacity"]["free"][task_type] > 0
                    and real_capacity > 0
                    and type_capacity > 0
                ):
                    try:
                        if task_type not in pending_task_queue:
                            break
                        x = pending_task_queue[task_type].get(block=False)
                    except queue.Empty:
                        break
                    else:
                        log.debug(f"Get task {x}")
                        tasks.append(x)
                        tids[task_type].add(x["task_id"])
                        manager_ads["free_capacity"]["free"][task_type] -= 1
                        manager_ads["free_capacity"]["total_workers"] -= 1
                        real_capacity -= 1
                        type_capacity -= 1
            # Dispatch tasks to unused container slots on the manager
            else:
                task_types = list(pending_task_queue.keys())
                random.shuffle(task_types)
                for task_type in task_types:
                    while (
                        manager_ads["free_capacity"]["free"]["unused"] > 0
                        and manager_ads["free_capacity"]["total_workers"] > 0
                        and real_capacity > 0
                    ):
                        try:
                            x = pending_task_queue[task_type].get(block=False)
                        except queue.Empty:
                            break
                        else:
                            log.debug(f"Get task {x}")
                            tasks.append(x)
                            tids[task_type].add(x["task_id"])
                            manager_ads["free_capacity"]["free"]["unused"] -= 1
                            manager_ads["free_capacity"]["total_workers"] -= 1
                            real_capacity -= 1
        return tasks, tids

    # Cold routing round: allocate tasks of random types
    # to workers that are of different types on the manager
    # This will possibly cause container switching on the manager
    # This is needed to avoid workers being idle for too long
    # Potential issues may be that it could kill containers of short tasks frequently
    # Tune cold_routing_interval in the config to balance such a tradeoff
    log.debug("Cold function routing!")
    task_types = list(pending_task_queue.keys())
    random.shuffle(task_types)
    for task_type in task_types:
        while manager_ads["free_capacity"]["total_workers"] > 0 and real_capacity > 0:
            try:
                x = pending_task_queue[task_type].get(block=False)
            except queue.Empty:
                break
            else:
                log.debug(f"Get task {x}")
                tasks.append(x)
                tids[task_type].add(x["task_id"])
                manager_ads["free_capacity"]["total_workers"] -= 1
                real_capacity -= 1
    return tasks, tids
