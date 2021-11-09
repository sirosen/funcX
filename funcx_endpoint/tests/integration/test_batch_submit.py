import argparse
import time

import funcx
from funcx.sdk.client import FuncXClient
from funcx.serialize import FuncXSerializer

fxs = FuncXSerializer()

# funcx.set_stream_logger()


def double(x):
    return x * 2


def test(fxc, ep_id, task_count=10):

    fn_uuid = fxc.register_function(double, description="Yadu double")
    print("FN_UUID : ", fn_uuid)

    start = time.time()
    task_ids = fxc.map_run(
        list(range(task_count)), endpoint_id=ep_id, function_id=fn_uuid
    )
    delta = time.time() - start
    print(f"Time to launch {task_count} tasks: {delta:8.3f} s")
    print(f"Got {len(task_ids)} tasks_ids ")

    for _i in range(3):
        x = fxc.get_batch_result(task_ids)
        complete_count = sum(
            [1 for t in task_ids if t in x and x[t].get("pending", False)]
        )
        print(f"Batch status : {complete_count}/{len(task_ids)} complete")
        if complete_count == len(task_ids):
            break
        time.sleep(2)

    delta = time.time() - start
    print(f"Time to complete {task_count} tasks: {delta:8.3f} s")
    print(f"Throughput : {task_count / delta:8.3f} Tasks/s")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-e", "--endpoint", required=True)
    parser.add_argument("-c", "--count", default="10")
    args = parser.parse_args()

    print("FuncX version : ", funcx.__version__)
    fxc = FuncXClient(funcx_service_address="https://dev.funcx.org/api/v1")
    test(fxc, args.endpoint, task_count=int(args.count))
