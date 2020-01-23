import json
import sys
import argparse
import time
import os
import pickle
from funcx.sdk.client import FuncXClient


def get_time(launch_time):
    import time
    time.sleep(0.100) # Sleep 100ms
    return launch_time, time.time()

def test(fxc, ep_id):

    inter_task_duration = 4
    fn_uuid = fxc.register_function(get_time,
                                    ep_id, # TODO: We do not need ep id here
                                    description="New sum function defined without string spec")
    print("FN_UUID : ", fn_uuid)

    func_ids  = []
    for i in range(50):
        func_id = fxc.run(time.time(), endpoint_id=ep_id, function_id=fn_uuid)
        func_ids.append(func_id)
        print("Launched task : {}".format(func_id))
        if i == 10:
            start = time.time()
            time.sleep(0.5)
            print("Triggering endpoint shutdown at ", time.time())
            os.system("funcx-endpoint stop testing_1")
            trigger_t = time.time()
            t_left = inter_task_duration - (time.time() - start)
            print("Waking up in : {}".format(t_left))
            time.sleep(t_left)
        elif i == 20:
            start = time.time()
            time.sleep(0.5)
            print("Triggering endpoint start at ", time.time())
            os.system("funcx-endpoint start testing_1")
            restart_t = time.time()
            t_left = inter_task_duration - (time.time() - start)
            print("Waking up in : {}".format(t_left))
            time.sleep(t_left)
        else:
            time.sleep(inter_task_duration) # 100 ms gap

    time.sleep(5)

    data = {"trigger_t" : trigger_t,
            "restart_t" : restart_t,
            "data" : None}
    results = []
    for item in func_ids:
        print("Waiting for ", item)
        while True:
            try:
                x = fxc.get_result(item)
                results.append(x)
                print("Got result for item {}: {}".format(item, x))
                break
            except:
                print("Waiting")
                time.sleep(1)
    data["data"] = results

    with open("experiment.pkl", 'wb') as f:
        pickle.dump(data, f)

    print(data)



if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-e", "--endpoint", required=True)
    args = parser.parse_args()

    fxc = FuncXClient()
    test(fxc, args.endpoint)
