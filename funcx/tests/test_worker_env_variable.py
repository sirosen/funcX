import json
import sys
import time
import argparse

from funcx.sdk.client import FuncXClient

func_plat = """
def test_plat(event):
    import platform
    return platform.uname()
"""

func_sum = """
def test_sum_1(event):
    return sum(event)
"""

def sum_yadu_new01(event):
    return sum(event)

"""
@funcx.register(description="...")
def sum_yadu_new01(event):
    return sum(event)
"""

def test_env(event):
    import os
    return os.environ['REDIS_SERVERS']

def test(fxc, ep_id):

    fn_uuid = fxc.register_function(test_env,
                                    ep_id, # TODO: We do not need ep id here
                                    description="New sum function defined without string spec")
    print("FN_UUID : ", fn_uuid)


    res = fxc.run([1,2,3,99], endpoint_id=ep_id, function_id=fn_uuid)
    print(res)
    while True:
        try:
            result = fxc.get_result(res)
            break
        except Exception as e:
            print(e)
        time.sleep(2)
    print(result)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-e", "--endpoint", required=True)
    args = parser.parse_args()

    fxc = FuncXClient()
    test(fxc, args.endpoint)
