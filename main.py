#!/usr/bin/env python
import asyncio
import functools
import time
import threading
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
from joblib import Parallel, delayed
from more_itertools import chunked

concurrent = 1000

data = list(range(100000))
groups = list(chunked(data, concurrent))


def profile(logger=None):
    """
    :param logger:
    :return:
    """
    def _profile(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            start = time.time()
            res = func(*args, **kwargs)
            r = time.time() - start
            msg = '<{}> exec time: {} sec'.format(func.__name__, r)
            if logger is not None:
                logger.debug(msg)
            else:
                print(msg)
            return res
        return wrapper
    return _profile


def is_prime(n):
    """check the prime number"""
    if n < 2:
        return False
    elif n == 2:
        return True
    elif n % 2 == 0:
        return False
    i = 3
    while i*i <= n:
        if n % i == 0:
            return False
        i += 2
    return True


def _func(numbers):
    return [is_prime(n) for n in numbers]


@profile()
def exam_process_pool():

    with ProcessPoolExecutor(max_workers=20) as executor:
        #results = [executor.map(_func, group) for group in groups]
        #print('number: {}'.format(results))
        futures = {executor.submit(_func, group): group for group in groups}
        for future in as_completed(futures):
            r = futures[future]
            #print('future: {}'.format(r))
            #try:
            #    result = future.result()
            #    print("Result: {}".format(result))
            #except Exception as e:
            #    print("Exception:", e)
            #else:
            #    print('Data length = {}'.format(len(r)))


@profile()
def exam_thread_pool():

    def func(numbers):
        return [is_prime(n) for n in numbers]

    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = {executor.submit(func, group): group for group in groups}
        for future in as_completed(futures):
            r = futures[future]
            #print('future: {}'.format(r))
            #try:
            #    result = future.result()
            #    print("Result: {}".format(result))
            #except Exception as e:
            #    print("Exception:", e)
            #else:
            #    print('Data length = {}'.format(len(r)))


@profile()
def exam_thread():

    def func(*args):
        return [is_prime(n) for n in args]

    for group in groups:
        threading.Thread(target=func, args=group).start()


@profile()
def exam_asyncio():

    async def app(numbers):
        #await asyncio.sleep(random.choice([.5, .8, 1.0]))
        return [is_prime(n) for n in numbers]

    async def func():
        await asyncio.wait([app(group) for group in groups])

    loop = asyncio.get_event_loop()
    loop.run_until_complete(func())
    loop.close()


@profile()
def exam_blocking():
    results = []
    for group in groups:
        for num in group:
            results.append(is_prime(num))
    return results


@profile()
def exam_joblib():
    Parallel(n_jobs=20)([delayed(_func)(numbers) for numbers in groups])


@profile()
def exam_joblib_thread():
    Parallel(n_jobs=20, backend='threading')([delayed(_func)(numbers) for numbers in groups])


def main():
    exam_process_pool()
    exam_thread_pool()
    exam_thread()
    exam_asyncio()
    exam_blocking()
    exam_joblib()
    exam_joblib_thread()


if __name__ == "__main__":
    main()
