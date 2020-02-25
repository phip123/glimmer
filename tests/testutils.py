import time


def poll(condition, timeout=None, interval=0.5):
    remaining = 0
    if timeout is not None:
        remaining = timeout

    while not condition():
        if timeout is not None:
            remaining -= interval

            if remaining <= 0:
                raise TimeoutError('gave up waiting after %s seconds' % timeout)

        time.sleep(interval)


def assert_poll(condition, msg='Condition failed'):
    try:
        poll(condition, 2, 0.01)
    except TimeoutError:
        raise AssertionError(msg)
