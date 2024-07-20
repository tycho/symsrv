#!/usr/bin/python

import os
import signal
import subprocess
import sys
import termios
import tomllib

# Implementation from https://stackoverflow.com/a/66727983
def run_as_fg_process(*args, **kwargs):
    """
    the "correct" way of spawning a new subprocess:
    signals like C-c must only go
    to the child process, and not to this python.

    the args are the same as subprocess.Popen

    returns Popen().wait() value

    Some side-info about "how ctrl-c works":
    https://unix.stackexchange.com/a/149756/1321

    fun fact: this function took a whole night
              to be figured out.
    """

    old_pgrp = os.tcgetpgrp(sys.stdin.fileno())
    old_attr = termios.tcgetattr(sys.stdin.fileno())

    user_preexec_fn = kwargs.pop("preexec_fn", None)

    def new_pgid():
        if user_preexec_fn:
            user_preexec_fn()

        # set a new process group id
        os.setpgid(os.getpid(), os.getpid())

        # generally, the child process should stop itself
        # before exec so the parent can set its new pgid.
        # (setting pgid has to be done before the child execs).
        # however, Python 'guarantee' that `preexec_fn`
        # is run before `Popen` returns.
        # this is because `Popen` waits for the closure of
        # the error relay pipe '`errpipe_write`',
        # which happens at child's exec.
        # this is also the reason the child can't stop itself
        # in Python's `Popen`, since the `Popen` call would never
        # terminate then.
        # `os.kill(os.getpid(), signal.SIGSTOP)`

    try:
        # fork the child
        child = subprocess.Popen(*args, preexec_fn=new_pgid,
                                 **kwargs)

        # we can't set the process group id from the parent since the child
        # will already have exec'd. and we can't SIGSTOP it before exec,
        # see above.
        # `os.setpgid(child.pid, child.pid)`

        # set the child's process group as new foreground
        os.tcsetpgrp(sys.stdin.fileno(), child.pid)
        # revive the child,
        # because it may have been stopped due to SIGTTOU or
        # SIGTTIN when it tried using stdout/stdin
        # after setpgid was called, and before we made it
        # forward process by tcsetpgrp.
        os.kill(child.pid, signal.SIGCONT)

        # wait for the child to terminate
        ret = child.wait()

    finally:
        # we have to mask SIGTTOU because tcsetpgrp
        # raises SIGTTOU to all current background
        # process group members (i.e. us) when switching tty's pgrp
        # it we didn't do that, we'd get SIGSTOP'd
        hdlr = signal.signal(signal.SIGTTOU, signal.SIG_IGN)
        # make us tty's foreground again
        os.tcsetpgrp(sys.stdin.fileno(), old_pgrp)
        # now restore the handler
        signal.signal(signal.SIGTTOU, hdlr)
        # restore terminal attributes
        termios.tcsetattr(sys.stdin.fileno(), termios.TCSADRAIN, old_attr)

    return ret


def start_uvicorn():
    with open("config/main.toml", "rb") as f:
        main_config = tomllib.load(f)

    uvicorn = main_config['uvicorn']

    command = [
        "python",
        "-m", "uvicorn"
    ]

    if 'unix_socket' in uvicorn:
        command += ["--uds", uvicorn['unix_socket']]
    elif 'tcp_port' in uvicorn:
        command += ["--port", str(uvicorn['tcp_port'])]
    else:
        raise ValueError("Could not find an appropriate listen config in config/main.toml")

    if uvicorn.get('proxy_headers', False):
        command += ["--proxy-headers"]
        if 'forwarded_allow_ips' in uvicorn:
            command += [f"--forwarded-allow-ips", uvicorn['forwarded_allow_ips']]

    command += [
        "--workers", str(uvicorn.get('workers', 8)),
        "--log-config=config/log_config.yml",
        "symsrv:app"
    ]

    run_as_fg_process(command)
