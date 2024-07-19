#!/usr/bin/python

import tomllib
import subprocess

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

    command += [
        "--workers", str(uvicorn.get('workers', 8)),
        "symsrv:app"
    ]
    subprocess.run(command)
