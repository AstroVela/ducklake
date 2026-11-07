"""Owned, disposable PostgreSQL and MinIO processes for R6 conformance tests."""

import hashlib
import os
from pathlib import Path
import secrets
import shutil
import socket
import subprocess
import time
import urllib.request

import boto3
from botocore.config import Config
import psycopg


MINIO_SHA = "7c5bd8512c6e966455b1d198209358b2d191c77a83ab377c4073281065fb855f"


def port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def wait_ready(check, process, seconds=30):
    deadline = time.monotonic() + seconds
    while True:
        if process.poll() is not None:
            raise AssertionError("Owned test service exited before readiness")
        try:
            if check():
                return
        except (OSError, psycopg.OperationalError):
            pass
        if time.monotonic() >= deadline:
            raise AssertionError("Owned test service readiness deadline exceeded")
        time.sleep(0.05)


def client(endpoint, access, secret):
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        region_name="us-east-1",
        aws_access_key_id=access,
        aws_secret_access_key=secret,
        config=Config(
            signature_version="s3v4",
            retries={"total_max_attempts": 1},
            s3={"addressing_style": "path"},
            request_checksum_calculation="when_required",
            connect_timeout=5,
            read_timeout=30,
        ),
    )


class Services:
    def __init__(self, root, pg_bin, minio, object_root):
        self.root, self.pg_bin, self.minio, self.object_root = root, pg_bin, minio, object_root
        self.processes, self.logs = [], []
        for executable, expected in ((minio, MINIO_SHA),):
            if hashlib.sha256(executable.read_bytes()).hexdigest() != expected:
                raise AssertionError("Test service executable differs from repository pin")
        self.pg_port, self.s3_port, self.console_port = port(), port(), port()
        self.dsn = f"host=127.0.0.1 port={self.pg_port} user=r6 dbname=postgres sslmode=disable gssencmode=disable"
        self.endpoint = f"http://127.0.0.1:{self.s3_port}"
        self.secret = secrets.token_hex(24)
        self.bucket = "vortex-r6-" + secrets.token_hex(8)
        self.env = dict(os.environ, MINIO_ROOT_USER="r6-admin", MINIO_ROOT_PASSWORD=self.secret)

    def spawn(self, command, name, env=None):
        log = (self.root / name).open("ab")
        self.logs.append(log)
        p = subprocess.Popen([str(x) for x in command], stdout=log, stderr=log, env=env)
        self.processes.append(p)
        return p

    def start_pg(self):
        self.pg = self.spawn(
            [self.pg_bin / "postgres", "-D", self.root / "pg", "-p", self.pg_port, "-h", "127.0.0.1", "-k", self.root],
            "postgres.log",
        )

        def ready():
            with psycopg.connect(self.dsn) as c:
                return c.execute("SELECT 1").fetchone() == (1,)

        wait_ready(ready, self.pg)

    def start_s3(self):
        self.s3 = self.spawn(
            [
                self.minio,
                "server",
                "--address",
                f"127.0.0.1:{self.s3_port}",
                "--console-address",
                f"127.0.0.1:{self.console_port}",
                self.object_root,
            ],
            "minio.log",
            self.env,
        )

        def ready():
            with urllib.request.urlopen(self.endpoint + "/minio/health/ready", timeout=1) as r:
                return r.status == 200

        wait_ready(ready, self.s3)

    def __enter__(self):
        self.root.mkdir(mode=0o700)
        self.object_root.mkdir(mode=0o700)
        try:
            subprocess.run(
                [
                    str(self.pg_bin / "initdb"),
                    "-D",
                    str(self.root / "pg"),
                    "-U",
                    "r6",
                    "--auth=trust",
                    "--no-locale",
                    "--encoding=UTF8",
                ],
                stdout=(self.root / "initdb.log").open("wb"),
                stderr=subprocess.STDOUT,
                check=True,
            )
            self.start_pg()
            self.start_s3()
            self.root_client = client(self.endpoint, "r6-admin", self.secret)
            self.root_client.create_bucket(Bucket=self.bucket)
            self.root_client.put_bucket_versioning(Bucket=self.bucket, VersioningConfiguration={"Status": "Enabled"})
            return self
        except BaseException:
            self.__exit__(None, None, None)
            raise

    def restart(self):
        self.crash_exit_codes = []
        for process in (self.pg, self.s3):
            process.kill()
            self.crash_exit_codes.append(process.wait(timeout=15))
        self.start_pg()
        self.start_s3()

    def __exit__(self, *args):
        for p in reversed(self.processes):
            if p.poll() is None:
                p.terminate()
                try:
                    p.wait(timeout=15)
                except subprocess.TimeoutExpired:
                    p.kill()
                    p.wait(timeout=15)
        for log in self.logs:
            log.close()
        shutil.rmtree(self.object_root)
