import subprocess
import os


def run(args):
    subprocess.run(
        args, universal_newlines=True, 
        stderr=subprocess.STDOUT, check=True)


if __name__ == "__main__":
    os.environ["TEST_NEO4J_IPV6_ENABLED"] = "False"

    if os.environ.get("TEST_DRIVER_LITE", False):
        ignore = "--ignore=neo4j-driver"
    else:
        ignore = "--ignore=neo4j-driver-lite"

    run(["npm", "run", "test::integration", "--", ignore])
