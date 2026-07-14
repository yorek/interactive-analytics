#!/usr/bin/env bash
set -euo pipefail

# setup test

./iwtpch.sh setup --scale 1
./iwtpch.sh setup --scale 10
./iwtpch.sh setup --scale 100
./iwtpch.sh setup --scale 1000

# run tests

./iwtpch.sh run --target standard --workload original --scale 1 --repeat 5
./iwtpch.sh run --target interactive --workload original --scale 1 --repeat 5

./iwtpch.sh run --target standard --workload original --scale 10 --repeat 5
./iwtpch.sh run --target interactive --workload original --scale 10 --repeat 5

./iwtpch.sh run --target standard --workload original --scale 100 --repeat 5
./iwtpch.sh run --target interactive --workload original --scale 100 --repeat 5

./iwtpch.sh run --target standard --workload original --scale 1000 --repeat 5
./iwtpch.sh run --target interactive --workload original --scale 1000 --repeat 5

# tear down tests

./iwtpch.sh teardown --scale 1
./iwtpch.sh teardown --scale 10
./iwtpch.sh teardown --scale 100
./iwtpch.sh teardown --scale 1000


