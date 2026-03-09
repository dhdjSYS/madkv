set working-directory := '..'

# list project 2 just recipes
default:
    @just --list p2 --unsorted
# sudo snap install --edge --classic just
# sudo snap set system homedirs=/users
# install extra dependencies of your KV code
deps:
    sudo apt-get update && sudo apt-get install -y protobuf-compiler snapd
    sudo snap install go --classic
    go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
    go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

# build your executables in release mode
build:
    export PATH=$PATH:$(go env GOPATH)/bin && cd kvstore && protoc --go_out=pb --go_opt=paths=source_relative --go-grpc_out=pb --go-grpc_opt=paths=source_relative -I proto proto/kvstore.proto
    cd kvstore && go build -o ../bin/kvserver ./server
    cd kvstore && go build -o ../bin/kvclient ./client
    cd kvstore && go build -o ../bin/kvmanager ./manager

# clean the build of your executables
clean:
    rm -f bin/kvserver bin/kvclient bin/kvmanager bin/kvserver.exe bin/kvclient.exe bin/kvmanager.exe

# run your KV store manager
manager man_port="3666" servers="127.0.0.1:3777,127.0.0.1:3778,127.0.0.1:3779":
    ./bin/kvmanager --listen "0.0.0.0:{{man_port}}" --servers "{{servers}}"

# run your KV store server
server id="0" manager="127.0.0.1:3666" api_port="3777" backer_path="./backer.s0":
    ./bin/kvserver --listen "0.0.0.0:{{api_port}}" --backer "{{backer_path}}" --id "{{id}}" --manager "{{manager}}"

# run you KV store client in stdin/out interface mode
client manager="127.0.0.1:3666":
    ./bin/kvclient --manager "{{manager}}"

# kill all processes of your KV store system
kill:
    -pkill -f kvserver 2>/dev/null
    -pkill -f kvclient 2>/dev/null
    -pkill -f kvmanager 2>/dev/null

# launch manager + n servers locally (e.g. just p2::cluster 3)
cluster n="1" ip="10.10.1.1" base_port="3777" man_port="3666" backer_prefix="./backer" flush="true":
    #!/usr/bin/env bash
    just p2::build
    # flush backer dirs if requested
    if [ "{{flush}}" = "true" ]; then
        for i in $(seq 0 $(({{n}} - 1))); do
            rm -rf "{{backer_prefix}}.s${i}"
        done
        echo "flushed backer dirs"
    fi
    # build servers list: {{ip}}:base_port, base_port+1, ...
    servers=""
    for i in $(seq 0 $(({{n}} - 1))); do
        port=$(({{base_port}} + i))
        [ -n "$servers" ] && servers="$servers,"
        servers="${servers}{{ip}}:${port}"
    done
    # start manager
    ./bin/kvmanager --listen "0.0.0.0:{{man_port}}" --servers "$servers" &
    sleep 0.3
    # start servers
    for i in $(seq 0 $(({{n}} - 1))); do
        port=$(({{base_port}} + i))
        ./bin/kvserver --listen "0.0.0.0:${port}" --backer "{{backer_prefix}}.s${i}" --id "$i" --manager "{{ip}}:{{man_port}}" &
    done
    echo "cluster up: manager={{ip}}:{{man_port}}, ${{n}} servers ($servers)"
    wait

# NOTE: feel free to add more recipes as you see fit...
#       also feel free to add extra parameters to the recipes as you see fit,
#       but don't change the existing parameters

python_run := if `which uv || true` != "" { "uv run" } else { "python3" }
tmpdir_prefix := "/tmp/madkv-p2"

# launch the KV service components on node (see README for conventions)
service node="s0" manager="127.0.0.1:3666" servers="127.0.0.1:3777,127.0.0.1:3778,127.0.0.1:3779" backer_prefix="./backer":
    just p2::build
    just utils::build
    cargo run -p runner -r --bin service -- \
        --server-just-args p2::server \
            $({{python_run}} justmod/xtract.py partid "{{node}}") \
            "{{manager}}" \
            $({{python_run}} justmod/xtract.py portof "{{servers}}" "{{node}}") \
            "{{backer_prefix}}.{{node}}" \
        --manager-just-args p2::manager \
            $({{python_run}} justmod/xtract.py portof "{{manager}}" "{{node}}") \
            "{{servers}}" \
        --node-id "{{node}}"

# ensure a subdir under 'tmp/' exists
tmpdir subdir:
    mkdir -p "{{tmpdir_prefix}}/{{subdir}}"

# run a fuzz testing scenario
fuzz nservers="1" crashing="no" manager="127.0.0.1:3666": (tmpdir "fuzz")
    just p2::build
    just utils::build
    cargo run -p runner -r --bin fuzzer -- \
        --num-clis 5 \
        --conflict \
        --client-just-args p2::client "{{manager}}" \
        | tee "{{tmpdir_prefix}}/fuzz/fuzz-{{nservers}}-{{crashing}}.log"
    just p2::kill

# run a YCSB benchmark workload
bench nclis wload nservers="1" manager="127.0.0.1:3666": (tmpdir "bench")
    just p2::build
    just utils::build
    just utils::ycsb
    cargo run -p runner -r --bin bencher -- \
        --num-clis "{{nclis}}" \
        --workload "{{wload}}" \
        --client-just-args p2::client "{{manager}}" \
        | tee "{{tmpdir_prefix}}/bench/bench-{{nclis}}-{{wload}}-{{nservers}}.log"
    just p2::kill

# run interactive partitioned cluster failover test (test6)
test6 manager="10.10.1.1:3666":
    ./bin/kvclient --manager "{{manager}}" --test 6

# run all expected tests & benchmarks before report generation
runall addr="10.10.1.1:3666":
    #!/usr/bin/env bash
    set -e
    step=0
    run_step() {
        step=$((step + 1))
        echo ""
        echo "=== Step $step: $1 ==="
        read -r -p "Press Enter to continue..."
        eval "$2"
    }

    # fuzz tests
    run_step "fuzz 3 servers healthy"    "just p2::fuzz 3 no {{addr}}"
    run_step "fuzz 3 servers crashing"   "just p2::fuzz 3 yes {{addr}}"
    run_step "fuzz 5 servers crashing"   "just p2::fuzz 5 yes {{addr}}"

    # ycsb workloads a-f, 1/3/5 partitions, 10 clients
    for wl in a b c d e f; do
        for ns in 1 3 5; do
            run_step "ycsb-$wl ${ns} parts 10 clis" "just p2::bench 10 $wl $ns {{addr}}"
        done
    done

    # ycsb-a scaling: vary client count with 1 and 5 partitions
    for ns in 1 5; do
        for nc in 1 20 30; do
            run_step "ycsb-a ${ns} parts ${nc} clis" "just p2::bench $nc a $ns {{addr}}"
        done
    done

    echo ""
    echo "=== All tests & benchmarks complete ==="

# generate .md report template from existing results (wip)
report:
    {{python_run}} sumgen/proj2.py
