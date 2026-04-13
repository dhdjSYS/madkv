set working-directory := '..'

# list project 3 just recipes
default:
    @just --list p3 --unsorted

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

# run your KV store manager replica
manager rep_id="0" \
        man_port="3666" \
        p2p_port="3606" \
        peers="127.0.0.1:3607,127.0.0.1:3608" \
        server_rf="3" \
        servers="127.0.0.1:3777,127.0.0.1:3778,127.0.0.1:3779,127.0.0.1:3780,127.0.0.1:3781,127.0.0.1:3782"\
        backer_path="./backer.m.0":
    ./bin/kvmanager \
        --replica_id "{{rep_id}}" \
        --man_listen "0.0.0.0:{{man_port}}" \
        --p2p_listen "0.0.0.0:{{p2p_port}}" \
        --peer_addrs "{{peers}}" \
        --server_rf "{{server_rf}}" \
        --server_addrs "{{servers}}" \
        --backer_path "{{backer_path}}"

# run your KV store server replica
server part_id="0" \
       rep_id="0" \
       managers="127.0.0.1:3666,127.0.0.1:3667,127.0.0.1:3668" \
       api_port="3777" \
       p2p_port="3707" \
       peers="127.0.0.1:3708,127.0.0.1:3709" \
       backer_path="./backer.s0.0":
    ./bin/kvserver \
        --partition_id "{{part_id}}" \
        --replica_id "{{rep_id}}" \
        --manager_addrs "{{managers}}" \
        --api_listen "0.0.0.0:{{api_port}}" \
        --p2p_listen "0.0.0.0:{{p2p_port}}" \
        --peer_addrs "{{peers}}" \
        --backer_path "{{backer_path}}"

# run you KV store client in stdin/out interface mode
client managers="10.10.1.1:3666":
    ./bin/kvclient --manager_addrs "{{managers}}"

# interactive test: kill <=f followers across >=2 partitions, verify reads/writes stay consistent
test1 managers="10.10.1.1:3666":
    ./bin/kvclient --manager_addrs "{{managers}}" --test 1

# interactive test: kill the leader of a partition, verify transparent election + consistent ops
test2 managers="10.10.1.1:3666":
    ./bin/kvclient --manager_addrs "{{managers}}" --test 2

# interactive test: kill >f servers in one partition, verify requests to it hang (no stale reads)
test3 managers="10.10.1.1:3666":
    ./bin/kvclient --manager_addrs "{{managers}}" --test 3

# kill all processes of your KV store system
kill:
    -pkill -f kvserver 2>/dev/null
    -pkill -f kvclient 2>/dev/null
    -pkill -f kvmanager 2>/dev/null

# launch manager + n partitions x rf servers locally on {{ip}}
# e.g. just p3::cluster 3 3  -> 1 manager + 3 partitions of 3 replicas = 9 servers
cluster n="1" rf="3" ip="10.10.1.1" api_base="3777" p2p_base="3707" man_port="3666" backer_prefix="./backer" flush="true":
    #!/usr/bin/env bash
    set -e
    just p3::build
    total=$(({{n}} * {{rf}}))
    if [ "{{flush}}" = "true" ]; then
        rm -rf "{{backer_prefix}}.m.0"
        for i in $(seq 0 $((total - 1))); do
            p=$((i / {{rf}}))
            r=$((i % {{rf}}))
            rm -rf "{{backer_prefix}}.s${p}.${r}"
        done
        echo "flushed backer dirs"
    fi
    # flat server list in partition-major order: p0r0, p0r1, ..., p1r0, ...
    servers=""
    for i in $(seq 0 $((total - 1))); do
        port=$(({{api_base}} + i))
        [ -n "$servers" ] && servers="$servers,"
        servers="${servers}{{ip}}:${port}"
    done
    # single (non-replicated) manager
    ./bin/kvmanager \
        --replica_id 0 \
        --man_listen "0.0.0.0:{{man_port}}" \
        --p2p_listen "0.0.0.0:3606" \
        --peer_addrs "none" \
        --server_rf "{{rf}}" \
        --server_addrs "$servers" \
        --backer_path "{{backer_prefix}}.m.0" &
    sleep 0.3
    # n partitions x rf replicas
    for p in $(seq 0 $(({{n}} - 1))); do
        for r in $(seq 0 $(({{rf}} - 1))); do
            idx=$((p * {{rf}} + r))
            api_port=$(({{api_base}} + idx))
            p2p_port=$(({{p2p_base}} + idx))
            # peer_addrs: other replicas' p2p addrs in this partition, ordered by replica id, self excluded
            if [ "{{rf}}" -eq 1 ]; then
                peers="none"
            else
                peers=""
                for pr in $(seq 0 $(({{rf}} - 1))); do
                    if [ "$pr" -ne "$r" ]; then
                        peer_port=$(({{p2p_base}} + p * {{rf}} + pr))
                        [ -n "$peers" ] && peers="$peers,"
                        peers="${peers}{{ip}}:${peer_port}"
                    fi
                done
            fi
            echo "launching: ./bin/kvserver --partition_id $p --replica_id $r --manager_addrs {{ip}}:{{man_port}} --api_listen 0.0.0.0:${api_port} --p2p_listen 0.0.0.0:${p2p_port} --peer_addrs $peers --backer_path {{backer_prefix}}.s${p}.${r}"
            ./bin/kvserver \
                --partition_id "$p" \
                --replica_id "$r" \
                --manager_addrs "{{ip}}:{{man_port}}" \
                --api_listen "0.0.0.0:${api_port}" \
                --p2p_listen "0.0.0.0:${p2p_port}" \
                --peer_addrs "$peers" \
                --backer_path "{{backer_prefix}}.s${p}.${r}" &
        done
    done
    echo "cluster up: manager={{ip}}:{{man_port}}, {{n}} partitions x {{rf}} replicas = ${total} servers ($servers)"
    wait

# NOTE: feel free to add more recipes as you see fit...
#       also feel free to add extra parameters to the recipes as you see fit,
#       but don't change the existing parameters

python_run := if `which uv || true` != "" { "uv run" } else { "python3" }
tmpdir_prefix := "/tmp/madkv-p3"

# launch the KV service components on node (see README for conventions)
service node="s0.0" \
        managers="127.0.0.1:3666,127.0.0.1:3667,127.0.0.1:3668" \
        manager_p2ps="127.0.0.1:3606,127.0.0.1:3607,127.0.0.1:3608" \
        server_rf="3" \
        servers="127.0.0.1:3777,127.0.0.1:3778,127.0.0.1:3779,127.0.0.1:3780,127.0.0.1:3781,127.0.0.1:3782" \
        server_p2ps="127.0.0.1:3707,127.0.0.1:3708,127.0.0.1:3709,127.0.0.1:3710,127.0.0.1:3711,127.0.0.1:3712" \
        backer_prefix="./backer":
    just p3::build
    just utils::build
    cargo run -p runner -r --bin service -- \
        --server-just-args p3::server \
            $({{python_run}} justmod/xtract.py partid "{{node}}") \
            $({{python_run}} justmod/xtract.py repid "{{node}}") \
            "{{managers}}" \
            $({{python_run}} justmod/xtract.py portof "{{servers}}" "{{node}}" "{{server_rf}}") \
            $({{python_run}} justmod/xtract.py portof "{{server_p2ps}}" "{{node}}" "{{server_rf}}") \
            $({{python_run}} justmod/xtract.py peersof "{{server_p2ps}}" "{{node}}" "{{server_rf}}") \
            "{{backer_prefix}}.{{node}}" \
        --manager-just-args p3::manager \
            $({{python_run}} justmod/xtract.py repid "{{node}}") \
            $({{python_run}} justmod/xtract.py portof "{{managers}}" "{{node}}") \
            $({{python_run}} justmod/xtract.py portof "{{manager_p2ps}}" "{{node}}") \
            $({{python_run}} justmod/xtract.py peersof "{{manager_p2ps}}" "{{node}}") \
            "{{server_rf}}" \
            "{{servers}}" \
            "{{backer_prefix}}.{{node}}" \
        --node-id "{{node}}"

# ensure a subdir under 'tmp/' exists
tmpdir subdir:
    mkdir -p "{{tmpdir_prefix}}/{{subdir}}"

# run a fuzz testing scenario
fuzz server_rf="5" crashing="no" \
     managers="127.0.0.1:3666,127.0.0.1:3667,127.0.0.1:3668": (tmpdir "fuzz")
    just p3::build
    just utils::build
    cargo run -p runner -r --bin fuzzer -- \
        --num-clis 5 \
        --conflict \
        --client-just-args p3::client "{{managers}}" \
        | tee "{{tmpdir_prefix}}/fuzz/fuzz-{{server_rf}}-{{crashing}}.log"
    just p3::kill

# run a YCSB benchmark workload
bench nclis wload server_rf="3" \
      managers="127.0.0.1:3666,127.0.0.1:3667,127.0.0.1:3668": (tmpdir "bench")
    just p3::build
    just utils::build
    just utils::ycsb
    cargo run -p runner -r --bin bencher -- \
        --num-clis "{{nclis}}" \
        --workload "{{wload}}" \
        --client-just-args p3::client "{{managers}}" \
        | tee "{{tmpdir_prefix}}/bench/bench-{{nclis}}-{{wload}}-{{server_rf}}.log"
    # just p3::kill

# generate .md report template from existing results (wip)
report:
    {{python_run}} sumgen/proj3.py
