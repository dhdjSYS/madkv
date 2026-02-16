set working-directory := '..'

# list project 1 just recipes
default:
    @just --list p1 --unsorted

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

# clean the build of your executables
clean:
    rm -f bin/kvserver bin/kvclient bin/kvserver.exe bin/kvclient.exe

# run your KV store server
server listen="0.0.0.0:3777":
    ./bin/kvserver --listen "{{listen}}"

# run you KV store client in stdin/out interface mode
client server="127.0.0.1:3777":
    ./bin/kvclient --server "{{server}}"

# run your KV store testcase 1 client
test1 server="127.0.0.1:3777":
    ./bin/kvclient --server "{{server}}" --test 1

# run your KV store testcase 2 client
test2 server="127.0.0.1:3777":
    ./bin/kvclient --server "{{server}}" --test 2

# run your KV store testcase 3 client
test3 server="127.0.0.1:3777":
    ./bin/kvclient --server "{{server}}" --test 3 --clientid 0 &
    ./bin/kvclient --server "{{server}}" --test 3 --clientid 1 &
    ./bin/kvclient --server "{{server}}" --test 3 --clientid 2 &
    wait

# run your KV store testcase 4 client(s) - concurrent relay
test4 server="127.0.0.1:3777":
    ./bin/kvclient --server "{{server}}" --test 4 --clientid 0 &
    ./bin/kvclient --server "{{server}}" --test 4 --clientid 1 &
    ./bin/kvclient --server "{{server}}" --test 4 --clientid 2 &
    wait

# run your KV store testcase 5 client(s) - concurrent contention
test5 server="127.0.0.1:3777":
    ./bin/kvclient --server "{{server}}" --test 5 --clientid 0 &
    ./bin/kvclient --server "{{server}}" --test 5 --clientid 1 &
    ./bin/kvclient --server "{{server}}" --test 5 --clientid 2 &
    wait

# kill all processes of your KV store system
kill:
    -pkill -f kvserver 2>/dev/null
    -pkill -f kvclient 2>/dev/null

# NOTE: feel free to add more recipes as you see fit...
#       also feel free to add extra parameters to the recipes as you see fit,
#       but don't change the existing parameters

python_run := if `which uv || true` != "" { "uv run" } else { "python3" }
tmpdir_prefix := "/tmp/madkv-p1"

# launch a long-running KV store service
service listen="0.0.0.0:3777":
    just p1::build
    just utils::build
    cargo run -p runner -r --bin service -- \
        --server-just-args p1::server "{{listen}}" \
        --manager-just-args none

# ensure a subdir under 'tmp/' exists
tmpdir subdir:
    mkdir -p "{{tmpdir_prefix}}/{{subdir}}"

# run a student-provided testcase
testcase num server="127.0.0.1:3777": (tmpdir "tests")
    just p1::build
    just "p1::test{{num}}" "{{server}}" \
        | tee "{{tmpdir_prefix}}/tests/test{{num}}.log"
    just p1::kill

# run a fuzz testing scenario
fuzz nclis conflict="no" server="127.0.0.1:3777": (tmpdir "fuzz")
    just p1::build
    just utils::build
    cargo run -p runner -r --bin fuzzer -- \
        --num-clis "{{nclis}}" \
        {{ if conflict == "no" { "" } else { "--conflict" } }} \
        --client-just-args p1::client "{{server}}" \
        | tee "{{tmpdir_prefix}}/fuzz/fuzz-{{nclis}}-{{conflict}}.log"
    just p1::kill

# run a YCSB benchmark workload
bench nclis wload server="127.0.0.1:3777": (tmpdir "bench")
    just p1::build
    just utils::build
    just utils::ycsb
    cargo run -p runner -r --bin bencher -- \
        --num-clis "{{nclis}}" \
        --workload "{{wload}}" \
        --client-just-args p1::client "{{server}}" \
        | tee "/tmp/madkv-p1/bench/bench-{{nclis}}-{{wload}}.log"
    just p1::kill

# generate .md report template from existing results (wip)
report:
    {{python_run}} sumgen/proj1.py
