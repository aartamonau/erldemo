#/usr/bin/env bash

PORT=
NODE=
BOOTSTRAP_NODE=
COOKIE=
ARGS=(foreground)

usage() {
    cat >&2 <<EOF
Usage: $0
         --port <PORT>
         --node <node name>
         --bootstrap <node name>
EOF
}

while [ $# -gt 0 ]; do
    case "$1" in
        -h|--help)
            usage
            exit 0
            ;;
        -p|--port)
            PORT="$2"
            shift 2
            ;;
        -n|--node)
            NODE="$2"
            shift 2
            ;;
        -b|--bootstrap)
            BOOTSTRAP_NODE="$2"
            shift 2
            ;;
        -c|--cookie)
            COOKIE="$2"
            shift 2
            ;;
        --)
            shift
            ARGS=("$@")
            break
            ;;
        *)
            echo "Unknown flag '$1'" >&2
            usage
            exit 1
    esac
done

if [ -n "$NODE" ]; then
    export NODE
fi

if [ -n "$PORT" ]; then
    export PORT
fi

if [ -n "$BOOTSTRAP_NODE" ]; then
    export BOOTSTRAP_NODE
fi

if [ -n "$COOKIE" ]; then
    export COOKIE
fi

exec _build/default/rel/demo/bin/demo "${ARGS[@]}"
