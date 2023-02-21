demo
=====

This is an small Erlang application demostrating some of the features of
Erlang/OTP.

How to read the code
-----

I commented the core modules heavily. So hopefully they can be read by those
having no prior exposure to Erlang whatsoever.

The recommended order is below:

 1. `src/coordinator.erl`

    Implements the coordinator behavior that accepts abstract CPU-intensive
    jobs, deduplicates them when possible, computes job results limiting
    maximum parallelism.

 2. `src/demo_worker.erl`

    Instantiates the coordinator behavior telling it how to compute Fibonacci
    numbers (in a way that is deliberately very slow).

    An instance of this process will be running on each Erlang node.

 3. `src/demo.erl`

    A wrapper module that distributes jobs to `demo_worker` processes an
    connected nodes using consistent hashing.

 4. `src/demo_rest.erl`

     Optional (as I didn't leave any comments here). This module exposes the
     APIs exposed by `demo` via HTTP.

Build
-----

    $ rebar3 compile
    $ rebar3 release

Run
-----

# First node
    $ ./start.sh --node node1@127.0.0.1 --cookie secret --port 8080

# Second node
    $ ./start.sh --node node2@127.0.0.1 --cookie secret --bootstrap node1@127.0.0.1 --port 8081

APIs
-----

# Get statistics

```sh
$ curl -s -XGET http://127.0.0.1:8080/stats | jq .
{
  "node1@127.0.0.1": {
    "ok": 129,
    "failed": 0,
    "computed_ok": 88,
    "computed_failed": 0,
    "accepted": 135
  }
}
```

# Reset statistics
```sh
$ curl -s -XDELETE http://127.0.0.1:8080/stats | jq .
"ok"
```

# Compute the N-th Fibonacci number
```sh
$ curl -s -XGET http://127.0.0.1:8080/fibonacci/42 | jq .
{
  "result": 433494437,
  "n": 42
}
```
