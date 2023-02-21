%% An example process implementing the coordinator behavior. It knows how to
%% compute Fibonacci numbers in an extremely inefficient way. This imitates a
%% heavy computation where we can see the benefit of deduplication.
-module(demo_worker).

-behavior(coordinator).

-export([start_link/0, get_stats/0, reset_stats/0]).
-export([fibonacci/2]).

%% The sole coordinator callback function.
-export([handle_job/1]).

%% The name we'll give to the process. ?MODULE stands for demo_worker.
-define(SERVER_NAME, ?MODULE).

%% The job timeout we'll be using.
-define(TIMEOUT, 30000).

%% Start the process. Refer to coordinator for more details.
start_link() ->
    coordinator:start_link(?SERVER_NAME, ?MODULE).

%% Get the job statistics.
get_stats() ->
    coordinator:get_stats(?SERVER_NAME).

%% Rest the job statistics.
reset_stats() ->
    coordinator:reset_stats(?SERVER_NAME).

%% Submit a job computing the N-th Fibonacci number to the specified node.
fibonacci(Node, N) ->
    coordinator:submit_job(Node, ?SERVER_NAME, {fibonacci, N}, ?TIMEOUT).

%% The callback that coordinator will call into to interpret submitted jobs.
handle_job({fibonacci, N}) ->
    true = N >= 0,
    compute_fibonacci(N).

compute_fibonacci(0) ->
    1;
compute_fibonacci(1) ->
    1;
compute_fibonacci(N) ->
    compute_fibonacci(N - 1) + compute_fibonacci(N - 2).
