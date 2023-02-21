%% This module defines a coordinator behavior. Behaviors in Erlang is a way to
%% express common code patterns. Each behavior defines a set of callback
%% functions that must be implemented to instantiate the behavior. The
%% behavior itself implements the parts that are common for all uses, while
%% the callback module implements the parts that are specific to a particular
%% use.
%%
%% See https://www.erlang.org/doc/design_principles/des_princ.html#behaviours
%% for more information.
%%
%% The coordinator behavior:
%%
%%  1. Accepts abstract CPU-intensive jobs from clients.
%%  2. Calls into the callback module to compute the result of the job.
%%  3. Multiple jobs can be computed at the same time up to a certain limit.
%%  4. If multiple clients request the same job to be computed, the result will be
%%     computed once and returned to all the requesting clients. So jobs are
%%     deduplicated when possible.
-module(coordinator).

%% The implementation itself uses the standard gen_server behavior. This
%% behavior provides an easy way to implement a stateful server process that
%% clients can interact with via synchronous and asynchronous requests.
%%
%% gen_server is probably the most commonly used Erlang behavior.
%%
%% More info: https://www.erlang.org/doc/man/gen_server.html
-behavior(gen_server).

%% These are going to be the APIs exposed by our behavior.
-export([start_link/2]).
-export([submit_job/4, get_stats/1, reset_stats/1]).

%% All of these functions are gen_server callbacks. They need to be exported
%% to implement the gen_server behavior. Otherwise they are internal to this
%% module.
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-include_lib("kernel/include/logger.hrl").

%% It's the callback module that defines what jobs are handled by it. So for
%% the purposes of this module, jobs can be any valid Erlang term.
-type job() :: any().

%% In order to implement the coordinator behavior, the callback module will
%% need to provide a definition for this function.
%%
%% The coordinator module will call it when a job is submitted. The value
%% returned by the callback will be returned to the client(s).
-callback handle_job(Job :: job()) -> any().

%% The coordinator will keep track of various statistics.
-type stats() ::
        #{ %% The number of jobs that have been accepted. Some of these may
           %% still be computing.
           accepted := non_neg_integer(),

           %% The total number of jobs that computed successfully.
           ok := non_neg_integer(),

           %% The total number of jobs that failed due to an exception.
           failed := non_neg_integer(),

           %% The total number of deduplicated jobs that computed
           %% successfully.
           computed_ok := non_neg_integer(),

           %% The total number of deduplicated jobs that failed.
           computed_failed := non_neg_integer() }.

%% The state that the coordinator process maintains.
-record(state,
        { %% The name of the callback module, so we can call its handle_job function.
          module :: module(),

          %% Every job will be computed in its own process. This is a mapping
          %% from the worker process id to the job its computing.
          workers :: #{pid() => job()},

          %% A queue of pending jobs. These are the jobs that are waiting for
          %% their turn to be computed.
          queue :: queue:queue(job()),

          %% A map of clients waiting for each job. So given a job, we can
          %% find all the clients waiting for its result.
          waiters :: #{job() => [gen_server:from()]},

          %% A maximum number of jobs that can be computed in parallel.
          max_workers :: pos_integer(),

          %% Job statistics.
          stats :: stats()
        }).

%% This function will start a coordinator process named Name that uses Module
%% as the callback module.
%%
%% Processes in Erlang VM can be anonymous or registered. Anonymous processes
%% can only be addressed via the process identifier returned by the function
%% that started the process. Registered processes can be addressed by a
%% well-known name.
%%
%% It's typical for long-lived processes to be given names. Then any requests
%% can be routed to the process using the name.
%%
%% The standard gen_server behavior allows starting both named and anonymous
%% processes. For simplicity, we require that coordinator processes are always
%% named.
%%
%% LINKS
%%
%% The _link suffix in the function name is a common convention indicating
%% that the function starts a process and atomically links it to the caller
%% process.
%%
%% Linking two processes means that if either of the processes dies, the other
%% side will get notified. So if our coordinator process dies for any reason,
%% the parent process will get notified. By default exit signals (signals
%% produced by links) kill the receiving process as well. But this behavior
%% can be modified by setting the 'trap_exit' process flag.
-spec start_link(atom(), module()) -> gen_server:start_ret().
start_link(Name, Module) ->
    %% Start a gen_server process
    gen_server:start_link(
      %% Register it under name Name locally
      {local, Name},
      %% Use the callbacks defined in this module (i.e. in 'coordinator').
      ?MODULE,
      %% This is a set of arguments that will be passed to the
      %% coordinator:init/1 callback. In this case, it's the name of the
      %% callback module implementing the coordinator behavior.
      [Module],
      %% gen_server flags that we don't use here
      []).

%% Submit a job to a coordinator process given its name and the node it's
%% running on. Block until the job produces a result. Abandon the job after
%% the given timeout.
%%
%% DISTRIBUTED ERLANG
%%
%% Multiple instances of Erlang runtime can be connected to each other to form
%% a cluster. Each individual runtime is referred to as a node. Each node is
%% identified by a name that is given to it when it's started. Any node in the
%% cluster can communicate to all other nodes transparently.
%%
%% submit_job/4 allows submitting a job to a named coordinator running on any
%% node in the Erlang cluster.
%%
%% TIMEOUTS
%%
%% Timeouts in erlang are typically specified in milliseconds. A special atom
%% 'infinity' can be used to say "no timeout".
%%
%% Note that this is a client-side timeout. Meaning that the server process
%% does not know anything about these timeouts. Even if a call times out, the
%% server process will finish handling the call.
-spec submit_job(node(), atom(), job(), timeout()) -> any().
submit_job(Node, Name, Job, Timeout) ->
    %% gen_server:call/3 makes a synchronous request to the server process
    case gen_server:call(
           %% This is what's known as a server reference. It can be a pid, a
           %% name, and more. In this case, we are saying to call a server
           %% process with the given name running on a (possibly) different
           %% erlang node.
           {Name, Node},
           %% This is the request that will be passed as is to coordinator:handle_call/3.
           {submit_job, Job},
           Timeout) of

        %% The shape of the return value is defined by handle_call/3
        %% callback. In our case, we either have computed the result
        %% successfully, or failed because handle_job/1 raised an exception.
        %%
        %% gen_server:call/3 itself will raise an exception after the timeout
        %% expires and in some other exceptional situations.
        {ok, Result} ->
            Result;
        {raised, Class, Reason, Stacktrace} ->
            %% We are reraising the caught exception in the context of the
            %% calling process.
            erlang:raise(Class, Reason, Stacktrace)
    end.

%% Collect job statistics from all currently connected erlang nodes.
-spec get_stats(atom()) -> #{node() => stats()}.
get_stats(Name) ->
    %% This returns a list of currently connected (visible) erlang nodes
    %% including the current node itself.
    %%
    %% Nodes can be marked hidden in which case they will be excluded from the
    %% result (the 'visible' flag). A typical use for hidden nodes is remote
    %% shells.
    Nodes = nodes([this, visible]),

    %% gen_server:multi_call/3 will submit the given synchronous request to a
    %% named process on all specified nodes. It returns a tuple of successful
    %% responses (first element) and any nodes that failed (second element).
    %%
    %% For simplicity, we are ignoring the errors here.
    {Results, _} = gen_server:multi_call(Nodes, Name, get_stats),

    %% The results returned by gen_server:multi_call/3 are a property list
    %% which, roughly speaking, is simply a list of tuples of arity two. The
    %% second element of each tuple is the result, the first element is the
    %% node that returned this result.
    %%
    %% We are converting the result to a map, which is a more efficient way to
    %% represent the same information.
    %%
    %% E.g.
    %%
    %% #{'test1@127.0.0.1' =>
    %%       #{accepted => 118,computed_failed => 0,computed_ok => 82,
    %%         failed => 0,ok => 114},
    %%   'test2@127.0.0.1' =>
    %%       #{accepted => 94,computed_failed => 0,computed_ok => 63,
    %%         failed => 0,ok => 90}}
    maps:from_list(Results).

%% Reset job statistics on all connected nodes.
-spec reset_stats(atom()) -> ok | {error, {failed_nodes, [node()]}}.
reset_stats(Name) ->
    Nodes = nodes([this, visible]),
    {_, BadNodes} = gen_server:multi_call(Nodes, Name, reset_stats),
    case BadNodes of
        [] ->
            ok;
        _ ->
            {error, {failed_nodes, BadNodes}}
    end.

%% This is a gen_server callback that is called by
%% gen_server:start_link/4. init/1 is meant to construct the initial server
%% state. As long as it returns {ok, _}, gen_server:start_link/4 will succeed.
init([Module]) ->
    %% As mentioned, by default exit signals from linked processes
    %% terminate the other end of the link.
    %%
    %% Setting the 'trap_exit' process flag to true changes this
    %% behavior. Now, any exit signals are delivered as regular messages of
    %% shape {'EXIT', Pid, Reason}. Where Pid identifies the process that
    %% generated the signal and Reason is an arbitrary term that is the
    %% "termination reason" of the linked process.
    %%
    %% The worker processes are spawned linked to the coordinator
    %% process. Trapping exits means that the coordinator process will be able
    %% to handle dying worker processes without dying itself. The worker
    %% processes do not trap exits, which means that they will be terminated
    %% in the event the coordinator process terminates.
    process_flag(trap_exit, true),

    %% This is our initial state.
    State = #state{ module = Module,             % remember the callback module
                    workers = #{},               % no workers present
                    queue = queue:new(),         % no jobs
                    %% Set max parallelism to the number of runtime schedulers
                    %% (by default it's equal to the number of the logical CPUs
                    %% in the system).
                    max_workers = erlang:system_info(schedulers),
                    waiters = #{},               % no clients waiting
                    stats = new_stats()          % empty statistics
                  },

    %% Indicate to gen_server that we are good to go. This is the point where
    %% gen_server:start_link/4 replies to its caller.
    {ok, State}.

%% handle_call/3 is the callback called by gen_server to handle requests
%% submitted via gen_server:call/3 and gen_server:multi_call/4.
%%
%% The three arguments to handle_call are:
%%
%%  1. The request.
%%  2. An opaque From tag. This can be used to reply to the client using gen_server:reply/2
%%     function.
%%  3. The current server state.
handle_call({submit_job, Job}, From, State) ->
    handle_submit_job(Job, From, State);
handle_call(get_stats, _From, #state{stats = Stats} = State) ->
    %% get_stats request is very simple. We just need to return the statistics
    %% from the server state.
    %%
    %% {reply, Reply, NewState} is the simplest return value that
    %% handle_call/3 can return. It simply tells gen_server to reply to the
    %% client immediately (using the value in Reply). NewState will become the
    %% new server state.
    %%
    %% In our case, we are replying with the statistics and leaving the server
    %% state unchanged.
    {reply, Stats, State};
handle_call(reset_stats, _From, #state{stats = Stats} = State) ->
    %% Resetting statistics is a bit more complicated. We can't simply drop
    %% all statistics to zero. All pending jobs must still count as accepted.
    #{accepted := Accepted, ok := Ok, failed := Failed} = Stats,
    Completed = Ok + Failed,
    PendingJobs = Accepted - Completed,

    %% The number of pending jobs must not be negative. This line asserts that
    %% this is the case. If due to a bug the assertion is violated, the
    %% process will terminate and then get restarted by the parent supervisor.
    true = PendingJobs >= 0,

    NewStats = inc_stat(accepted, PendingJobs, new_stats()),
    {reply, ok, State#state{stats = NewStats}};
handle_call(Call, From, State) ->
    ?LOG_INFO("Unexpected call ~p from ~p", [Call, From]),
    {reply, nack, State}.

%% handle_cast/2 is meant for asynchronous requests to a gen_server
%% process. We don't use it, but the callback is required and so cannot be
%% omitted.
handle_cast(Cast, State) ->
    ?LOG_INFO("Unexpected cast ~p", [Cast]),
    {noreply, State}.

%% handle_info/2 is the gen_server callback that is called on every message
%% that the server receives (with the exception of the requests handled by
%% handle_call/3 and handle_cast/2).
handle_info({'EXIT', Pid, Reason}, State) ->
    %% One of the worker processes terminated. Handle the result it produced.
    handle_worker_exit(Pid, Reason, State);
handle_info(Msg, State) ->
    ?LOG_INFO("Unexpected message ~p", [Msg]),
    {noreply, State}.

%% gen_server callback that is called when the server process terminates. Can
%% be used for cleanup.
terminate(_Reason, _State) ->
    ok.

%% Handle a newly submitted job.
handle_submit_job(Job, From,
                  #state{waiters = Waiters,
                         queue = Queue,
                         stats = Stats} = State) ->
    %% Are there any clients already waiting for this job?
    JobWaiters = maps:get(Job, Waiters, []),

    %% Add the new client to the list.
    NewJobWaiters = [From | JobWaiters],

    %% Put the updated list back to the client map.
    NewWaiters = Waiters#{Job => NewJobWaiters},

    %% Should we add the job to the queue?
    NewQueue =
        case JobWaiters of
            [] ->
                %% This is the first client for this job. So it's a new job,
                %% and we should add it to the queue.
                queue:in(Job, Queue);
            _ ->
                %% The job is either already in the queue, or possibly is
                %% already being computed. No changes required.
                Queue
        end,

    %% Save the updated values in the state, update the statistics.
    NewState = State#state{waiters = NewWaiters,
                           queue = NewQueue,
                           stats = inc_stat(accepted, Stats)},

    %% Note, that unlike the handler for get_stats and reset_stats, this one
    %% does not return {reply, _, _}. So no reply will produced by gen_server
    %% until a later point when gen_server:reply(From, _) is called.
    {noreply,
     %% We've possibly added new jobs to the queue, so we should check if any
     %% workers can be started.
     maybe_start_worker(NewState)}.

%% Check and start a worker process for the next pending job if possible.
maybe_start_worker(#state{workers = Workers,
                          max_workers = MaxWorkers,
                          queue = Queue} = State) ->
    %% How many active workers do we currently have?
    NumWorkers = maps:size(Workers),

    case NumWorkers >= MaxWorkers orelse queue:is_empty(Queue) of
        true ->
            %% If we are above the limit or there are no waiting jobs, do
            %% nothing.
            State;
        false ->
            %% Otherwise, take the next job.
            {{value, Job}, NewQueue} = queue:out(Queue),
            NewState = State#state{queue = NewQueue},

            %% And start a new worker for this job.
            start_worker(Job, NewState)
    end.

%% Start a worker process for a job.
start_worker(Job, #state{module = Module,
                         workers = Workers} = State) ->
    %% spawn_link/1 will start a new process and return its pid. The process
    %% will be linked to the parent.
    Pid = spawn_link(
            %% The function passed to spawn_link/1 will be evaluated in the
            %% spawned process.
            fun () ->
                    Result =
                        %% We're simply calling the callback function to
                        %% compute the result catching any exceptions.
                        try Module:handle_job(Job)
                        of R -> {ok, R}
                        catch
                            Class:Reason:Stacktrace ->
                                {raised, Class, Reason, Stacktrace}
                        end,

                    %% exit/1 terminates the current process. It accepts any
                    %% Erlang term as the termination reason. We are using it
                    %% as a lazy way to communicate the result to the parent.
                    exit(Result)
            end),

    %% Remember the worker and which job it corresponds to. So we know which
    %% clients to respond to later.
    NewWorkers = Workers#{Pid => Job},
    State#state{workers = NewWorkers}.

%% Handle an exit signal from a worker.
handle_worker_exit(Pid, Result, #state{workers = Workers,
                                       waiters = Waiters,
                                       stats = Stats} = State) ->
    %% Find the worker by the pid. The pid must be in the worker map.
    {Job, NewWorkers} = maps:take(Pid, Workers),

    %% Find the clients waiting for the job.
    {JobWaiters, NewWaiters} = maps:take(Job, Waiters),

    %% The number of waiting clients corresponds to the number of
    %% non-deduplicated jobs that we knocked off.
    NumJobs = length(JobWaiters),

    %% Update the stats depending on whether we successfully computed the
    %% result or not.
    NewStats =
        case Result of
            {ok, _} ->
                inc_stat(
                  computed_ok,
                  inc_stat(ok, NumJobs, Stats));
            {raised, _, _, _} ->
                inc_stat(
                  computed_failed,
                  inc_stat(failed, NumJobs, Stats))
        end,

    %% Remove the worker and clients from the state.
    NewState = State#state{workers = NewWorkers, waiters = NewWaiters, stats = NewStats},

    %% Reply to all the waiting clients.
    lists:foreach(
      fun (Waiter) ->
              gen_server:reply(Waiter, Result)
      end, JobWaiters),

    {noreply,
     %% Now that a worker terminated, we might be able to start another one.
     maybe_start_worker(NewState)}.

-spec new_stats() -> stats().
new_stats() ->
    #{accepted => 0,
      ok => 0,
      failed => 0,
      computed_ok => 0,
      computed_failed => 0}.

-spec inc_stat(atom(), stats()) -> stats().
inc_stat(Name, Stats) ->
    inc_stat(Name, 1, Stats).

-spec inc_stat(atom(), non_neg_integer(), stats()) -> stats().
inc_stat(Name, By, Stats) ->
    true = By >= 0,

    maps:update_with(
      Name,
      fun (Value) ->
              By + Value
      end, Stats).
