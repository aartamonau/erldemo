-module(demo).

%% The APIs.
-export([fibonacci/1, get_stats/0, reset_stats/0]).

%% Export for debugging/interactive use.
-export([node_map/0, pick_node_by_hash/2]).

%% The number of points to assign to each node on the hash ring (see
%% node_map/0 below).
-define(NUM_TOKENS, 1000).

%% Get job statistics.
get_stats() ->
    demo_worker:get_stats().

%% Reset job statistics.
reset_stats() ->
    demo_worker:reset_stats().

%% Compute the N-th Fibonacci number.
%%
%% The job to compute the result will be submitted to one of the currently
%% connected and visible Erlang nodes.
%%
%% The node to submit the job to is decided by a consistent hashing-like
%% scheme: https://en.wikipedia.org/wiki/Consistent_hashing
fibonacci(N) ->
    Node = pick_node({fibonacci, N}),
    demo_worker:fibonacci(Node, N).

%% Pick a node to send a job to.
pick_node(Job) ->
    %% erlang:phash2 knows how to compute a hash for any Erlang term
    Hash = erlang:phash2(Job),
    pick_node_by_hash(Hash, node_map()).

%% Returns a map assigning hash ranges to destination nodes.
%%
%% E.g.:
%%
%% [{18501896,'test2@127.0.0.1'},
%%  {57914448,'test1@127.0.0.1'},
%%  {73255941,'test2@127.0.0.1'},
%%  {98903830,'test1@127.0.0.1'}]
%%
%% This can be read as follows:
%%
%%  - Everything from 0 to 18501896 will go to node 'test2@127.0.0.1'
%%  - Everything from 18501897 to 57914448 will go to 'test1@127.0.0.1'
%%  - From 57914449 to 73255941 will go to 'test2@127.0.0.1' again
%%  - 73255942 to 98903830 to 'test1@127.0.0.1'
%%  - Everything from 98903831 and above to 'test2@127.0.0.1'.
node_map() ->
    Nodes = nodes([this, visible]),
    Map = lists:flatmap(
            fun (Node) ->
                    NodeBinary = atom_to_binary(Node, latin1),
                    [{erlang:phash2({NodeBinary, Token}), Node} || Token <- lists:seq(1, ?NUM_TOKENS)]
            end, Nodes),
    lists:sort(Map).

%% Given a job hash and a node map, find the node to send the job to.
%%
%% E.g.:
%%
%% Map =
%%   [{18501896,'test2@127.0.0.1'},
%%    {57914448,'test1@127.0.0.1'},
%%    {73255941,'test2@127.0.0.1'},
%%    {98903830,'test1@127.0.0.1'}].
%% > demo:pick_node_by_hash(erlang:phash2({fibonacci, 36}), Map).
%% 'test1@127.0.0.1'
%% > demo:pick_node_by_hash(erlang:phash2({fibonacci, 37}), Map).
%% 'test2@127.0.0.1'
pick_node_by_hash(Hash, Map) ->
    case lists:dropwhile(
           fun ({NodeHash, _}) ->
                   Hash > NodeHash
           end, Map) of
        [] ->
            [{_, FirstNode} | _] = Map,
            FirstNode;
        [{_, Node} | _] ->
            Node
    end.
