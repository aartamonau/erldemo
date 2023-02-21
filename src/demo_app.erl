%%%-------------------------------------------------------------------
%% @doc demo public API
%% @end
%%%-------------------------------------------------------------------

-module(demo_app).

-behaviour(application).

-include_lib("kernel/include/logger.hrl").

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    {ok, Port} = application:get_env(demo, port),
    ?LOG_INFO("Starting web server on port ~p", [Port]),
    {ok, _} = demo_rest:start(Port),
    ?LOG_INFO("Web server started on port ~p", [Port]),

    {ok, Node} = application:get_env(bootstrap_node),
    case Node of
        undefined ->
            ok;
        _ when is_atom(Node) ->
            ?LOG_INFO("Connecting to node ~p", [Node]),
            case net_adm:ping(Node) of
                pong ->
                    ?LOG_INFO("Connected to ~p successfully", [Node]);
                pang ->
                    ?LOG_ERROR("Failed to connect to bootstrap node ~p", [Node]),
                    init:stop(1),
                    init:get_status()
            end
    end,

    demo_sup:start_link().

stop(_State) ->
    demo_rest:stop().

%% internal functions
