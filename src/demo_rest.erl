-module(demo_rest).

-export([start/1, stop/0]).
-export([allowed_methods/2, init/2]).

start(Port) ->
    Dispatch =
        cowboy_router:compile(
          [
           {'_', [
                  {"/fibonacci/:n", ?MODULE, #{handler => fun handle_fibonacci/1,
                                               methods => [<<"GET">>]}},
                  {"/stats", ?MODULE, #{handler => fun handle_stats/1,
                                        methods => [<<"DELETE">>, <<"GET">>]}},
                  {"/map", ?MODULE, #{handler => fun handle_map/1,
                                      methods => [<<"GET">>]}}
                 ]}
          ]),
    Options = #{env => #{dispatch => Dispatch}},
    cowboy:start_clear(http, [{port, Port}], Options).

stop() ->
    ok = cowboy:stop_listener(http).

init(Req, Opts) ->
    Handler = maps:get(handler, Opts),
    {ok, Handler(Req), Opts}.

allowed_methods(_Req, Opts) ->
    maps:get(methods, Opts).

handle_fibonacci(Req) ->
    N = cowboy_req:binding(n, Req),
    try
        binary_to_integer(N)
    of N1 ->
            reply_ok(#{n => N1,
                       result => demo:fibonacci(N1)}, Req)
    catch
        error:badarg ->
            reply_error(<<"invalid parameter">>, Req)
    end.

handle_stats(Req) ->
    case cowboy_req:method(Req) of
        <<"DELETE">> ->
            case demo:reset_stats() of
                ok ->
                    reply_ok(<<"ok">>, Req);
                {error, {failed_nodes, Nodes}} ->
                    Message = io_lib:format("Could not reset stats on nodes ~p", [Nodes]),
                    reply_error(Message, Req)
            end;
        <<"GET">> ->
            reply_ok(demo:get_stats(), Req)
    end.

handle_map(Req) ->
    Map = [#{hash => Hash,
             node => atom_to_binary(Node, latin1)} || {Hash, Node} <- demo:node_map()],
    reply_ok(Map, Req).

headers() ->
    #{<<"content-type">> => <<"application/json">>}.

reply_json(Code, Value, Req) ->
    cowboy_req:reply(Code, headers(), jiffy:encode(Value), Req).

reply_ok(Value, Req) ->
    reply_json(200, Value, Req).

reply_error(Error, Req) ->
    reply_json(400, iolist_to_binary(Error), Req).
