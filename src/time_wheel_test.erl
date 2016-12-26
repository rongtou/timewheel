%% @author rong
%% @doc 
-module(time_wheel_test).

-behaviour(gen_server).

-export([start_link/0, handle/2, add_timer/2, del_timer/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {tw}).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

add_timer(Msg, Time) ->
    gen_server:call(?MODULE, {add_timer, Msg, Time}).

del_timer(TimerRef) ->
    gen_server:call(?MODULE, {del_timer, TimerRef}).

init([]) ->
    {ok, TW} = time_wheel:init(),
    {ok, TimerRef, TW1} = time_wheel:add_timer({?MODULE, loop_msg}, 100, TW),
    io:format("add timer ~w~n", [TimerRef]),
    erlang:send_after(10, self(), loop),
    {ok, #state{tw = TW1}}.

handle(loop_msg = Msg, TW) ->
    io:format("time out: ~w ~w~n", [Msg, os:timestamp()]),
    {ok, _TimerRef, TW1} = time_wheel:add_timer({?MODULE, Msg}, 100, TW),
    TW1;
handle(Msg, TW) ->
    io:format("time out: ~w ~w~n", [Msg, os:timestamp()]),
    TW.

handle_call({add_timer, Msg, Time}, _From, State) ->
    #state{tw = TW} = State,
    {ok, TimerRef, TW1} = time_wheel:add_timer({?MODULE, Msg}, Time, TW),
    {reply, TimerRef, State#state{tw = TW1}};

handle_call({del_timer, TimerRef}, _From, State) ->
    #state{tw = TW} = State,
    TW1 = time_wheel:del_timer(TimerRef, TW),
    {reply, ok, State#state{tw = TW1}}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(loop, State) ->
    #state{tw = TW} = State,
    erlang:send_after(10, self(), loop),
    TW1 = time_wheel:update(TW),
    {noreply, State#state{tw = TW1}}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
