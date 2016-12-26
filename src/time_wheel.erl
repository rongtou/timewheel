%% @author rong
%% @doc 
-module(time_wheel).

-export([init/0, update/1, add_timer/3, del_timer/2]).

-define(TIME_NEAR_SHIFT, 8).
-define(TIME_NEAR, (1 bsl ?TIME_NEAR_SHIFT)).
-define(TIME_LEVEL_SHIFT, 6).
-define(TIME_LEVEL, (1 bsl ?TIME_LEVEL_SHIFT)).
-define(TIME_NEAR_MASK, (?TIME_NEAR-1)).
-define(TIME_LEVEL_MASK, (?TIME_LEVEL-1)).

-record(time_event, {ref, expire, msg}).

-record(time_wheel, {tick = 0, cp = 0, near, levels}).

init() ->
    Near = lists:foldl(fun(Idx, Maps) ->
        maps:put(Idx, [], Maps) 
    end, maps:new(), lists:seq(0, ?TIME_NEAR-1)),

    Levels = lists:foldl(fun(Level, Maps) ->

        LvMaps = lists:foldl(fun(Idx, Maps2) ->
            maps:put(Idx, [], Maps2) 
        end, maps:new(), lists:seq(0, ?TIME_LEVEL-1)),

        maps:put(Level, LvMaps, Maps) 
    end, maps:new(), lists:seq(0, 3)),

    {ok, #time_wheel{cp = get_time(), near = Near, levels = Levels}}.

update(TW) ->
    CP = get_time(),
    if 
        CP < TW#time_wheel.cp ->
            % 系统时间发生了倒退
            TW#time_wheel{cp = CP};
        CP > TW#time_wheel.cp ->
            Diff = CP - TW#time_wheel.cp,
            TW1 = TW#time_wheel{cp = CP},
            lists:foldl(fun(_, TW2) ->
                timer_update(TW2)
            end, TW1, lists:seq(1, Diff));
        true ->
            io:format("err time dd ~w: ~w ~n", [CP, TW#time_wheel.cp]),
            TW
    end.

% Time 别大于2^32
add_timer(Msg, Time, TW) ->
    FixTime = if 
        Time =< 0 -> Time;
        true -> Time
    end,
    #time_wheel{tick = Tick} = TW,
    Ref = erlang:make_ref(),
    Expire = Tick + FixTime,
    Event = #time_event{ref = Ref, msg = Msg, expire = Expire},
    TW1 = add_timer_event(Event, TW),
    {ok, {Ref, Expire}, TW1}.

add_timer_event(Event, TW) ->
    #time_event{expire = Expire} = Event,
    update_time_event(fun(EventList) -> [Event|EventList] end, Expire, TW).

del_timer({Ref, Expire}, TW) ->
    update_time_event(fun(EventList) -> 
        lists:keydelete(Ref, #time_event.ref, EventList) 
    end, Expire, TW).

update_time_event(Fun, Expire, TW) ->
    #time_wheel{tick = Tick, near = Near, levels = Levels} = TW,
    case (Expire bor ?TIME_NEAR_MASK) == (Tick bor ?TIME_NEAR_MASK) of
        true ->
            Idx = (Expire band ?TIME_NEAR_MASK),
            EventList = maps:get(Idx, Near),
            Near1 = maps:put(Idx, Fun(EventList), Near),
            TW#time_wheel{near = Near1};
        false ->
            Mask = ?TIME_NEAR bsl ?TIME_LEVEL_SHIFT,
            Level = calc_level(Expire, Tick, Mask, 0),
            LevelMap = maps:get(Level, Levels),
            Idx = (Expire bsr (?TIME_NEAR_SHIFT + Level*?TIME_LEVEL_SHIFT)) band ?TIME_LEVEL_MASK,
            EventList = maps:get(Idx, LevelMap),
            LevelMap1 = maps:put(Idx, Fun(EventList), LevelMap),
            Levels1 = maps:put(Level, LevelMap1, Levels),
            TW#time_wheel{levels = Levels1}
    end.

calc_level(_, _, _, 3) -> 3;
calc_level(Expire, Tick, Mask, Level) ->
    case (Expire bor (Mask-1)) == (Tick bor (Mask-1)) of
        true ->
            Level;
        false ->
            calc_level(Expire, Tick, Mask bsl ?TIME_LEVEL_SHIFT, Level+1)
    end.

timer_update(TW) ->
    TW1 = timer_execute(TW),
    TW2 = timer_shift(TW1),
    timer_execute(TW2).

timer_shift(TW) ->
    #time_wheel{tick = Tick} = TW,
    NewTick = rewind(Tick + 1),
    TW1 = TW#time_wheel{tick = NewTick},
    if
        NewTick == 0 ->
            cascade(3, 0, TW);
        true ->
            Time = NewTick bsr ?TIME_NEAR_SHIFT,
            timer_shift_1(Time, 0, ?TIME_NEAR, TW1)
    end.

timer_shift_1(Time, Level, Mask, TW) ->
    #time_wheel{tick = Tick} = TW,
    case Tick band (Mask-1) of
        0 ->
            Idx = Time band ?TIME_LEVEL_MASK,
            if  Idx /= 0 ->
                    cascade(Level, Idx, TW); 
                true ->
                    timer_shift_1(Time bsr ?TIME_LEVEL_SHIFT, Level+1, 
                        Mask bsl ?TIME_LEVEL_SHIFT, TW)
            end;
        _ ->
            TW
    end.

cascade(Level, Idx, TW) ->
    #time_wheel{levels = Levels} = TW,
    LevelMap = maps:get(Level, Levels),
    EventList = maps:get(Idx, LevelMap),
    LevelMap1 = maps:put(Idx, [], LevelMap),
    Levels1 = maps:put(Level, LevelMap1, Levels),
    TW1 = TW#time_wheel{levels = Levels1},
    lists:foldl(fun(Event, TWAcc) ->
        add_timer_event(Event, TWAcc)
    end, TW1, EventList).

timer_execute(TW) ->
    #time_wheel{tick = Tick} = TW,
    Idx = Tick band ?TIME_NEAR_MASK,
    {EventList, TW1} = extract_near(Idx, TW),
    TW3 = lists:foldl(fun(#time_event{msg= {Mod, Info}}, TW2) ->
        Mod:handle(Info, TW2)
    end, TW1, EventList),
    TW3.

extract_near(Idx, TW) ->
    #time_wheel{near = Near} = TW,
    EventList = maps:get(Idx, Near),
    Near1 = maps:put(Idx, [], Near),
    {EventList, TW#time_wheel{near = Near1}}.

get_time() ->
    {MegaSecs, Secs, MicroSecs} = os:timestamp(),
    (MegaSecs*1000000 + Secs)*100 + MicroSecs div 10000.

rewind(Tick) ->
    Max = 1 bsl 32,
    if
        Tick >= Max -> 
            Tick - Max;
        true -> 
            Tick
    end.
