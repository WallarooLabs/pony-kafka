use "collections"
use "../customlogger"

trait FsmState
  fun string(): String

primitive FsmStateAny is FsmState
  fun string(): String => "FsmStateAny"

primitive FsmStateNotOld is FsmState
  fun string(): String => "FsmStateNotOld"

class Fsm[A]
  let state_transitions: MapIs[(FsmState val, FsmState val), {ref(FsmState val,
    Fsm[A], A) ?} ref] = state_transitions.create()
  let states: SetIs[FsmState val] = states.create()
  let valid_transitions: Array[(FsmState val, FsmState val, {ref(FsmState val,
    Fsm[A], A) ?} ref)] = valid_transitions.create()
  var error_func: {ref(FsmState val, FsmState val, Fsm[A], A)} ref =
    {ref(old_state: FsmState val, new_state: FsmState val, state_machine:
    Fsm[A], data: A) => None} ref
  var _current_state: FsmState val = FsmStateAny
  let _logger: Logger[String]


  new create(logger: Logger[String]) => _logger = logger

  fun ref initialize(initial_state: FsmState val,
    error_func': {ref(FsmState val, FsmState val, Fsm[A], A)} ref) ?
  =>
    if (initial_state is FsmStateAny) or (initial_state is FsmStateNotOld) then
      _logger(Error) and _logger.log(Error,
        "Initial state can't be FsmStateAny/FsmStateNotOld")
      error
    end

    _current_state = initial_state
    error_func = error_func'

    for (old_state, new_state, func) in valid_transitions.values() do
      if old_state is FsmStateAny then
        _add_state_transitions_old_any(new_state, func)
      elseif old_state is FsmStateNotOld then
        _add_state_transitions_old_not_self(new_state, func)
      elseif new_state is FsmStateAny then
        _add_state_transitions_new_any(old_state, func)
      elseif new_state is FsmStateNotOld then
        _add_state_transitions_new_not_self(old_state, func)
      else
        _logger(Fine) and _logger.log(Fine,
          "Adding valid state transition from: " + old_state.string() + " to " +
          new_state.string())
        state_transitions.insert((old_state, new_state), func)
      end
    end

  fun ref _add_state_transitions_old_any(new_state: FsmState val,
    func: {ref(FsmState val, Fsm[A], A) ?} ref) ?
  =>
    for s in states.values() do
      _logger(Fine) and _logger.log(Fine, "Adding valid state transition from: "
         + s.string() + " to " + new_state.string())
      state_transitions.insert((s, new_state), func)
    end

  fun ref _add_state_transitions_old_not_self(new_state: FsmState val,
    func: {ref(FsmState val, Fsm[A], A) ?} ref) ?
  =>
    for s in states.values() do
      if s is new_state then
        continue
      end
      _logger(Fine) and _logger.log(Fine,
        "Adding valid state transition from: " + s.string() + " to " +
        new_state.string())
      state_transitions.insert((s, new_state), func)
    end

  fun ref _add_state_transitions_new_any(old_state: FsmState val,
    func: {ref(FsmState val, Fsm[A], A) ?} ref) ?
  =>
    for s in states.values() do
      _logger(Fine) and _logger.log(Fine, "Adding valid state transition from: "
         + old_state.string() + " to " + s.string())
      state_transitions.insert((old_state, s), func)
    end

  fun ref _add_state_transitions_new_not_self(old_state: FsmState val,
    func: {ref(FsmState val, Fsm[A], A) ?} ref) ?
  =>
    for s in states.values() do
      if s is old_state then
        continue
      end
      _logger(Fine) and _logger.log(Fine, "Adding valid state transition from: "
         + old_state.string() + " to " + s.string())
      state_transitions.insert((old_state, s), func)
    end

  fun ref add_allowed_state(state: FsmState val) ? =>
    if not (_current_state is FsmStateAny) then
      _logger(Error) and _logger.log(Error,
        "Can't add allowed states after initialization")
      error
    end

    states.set(state)

  fun ref valid_transition(old_state: FsmState val, new_state: FsmState val,
    func: {ref(FsmState val, Fsm[A], A) ?} ref) ?
  =>
    if not (_current_state is FsmStateAny) then
      _logger(Error) and _logger.log(Error,
        "Can't add valid transitions after initialization")
      error
    end

    if ((old_state is FsmStateAny) or (old_state is FsmStateNotOld)) and
      ((new_state is FsmStateAny) or (new_state is FsmStateNotOld)) then
      _logger(Error) and _logger.log(Error,
        "Old state and new state can't both be FsmStateAny/FsmStateNotOld")
      error
    end
    if ((not ((old_state is FsmStateAny) or (old_state is FsmStateNotOld))) and
      (not states.contains(old_state))) or ((not ((new_state is FsmStateAny) or
      (new_state is FsmStateNotOld)) and (not states.contains(new_state)))) then
      _logger(Error) and _logger.log(Error, "State not found. Either: " +
        old_state.string() + " or " + new_state.string())
      error
    end
    valid_transitions.push((old_state, new_state, func))

  fun ref transition_to(new_state: FsmState val, data: A) ? =>
    _logger(Fine) and _logger.log(Fine, "State transition from: " +
      _current_state.string() + " to " + new_state.string())
    let func = try
        state_transitions((_current_state, new_state))
      else
        _logger(Fine) and _logger.log(Fine,
          "State transition lookup failed. from: " + _current_state.string() +
          " to " + new_state.string())
      end

    match func
    | let f: {ref(FsmState val, Fsm[A], A) ?} ref =>
      let old_state = _current_state = new_state
      try
        f(old_state, this, consume data)
      else
        _logger(Error) and _logger.log(Error,
          "Error running function on state transition")
        error
      end
    else
      _logger(Error) and _logger.log(Error, "state transition not valid")
      error_func(_current_state, new_state, this, consume data)
      error
    end

  fun current_state(): FsmState val => _current_state
