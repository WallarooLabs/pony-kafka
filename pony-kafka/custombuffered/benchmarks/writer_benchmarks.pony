use ".."
use "ponybench"

class iso _WriterU8 is MicroBenchmark
  // Benchmark writing U8
  let _d: Writer = _d.create() .> reserve_current(10485760)
  var _i: U8 = 0

  fun name(): String =>
    "_WriterU8"

  fun ref apply() =>
    DoNotOptimise[None](_d.write_u8(_i))
    DoNotOptimise.observe()
    _i = _i + 1
    if _d.size() > 10485760 then
      _d.done()
      _d.reserve_current(10485760)
    end

class iso _WriterU16 is MicroBenchmark
  // Benchmark writing U16
  let _d: Writer = _d.create() .> reserve_current(10485760)
  var _i: U16 = 0

  fun name(): String =>
    "_WriterU16"

  fun ref apply() =>
    DoNotOptimise[None](_d.write_u16(_i))
    DoNotOptimise.observe()
    _i = _i + 1
    if _d.size() > 10485760 then
      _d.done()
      _d.reserve_current(10485760)
    end

class iso _WriterU32 is MicroBenchmark
  // Benchmark writing U32
  let _d: Writer = _d.create() .> reserve_current(10485760)
  var _i: U32 = 0

  fun name(): String =>
    "_WriterU32"

  fun ref apply() =>
    DoNotOptimise[None](_d.write_u32(_i))
    DoNotOptimise.observe()
    _i = _i + 1
    if _d.size() > 10485760 then
      _d.done()
      _d.reserve_current(10485760)
    end

class iso _WriterU64 is MicroBenchmark
  // Benchmark writing U64
  let _d: Writer = _d.create() .> reserve_current(10485760)
  var _i: U64 = 0

  fun name(): String =>
    "_WriterU64"

  fun ref apply() =>
    DoNotOptimise[None](_d.write_u64(_i))
    DoNotOptimise.observe()
    _i = _i + 1
    if _d.size() > 10485760 then
      _d.done()
      _d.reserve_current(10485760)
    end

class iso _WriterU128 is MicroBenchmark
  // Benchmark writing U128
  let _d: Writer = _d.create() .> reserve_current(10485760)
  var _i: U128 = 0

  fun name(): String =>
    "_WriterU128"

  fun ref apply() =>
    DoNotOptimise[None](_d.write_u128(_i))
    DoNotOptimise.observe()
    _i = _i + 1
    if _d.size() > 10485760 then
      _d.done()
      _d.reserve_current(10485760)
    end
