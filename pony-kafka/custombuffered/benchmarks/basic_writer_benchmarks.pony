use ".."
use "ponybench"

class iso _BasicWriterU8 is MicroBenchmark
  // Benchmark writing U8
  let _d: BasicWriter = _d.create() .> reserve_current(10485760)
  var _i: U8 = 0

  fun name(): String =>
    "_BasicWriterU8"

  fun ref before_iteration() =>
    _i = _i + 1
    if _d.size() > 10485760 then
      _d.done()
      _d.reserve_current(10485760)
    end

  fun ref apply() =>
    DoNotOptimise[None](_d.write_u8(_i))
    DoNotOptimise.observe()

class iso _BasicWriterU16 is MicroBenchmark
  // Benchmark writing U16
  let _d: BasicWriter = _d.create() .> reserve_current(10485760)
  var _i: U16 = 0

  fun name(): String =>
    "_BasicWriterU16"

  fun ref before_iteration() =>
    _i = _i + 1
    if _d.size() > 10485760 then
      _d.done()
      _d.reserve_current(10485760)
    end

  fun ref apply() =>
    DoNotOptimise[None](_d.write_u16(_i))
    DoNotOptimise.observe()

class iso _BasicWriterU32 is MicroBenchmark
  // Benchmark writing U32
  let _d: BasicWriter = _d.create() .> reserve_current(10485760)
  var _i: U32 = 0

  fun name(): String =>
    "_BasicWriterU32"

  fun ref before_iteration() =>
    _i = _i + 1
    if _d.size() > 10485760 then
      _d.done()
      _d.reserve_current(10485760)
    end

  fun ref apply() =>
    DoNotOptimise[None](_d.write_u32(_i))
    DoNotOptimise.observe()

class iso _BasicWriterU64 is MicroBenchmark
  // Benchmark writing U64
  let _d: BasicWriter = _d.create() .> reserve_current(10485760)
  var _i: U64 = 0

  fun name(): String =>
    "_BasicWriterU64"

  fun ref before_iteration() =>
    _i = _i + 1
    if _d.size() > 10485760 then
      _d.done()
      _d.reserve_current(10485760)
    end

  fun ref apply() =>
    DoNotOptimise[None](_d.write_u64(_i))
    DoNotOptimise.observe()

class iso _BasicWriterU128 is MicroBenchmark
  // Benchmark writing U128
  let _d: BasicWriter = _d.create() .> reserve_current(10485760)
  var _i: U128 = 0

  fun name(): String =>
    "_BasicWriterU128"

  fun ref before_iteration() =>
    _i = _i + 1
    if _d.size() > 10485760 then
      _d.done()
      _d.reserve_current(10485760)
    end

  fun ref apply() =>
    DoNotOptimise[None](_d.write_u128(_i))
    DoNotOptimise.observe()
