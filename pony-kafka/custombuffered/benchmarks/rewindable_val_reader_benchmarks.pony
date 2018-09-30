use ".."
use "ponybench"

class iso _RewindableValReaderU8 is MicroBenchmark
  // Benchmark reading U8
  let _d: RewindableValReader = _d.create()

  new iso create() =>
    _d.append(recover Array[U8].>undefined(10485760) end)

  fun name(): String =>
    "_RewindableValReaderU8"

  fun ref apply()? =>
    DoNotOptimise[U8](_d.read_u8()?)
    DoNotOptimise.observe()
    if _d.size() < 128 then
      _d.set_position(0)?
    end

class iso _RewindableValReaderU16 is MicroBenchmark
  // Benchmark reading U16
  let _d: RewindableValReader = _d.create()

  new iso create() =>
    _d.append(recover Array[U8].>undefined(10485760) end)

  fun name(): String =>
    "_RewindableValReaderU16"

  fun ref apply()? =>
    DoNotOptimise[U16](_d.read_u16()?)
    DoNotOptimise.observe()
    if _d.size() < 128 then
      _d.set_position(0)?
    end

class iso _RewindableValReaderU16Split is MicroBenchmark
  // Benchmark reading a split U16
  let _d: RewindableValReader = _d.create()

  new iso create() =>
    while _d.size() < 2 do
      _d.append(recover Array[U8].>undefined(1) end)
    end

  fun name(): String =>
    "_RewindableValReaderU16Split"

  fun ref before_iteration() =>
    while _d.size() < 2 do
      try
        _d.set_position(0)?
      end
    end

  fun ref apply()? =>
    DoNotOptimise[U16](_d.read_u16()?)
    DoNotOptimise.observe()

class iso _RewindableValReaderU32 is MicroBenchmark
  // Benchmark reading U32
  let _d: RewindableValReader = _d.create()

  new iso create() =>
    _d.append(recover Array[U8].>undefined(10485760) end)

  fun name(): String =>
    "_RewindableValReaderU32"

  fun ref apply()? =>
    DoNotOptimise[U32](_d.read_u32()?)
    DoNotOptimise.observe()
    if _d.size() < 128 then
      _d.set_position(0)?
    end

class iso _RewindableValReaderU32Split is MicroBenchmark
  // Benchmark reading a split U32
  let _d: RewindableValReader = _d.create()

  new iso create() =>
    while _d.size() < 4 do
      _d.append(recover Array[U8].>undefined(1) end)
    end

  fun name(): String =>
    "_RewindableValReaderU32Split"

  fun ref before_iteration() =>
    while _d.size() < 4 do
      try
        _d.set_position(0)?
      end
    end

  fun ref apply()? =>
    DoNotOptimise[U32](_d.read_u32()?)
    DoNotOptimise.observe()

class iso _RewindableValReaderU64 is MicroBenchmark
  // Benchmark reading U64
  let _d: RewindableValReader = _d.create()

  new iso create() =>
    _d.append(recover Array[U8].>undefined(10485760) end)

  fun name(): String =>
    "_RewindableValReaderU64"

  fun ref apply()? =>
    DoNotOptimise[U64](_d.read_u64()?)
    DoNotOptimise.observe()
    if _d.size() < 128 then
      _d.set_position(0)?
    end

class iso _RewindableValReaderU64Split is MicroBenchmark
  // Benchmark reading a split U64
  let _d: RewindableValReader = _d.create()

  new iso create() =>
    while _d.size() < 8 do
      _d.append(recover Array[U8].>undefined(1) end)
    end

  fun name(): String =>
    "_RewindableValReaderU64Split"

  fun ref before_iteration() =>
    while _d.size() < 8 do
      try
        _d.set_position(0)?
      end
    end

  fun ref apply()? =>
    DoNotOptimise[U64](_d.read_u64()?)
    DoNotOptimise.observe()

class iso _RewindableValReaderU128 is MicroBenchmark
  // Benchmark reading U128
  let _d: RewindableValReader = _d.create()

  new iso create() =>
    _d.append(recover Array[U8].>undefined(10485760) end)

  fun name(): String =>
    "_RewindableValReaderU128"

  fun ref apply()? =>
    DoNotOptimise[U128](_d.read_u128()?)
    DoNotOptimise.observe()
    if _d.size() < 128 then
      _d.set_position(0)?
    end

class iso _RewindableValReaderU128Split is MicroBenchmark
  // Benchmark reading a split U128
  let _d: RewindableValReader = _d.create()

  new iso create() =>
    while _d.size() < 16 do
      _d.append(recover Array[U8].>undefined(1) end)
    end

  fun name(): String =>
    "_RewindableValReaderU128Split"

  fun ref before_iteration() =>
    while _d.size() < 16 do
      try
        _d.set_position(0)?
      end
    end

  fun ref apply()? =>
    DoNotOptimise[U128](_d.read_u128()?)
    DoNotOptimise.observe()
