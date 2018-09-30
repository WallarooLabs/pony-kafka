use ".."
use "ponybench"

class iso _ValReaderU8 is MicroBenchmark
  // Benchmark reading U8
  let _d: ValReader = _d.create()
  let _b: Array[U8] val = recover Array[U8].>undefined(10485760) end

  new iso create() =>
    _d.append(_b)

  fun name(): String =>
    "_ValReaderU8"

  fun ref apply()? =>
    DoNotOptimise[U8](_d.read_u8()?)
    DoNotOptimise.observe()
    if _d.size() < 128 then
      _d.append(_b)
    end

class iso _ValReaderU16 is MicroBenchmark
  // Benchmark reading U16
  let _d: ValReader = _d.create()
  let _b: Array[U8] val = recover Array[U8].>undefined(10485760) end

  new iso create() =>
    _d.append(_b)

  fun name(): String =>
    "_ValReaderU16"

  fun ref apply()? =>
    DoNotOptimise[U16](_d.read_u16()?)
    DoNotOptimise.observe()
    if _d.size() < 128 then
      _d.append(_b)
    end

class iso _ValReaderU16Split is MicroBenchmark
  // Benchmark reading a split U16
  let _d: ValReader = _d.create()
  let _b: Array[Array[U8] val] = _b.create()

  new iso create() =>
    while _b.size() < 2 do
      _b.push(recover Array[U8].>undefined(1) end)
    end

  fun name(): String =>
    "_ValReaderU16Split"

  fun ref before_iteration() =>
    while _d.size() < 2 do
      _d.clear()
      for b in _b.values() do
        _d.append(b)
      end
    end

  fun ref apply()? =>
    DoNotOptimise[U16](_d.read_u16()?)
    DoNotOptimise.observe()

class iso _ValReaderU32 is MicroBenchmark
  // Benchmark reading U32
  let _d: ValReader = _d.create()
  let _b: Array[U8] val = recover Array[U8].>undefined(10485760) end

  new iso create() =>
    _d.append(_b)

  fun name(): String =>
    "_ValReaderU32"

  fun ref apply()? =>
    DoNotOptimise[U32](_d.read_u32()?)
    DoNotOptimise.observe()
    if _d.size() < 128 then
      _d.append(_b)
    end

class iso _ValReaderU32Split is MicroBenchmark
  // Benchmark reading a split U32
  let _d: ValReader = _d.create()
  let _b: Array[Array[U8] val] = _b.create()

  new iso create() =>
    while _b.size() < 4 do
      _b.push(recover Array[U8].>undefined(1) end)
    end

  fun name(): String =>
    "_ValReaderU32Split"

  fun ref before_iteration() =>
    while _d.size() < 4 do
      _d.clear()
      for b in _b.values() do
        _d.append(b)
      end
    end

  fun ref apply()? =>
    DoNotOptimise[U32](_d.read_u32()?)
    DoNotOptimise.observe()

class iso _ValReaderU64 is MicroBenchmark
  // Benchmark reading U64
  let _d: ValReader = _d.create()
  let _b: Array[U8] val = recover Array[U8].>undefined(10485760) end

  new iso create() =>
    _d.append(_b)

  fun name(): String =>
    "_ValReaderU64"

  fun ref apply()? =>
    DoNotOptimise[U64](_d.read_u64()?)
    DoNotOptimise.observe()
    if _d.size() < 128 then
      _d.append(_b)
    end

class iso _ValReaderU64Split is MicroBenchmark
  // Benchmark reading a split U64
  let _d: ValReader = _d.create()
  let _b: Array[Array[U8] val] = _b.create()

  new iso create() =>
    while _b.size() < 8 do
      _b.push(recover Array[U8].>undefined(1) end)
    end

  fun name(): String =>
    "_ValReaderU64Split"

  fun ref before_iteration() =>
    while _d.size() < 8 do
      _d.clear()
      for b in _b.values() do
        _d.append(b)
      end
    end

  fun ref apply()? =>
    DoNotOptimise[U64](_d.read_u64()?)
    DoNotOptimise.observe()

class iso _ValReaderU128 is MicroBenchmark
  // Benchmark reading U128
  let _d: ValReader = _d.create()
  let _b: Array[U8] val = recover Array[U8].>undefined(10485760) end

  new iso create() =>
    _d.append(_b)

  fun name(): String =>
    "_ValReaderU128"

  fun ref apply()? =>
    DoNotOptimise[U128](_d.read_u128()?)
    DoNotOptimise.observe()
    if _d.size() < 128 then
      _d.append(_b)
    end

class iso _ValReaderU128Split is MicroBenchmark
  // Benchmark reading a split U128
  let _d: ValReader = _d.create()
  let _b: Array[Array[U8] val] = _b.create()

  new iso create() =>
    while _b.size() < 16 do
      _b.push(recover Array[U8].>undefined(1) end)
    end

  fun name(): String =>
    "_ValReaderU128Split"

  fun ref before_iteration() =>
    while _d.size() < 16 do
      _d.clear()
      for b in _b.values() do
        _d.append(b)
      end
    end

  fun ref apply()? =>
    DoNotOptimise[U128](_d.read_u128()?)
    DoNotOptimise.observe()
