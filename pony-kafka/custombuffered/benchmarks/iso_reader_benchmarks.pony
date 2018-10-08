use ".."
use "ponybench"

class iso _IsoReaderU8 is MicroBenchmark
  // Benchmark reading U8
  let _d: IsoReader = _d.create()

  new iso create() =>
    _d.append(recover Array[U8].>undefined(10485760) end)

  fun name(): String =>
    "_IsoReaderU8"

  fun ref apply()? =>
    DoNotOptimise[U8](_d.read_u8()?)
    DoNotOptimise.observe()
    if _d.size() <= 128 then
      _d.append(recover Array[U8].>undefined(10485760) end)
    end

class iso _IsoReaderU16 is MicroBenchmark
  // Benchmark reading U16
  let _d: IsoReader = _d.create()

  new iso create() =>
    _d.append(recover Array[U8].>undefined(10485760) end)

  fun name(): String =>
    "_IsoReaderU16"

  fun ref apply()? =>
    DoNotOptimise[U16](_d.read_u16()?)
    DoNotOptimise.observe()
    if _d.size() <= 128 then
      _d.append(recover Array[U8].>undefined(10485760) end)
    end

class iso _IsoReaderU16Split is MicroBenchmark
  // Benchmark reading a split U16
  let _d: IsoReader = _d.create()

  fun name(): String =>
    "_IsoReaderU16Split"

  fun ref before_iteration() =>
    while _d.size() < 2 do
      _d.append(recover Array[U8].>undefined(1) end)
    end

  fun ref apply()? =>
    DoNotOptimise[U16](_d.read_u16()?)
    DoNotOptimise.observe()

class iso _IsoReaderU32 is MicroBenchmark
  // Benchmark reading U32
  let _d: IsoReader = _d.create()

  new iso create() =>
    _d.append(recover Array[U8].>undefined(10485760) end)

  fun name(): String =>
    "_IsoReaderU32"

  fun ref apply()? =>
    DoNotOptimise[U32](_d.read_u32()?)
    DoNotOptimise.observe()
    if _d.size() <= 128 then
      _d.append(recover Array[U8].>undefined(10485760) end)
    end

class iso _IsoReaderU32Split is MicroBenchmark
  // Benchmark reading a split U32
  let _d: IsoReader = _d.create()

  fun name(): String =>
    "_IsoReaderU32Split"

  fun ref before_iteration() =>
    while _d.size() < 4 do
      _d.append(recover Array[U8].>undefined(1) end)
    end

  fun ref apply()? =>
    DoNotOptimise[U32](_d.read_u32()?)
    DoNotOptimise.observe()

class iso _IsoReaderU64 is MicroBenchmark
  // Benchmark reading U64
  let _d: IsoReader = _d.create()


  fun name(): String =>
    "_IsoReaderU64"

  fun ref before_iteration() =>
    if _d.size() <= 8 then
      _d.clear()
      _d.append(recover Array[U8].>undefined(9) end)
    end

  fun ref apply()? =>
    DoNotOptimise[U64](_d.read_u64()?)
    DoNotOptimise.observe()

class iso _IsoReaderU64Split is MicroBenchmark
  // Benchmark reading a split U64
  let _d: IsoReader = _d.create()

  fun name(): String =>
    "_IsoReaderU64Split"

  fun ref before_iteration() =>
    while _d.size() < 8 do
      _d.append(recover Array[U8].>undefined(1) end)
    end

  fun ref apply()? =>
    DoNotOptimise[U64](_d.read_u64()?)
    DoNotOptimise.observe()

class iso _IsoReaderU128 is MicroBenchmark
  // Benchmark reading U128
  let _d: IsoReader = _d.create()


  fun name(): String =>
    "_IsoReaderU128"

  fun ref before_iteration() =>
    if _d.size() <= 16 then
      _d.clear()
      _d.append(recover Array[U8].>undefined(17) end)
    end

  fun ref apply()? =>
    DoNotOptimise[U128](_d.read_u128()?)
    DoNotOptimise.observe()

class iso _IsoReaderU128Split is MicroBenchmark
  // Benchmark reading a split U128
  let _d: IsoReader = _d.create()

  fun name(): String =>
    "_IsoReaderU128Split"

  fun ref before_iteration() =>
    while _d.size() < 16 do
      _d.append(recover Array[U8].>undefined(1) end)
    end

  fun ref apply()? =>
    DoNotOptimise[U128](_d.read_u128()?)
    DoNotOptimise.observe()
