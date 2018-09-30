use ".."
use "../.."
use "ponybench"
use "collections"

class iso _IsoReaderU8LE is MicroBenchmark
  // Benchmark reading Little Endian U8
  let _d: IsoReader = _d.create()

  new iso create() =>
    _d.append(recover Array[U8].>undefined(10485760) end)

  fun name(): String =>
    "_IsoReaderU8LE"

  fun ref apply()? =>
    DoNotOptimise[U8](LittleEndianDecoder.u8(_d)?)
    DoNotOptimise.observe()
    if _d.size() < 128 then
      _d.clear()
      _d.append(recover Array[U8].>undefined(10485760) end)
    end

class iso _IsoReaderI8LE is MicroBenchmark
  // Benchmark reading Little Endian I8
  let _d: IsoReader = _d.create()

  new iso create() =>
    _d.append(recover Array[U8].>undefined(10485760) end)

  fun name(): String =>
    "_IsoReaderI8LE"

  fun ref apply()? =>
    DoNotOptimise[I8](LittleEndianDecoder.i8(_d)?)
    DoNotOptimise.observe()
    if _d.size() < 128 then
      _d.clear()
      _d.append(recover Array[U8].>undefined(10485760) end)
    end

class iso _IsoReaderBoolLE is MicroBenchmark
  // Benchmark reading Little Endian Boolean
  let _d: IsoReader = _d.create()

  new iso create() =>
    _d.append(recover Array[U8].>undefined(10485760) end)

  fun name(): String =>
    "_IsoReaderBoolLE"

  fun ref apply()? =>
    DoNotOptimise[Bool](LittleEndianDecoder.bool(_d)?)
    DoNotOptimise.observe()
    if _d.size() < 128 then
      _d.clear()
      _d.append(recover Array[U8].>undefined(10485760) end)
    end

class iso _IsoReaderU16LE is MicroBenchmark
  // Benchmark reading Little Endian U16
  let _d: IsoReader = _d.create()

  new iso create() =>
    _d.append(recover Array[U8].>undefined(10485760) end)

  fun name(): String =>
    "_IsoReaderU16LE"

  fun ref apply()? =>
    DoNotOptimise[U16](LittleEndianDecoder.u16(_d)?)
    DoNotOptimise.observe()
    if _d.size() < 128 then
      _d.clear()
      _d.append(recover Array[U8].>undefined(10485760) end)
    end

class iso _IsoReaderI16LE is MicroBenchmark
  // Benchmark reading Little Endian I16
  let _d: IsoReader = _d.create()

  new iso create() =>
    _d.append(recover Array[U8].>undefined(10485760) end)

  fun name(): String =>
    "_IsoReaderI16LE"

  fun ref apply()? =>
    DoNotOptimise[I16](LittleEndianDecoder.i16(_d)?)
    DoNotOptimise.observe()
    if _d.size() < 128 then
      _d.clear()
      _d.append(recover Array[U8].>undefined(10485760) end)
    end

class iso _IsoReaderU32LE is MicroBenchmark
  // Benchmark reading Little Endian U32
  let _d: IsoReader = _d.create()

  new iso create() =>
    _d.append(recover Array[U8].>undefined(10485760) end)

  fun name(): String =>
    "_IsoReaderU32LE"

  fun ref apply()? =>
    DoNotOptimise[U32](LittleEndianDecoder.u32(_d)?)
    DoNotOptimise.observe()
    if _d.size() < 128 then
      _d.clear()
      _d.append(recover Array[U8].>undefined(10485760) end)
    end

class iso _IsoReaderI32LE is MicroBenchmark
  // Benchmark reading Little Endian I32
  let _d: IsoReader = _d.create()

  new iso create() =>
    _d.append(recover Array[U8].>undefined(10485760) end)

  fun name(): String =>
    "_IsoReaderI32LE"

  fun ref apply()? =>
    DoNotOptimise[I32](LittleEndianDecoder.i32(_d)?)
    DoNotOptimise.observe()
    if _d.size() < 128 then
      _d.clear()
      _d.append(recover Array[U8].>undefined(10485760) end)
    end

class iso _IsoReaderF32LE is MicroBenchmark
  // Benchmark reading Little Endian F32
  let _d: IsoReader = _d.create()

  new iso create() =>
    _d.append(recover Array[U8].>undefined(10485760) end)

  fun name(): String =>
    "_IsoReaderF32LE"

  fun ref apply()? =>
    DoNotOptimise[F32](LittleEndianDecoder.f32(_d)?)
    DoNotOptimise.observe()
    if _d.size() < 128 then
      _d.clear()
      _d.append(recover Array[U8].>undefined(10485760) end)
    end

class iso _IsoReaderU64LE is MicroBenchmark
  // Benchmark reading Little Endian U64
  let _d: IsoReader = _d.create()

  new iso create() =>
    _d.append(recover Array[U8].>undefined(10485760) end)

  fun name(): String =>
    "_IsoReaderU64LE"

  fun ref apply()? =>
    DoNotOptimise[U64](LittleEndianDecoder.u64(_d)?)
    DoNotOptimise.observe()
    if _d.size() < 128 then
      _d.clear()
      _d.append(recover Array[U8].>undefined(10485760) end)
    end

class iso _IsoReaderI64LE is MicroBenchmark
  // Benchmark reading Little Endian I64
  let _d: IsoReader = _d.create()

  new iso create() =>
    _d.append(recover Array[U8].>undefined(10485760) end)

  fun name(): String =>
    "_IsoReaderI64LE"

  fun ref apply()? =>
    DoNotOptimise[I64](LittleEndianDecoder.i64(_d)?)
    DoNotOptimise.observe()
    if _d.size() < 128 then
      _d.clear()
      _d.append(recover Array[U8].>undefined(10485760) end)
    end

class iso _IsoReaderF64LE is MicroBenchmark
  // Benchmark reading Little Endian F64
  let _d: IsoReader = _d.create()

  new iso create() =>
    _d.append(recover Array[U8].>undefined(10485760) end)

  fun name(): String =>
    "_IsoReaderF64LE"

  fun ref apply()? =>
    DoNotOptimise[F64](LittleEndianDecoder.f64(_d)?)
    DoNotOptimise.observe()
    if _d.size() < 128 then
      _d.clear()
      _d.append(recover Array[U8].>undefined(10485760) end)
    end

class iso _IsoReaderU128LE is MicroBenchmark
  // Benchmark reading Little Endian U128
  let _d: IsoReader = _d.create()

  new iso create() =>
    _d.append(recover Array[U8].>undefined(10485760) end)

  fun name(): String =>
    "_IsoReaderU128LE"

  fun ref apply()? =>
    DoNotOptimise[U128](LittleEndianDecoder.u128(_d)?)
    DoNotOptimise.observe()
    if _d.size() < 128 then
      _d.clear()
      _d.append(recover Array[U8].>undefined(10485760) end)
    end

class iso _IsoReaderI128LE is MicroBenchmark
  // Benchmark reading Little Endian I128
  let _d: IsoReader = _d.create()

  new iso create() =>
    _d.append(recover Array[U8].>undefined(10485760) end)

  fun name(): String =>
    "_IsoReaderI128LE"

  fun ref apply()? =>
    DoNotOptimise[I128](LittleEndianDecoder.i128(_d)?)
    DoNotOptimise.observe()
    if _d.size() < 128 then
      _d.clear()
      _d.append(recover Array[U8].>undefined(10485760) end)
    end

class iso _ValReaderU8LE is MicroBenchmark
  // Benchmark reading U8
  let _d: ValReader = _d.create()

  new iso create() =>
    _d.append(recover Array[U8].>undefined(10485760) end)

  fun name(): String =>
    "_ValReaderU8LE"

  fun ref apply()? =>
    DoNotOptimise[U8](LittleEndianDecoder.u8(_d)?)
    DoNotOptimise.observe()
    if _d.size() < 128 then
      _d.set_position(0)?
    end

class iso _ValReaderI8LE is MicroBenchmark
  // Benchmark reading I8
  let _d: ValReader = _d.create()

  new iso create() =>
    _d.append(recover Array[U8].>undefined(10485760) end)

  fun name(): String =>
    "_ValReaderI8LE"

  fun ref apply()? =>
    DoNotOptimise[I8](LittleEndianDecoder.i8(_d)?)
    DoNotOptimise.observe()
    if _d.size() < 128 then
      _d.set_position(0)?
    end

class iso _ValReaderBoolLE is MicroBenchmark
  // Benchmark reading Boolean
  let _d: ValReader = _d.create()

  new iso create() =>
    _d.append(recover Array[U8].>undefined(10485760) end)

  fun name(): String =>
    "_ValReaderBoolLE"

  fun ref apply()? =>
    DoNotOptimise[Bool](LittleEndianDecoder.bool(_d)?)
    DoNotOptimise.observe()
    if _d.size() < 128 then
      _d.set_position(0)?
    end

class iso _ValReaderU16LE is MicroBenchmark
  // Benchmark reading Little Endian U16
  let _d: ValReader = _d.create()

  new iso create() =>
    _d.append(recover Array[U8].>undefined(10485760) end)

  fun name(): String =>
    "_ValReaderU16LE"

  fun ref apply()? =>
    DoNotOptimise[U16](LittleEndianDecoder.u16(_d)?)
    DoNotOptimise.observe()
    if _d.size() < 128 then
      _d.set_position(0)?
    end

class iso _ValReaderI16LE is MicroBenchmark
  // Benchmark reading Little Endian I16
  let _d: ValReader = _d.create()

  new iso create() =>
    _d.append(recover Array[U8].>undefined(10485760) end)

  fun name(): String =>
    "_ValReaderI16LE"

  fun ref apply()? =>
    DoNotOptimise[I16](LittleEndianDecoder.i16(_d)?)
    DoNotOptimise.observe()
    if _d.size() < 128 then
      _d.set_position(0)?
    end

class iso _ValReaderU32LE is MicroBenchmark
  // Benchmark reading Little Endian U32
  let _d: ValReader = _d.create()

  new iso create() =>
    _d.append(recover Array[U8].>undefined(10485760) end)

  fun name(): String =>
    "_ValReaderU32LE"

  fun ref apply()? =>
    DoNotOptimise[U32](LittleEndianDecoder.u32(_d)?)
    DoNotOptimise.observe()
    if _d.size() < 128 then
      _d.set_position(0)?
    end

class iso _ValReaderI32LE is MicroBenchmark
  // Benchmark reading Little Endian I32
  let _d: ValReader = _d.create()

  new iso create() =>
    _d.append(recover Array[U8].>undefined(10485760) end)

  fun name(): String =>
    "_ValReaderI32LE"

  fun ref apply()? =>
    DoNotOptimise[I32](LittleEndianDecoder.i32(_d)?)
    DoNotOptimise.observe()
    if _d.size() < 128 then
      _d.set_position(0)?
    end

class iso _ValReaderF32LE is MicroBenchmark
  // Benchmark reading Little Endian F32
  let _d: ValReader = _d.create()

  new iso create() =>
    _d.append(recover Array[U8].>undefined(10485760) end)

  fun name(): String =>
    "_ValReaderF32LE"

  fun ref apply()? =>
    DoNotOptimise[F32](LittleEndianDecoder.f32(_d)?)
    DoNotOptimise.observe()
    if _d.size() < 128 then
      _d.set_position(0)?
    end

class iso _ValReaderU64LE is MicroBenchmark
  // Benchmark reading Little Endian U64
  let _d: ValReader = _d.create()

  new iso create() =>
    _d.append(recover Array[U8].>undefined(10485760) end)

  fun name(): String =>
    "_ValReaderU64LE"

  fun ref apply()? =>
    DoNotOptimise[U64](LittleEndianDecoder.u64(_d)?)
    DoNotOptimise.observe()
    if _d.size() < 128 then
      _d.set_position(0)?
    end

class iso _ValReaderI64LE is MicroBenchmark
  // Benchmark reading Little Endian I64
  let _d: ValReader = _d.create()

  new iso create() =>
    _d.append(recover Array[U8].>undefined(10485760) end)

  fun name(): String =>
    "_ValReaderI64LE"

  fun ref apply()? =>
    DoNotOptimise[I64](LittleEndianDecoder.i64(_d)?)
    DoNotOptimise.observe()
    if _d.size() < 128 then
      _d.set_position(0)?
    end

class iso _ValReaderF64LE is MicroBenchmark
  // Benchmark reading Little Endian F64
  let _d: ValReader = _d.create()

  new iso create() =>
    _d.append(recover Array[U8].>undefined(10485760) end)

  fun name(): String =>
    "_ValReaderF64LE"

  fun ref apply()? =>
    DoNotOptimise[F64](LittleEndianDecoder.f64(_d)?)
    DoNotOptimise.observe()
    if _d.size() < 128 then
      _d.set_position(0)?
    end

class iso _ValReaderU128LE is MicroBenchmark
  // Benchmark reading Little Endian U128
  let _d: ValReader = _d.create()

  new iso create() =>
    _d.append(recover Array[U8].>undefined(10485760) end)

  fun name(): String =>
    "_ValReaderU128LE"

  fun ref apply()? =>
    DoNotOptimise[U128](LittleEndianDecoder.u128(_d)?)
    DoNotOptimise.observe()
    if _d.size() < 128 then
      _d.set_position(0)?
    end

class iso _ValReaderI128LE is MicroBenchmark
  // Benchmark reading Little Endian I128
  let _d: ValReader = _d.create()

  new iso create() =>
    _d.append(recover Array[U8].>undefined(10485760) end)

  fun name(): String =>
    "_ValReaderI128LE"

  fun ref apply()? =>
    DoNotOptimise[I128](LittleEndianDecoder.i128(_d)?)
    DoNotOptimise.observe()
    if _d.size() < 128 then
      _d.set_position(0)?
    end

class iso _WriterU8LE is MicroBenchmark
  // Benchmark writing Little Endian U8
  let _d: Writer = _d.create() .> reserve_current(10485760)
  var _i: U8 = 0

  fun name(): String =>
    "_WriterU8LE"

  fun ref apply() =>
    DoNotOptimise[None](LittleEndianEncoder.u8(_d, _i))
    DoNotOptimise.observe()
    _i = _i + 1
    if _d.size() > 10485760 then
      _d.done()
      _d.reserve_current(10485760)
    end

class iso _WriterI8LE is MicroBenchmark
  // Benchmark writing Little Endian I8
  let _d: Writer = _d.create() .> reserve_current(10485760)
  var _i: I8 = 0

  fun name(): String =>
    "_WriterI8LE"

  fun ref apply() =>
    DoNotOptimise[None](LittleEndianEncoder.i8(_d, _i))
    DoNotOptimise.observe()
    _i = _i + 1
    if _d.size() > 10485760 then
      _d.done()
      _d.reserve_current(10485760)
    end

class iso _WriterBoolLE is MicroBenchmark
  // Benchmark writing Little Endian Boolean
  let _d: Writer = _d.create() .> reserve_current(10485760)
  var _i: U8 = 0

  fun name(): String =>
    "_WriterBoolLE"

  fun ref apply() =>
    DoNotOptimise[None](LittleEndianEncoder.bool(_d, (_i % 2) == 0))
    DoNotOptimise.observe()
    _i = _i + 1
    if _d.size() > 10485760 then
      _d.done()
      _d.reserve_current(10485760)
    end

class iso _WriterU16LE is MicroBenchmark
  // Benchmark writing Little Endian U16
  let _d: Writer = _d.create() .> reserve_current(10485760)
  var _i: U16 = 0

  fun name(): String =>
    "_WriterU16LE"

  fun ref apply() =>
    DoNotOptimise[None](LittleEndianEncoder.u16(_d, _i))
    DoNotOptimise.observe()
    _i = _i + 1
    if _d.size() > 10485760 then
      _d.done()
      _d.reserve_current(10485760)
    end

class iso _WriterI16LE is MicroBenchmark
  // Benchmark writing Little Endian I16
  let _d: Writer = _d.create() .> reserve_current(10485760)
  var _i: I16 = 0

  fun name(): String =>
    "_WriterI16LE"

  fun ref apply() =>
    DoNotOptimise[None](LittleEndianEncoder.i16(_d, _i))
    DoNotOptimise.observe()
    _i = _i + 1
    if _d.size() > 10485760 then
      _d.done()
      _d.reserve_current(10485760)
    end

class iso _WriterU32LE is MicroBenchmark
  // Benchmark writing Little Endian U32
  let _d: Writer = _d.create() .> reserve_current(10485760)
  var _i: U32 = 0

  fun name(): String =>
    "_WriterU32LE"

  fun ref apply() =>
    DoNotOptimise[None](LittleEndianEncoder.u32(_d, _i))
    DoNotOptimise.observe()
    _i = _i + 1
    if _d.size() > 10485760 then
      _d.done()
      _d.reserve_current(10485760)
    end

class iso _WriterI32LE is MicroBenchmark
  // Benchmark writing Little Endian I32
  let _d: Writer = _d.create() .> reserve_current(10485760)
  var _i: I32 = 0

  fun name(): String =>
    "_WriterI32LE"

  fun ref apply() =>
    DoNotOptimise[None](LittleEndianEncoder.i32(_d, _i))
    DoNotOptimise.observe()
    _i = _i + 1
    if _d.size() > 10485760 then
      _d.done()
      _d.reserve_current(10485760)
    end

class iso _WriterF32LE is MicroBenchmark
  // Benchmark writing Little Endian F32
  let _d: Writer = _d.create() .> reserve_current(10485760)
  var _i: F32 = 0

  fun name(): String =>
    "_WriterF32LE"

  fun ref apply() =>
    DoNotOptimise[None](LittleEndianEncoder.f32(_d, _i))
    DoNotOptimise.observe()
    _i = _i + 1
    if _d.size() > 10485760 then
      _d.done()
      _d.reserve_current(10485760)
    end

class iso _WriterU64LE is MicroBenchmark
  // Benchmark writing Little Endian U64
  let _d: Writer = _d.create() .> reserve_current(10485760)
  var _i: U64 = 0

  fun name(): String =>
    "_WriterU64LE"

  fun ref apply() =>
    DoNotOptimise[None](LittleEndianEncoder.u64(_d, _i))
    DoNotOptimise.observe()
    _i = _i + 1
    if _d.size() > 10485760 then
      _d.done()
      _d.reserve_current(10485760)
    end

class iso _WriterI64LE is MicroBenchmark
  // Benchmark writing Little Endian I64
  let _d: Writer = _d.create() .> reserve_current(10485760)
  var _i: I64 = 0

  fun name(): String =>
    "_WriterI64LE"

  fun ref apply() =>
    DoNotOptimise[None](LittleEndianEncoder.i64(_d, _i))
    DoNotOptimise.observe()
    _i = _i + 1
    if _d.size() > 10485760 then
      _d.done()
      _d.reserve_current(10485760)
    end

class iso _WriterF64LE is MicroBenchmark
  // Benchmark writing Little Endian F64
  let _d: Writer = _d.create() .> reserve_current(10485760)
  var _i: F64 = 0

  fun name(): String =>
    "_WriterF64LE"

  fun ref apply() =>
    DoNotOptimise[None](LittleEndianEncoder.f64(_d, _i))
    DoNotOptimise.observe()
    _i = _i + 1
    if _d.size() > 10485760 then
      _d.done()
      _d.reserve_current(10485760)
    end

class iso _WriterU128LE is MicroBenchmark
  // Benchmark writing Little Endian U128
  let _d: Writer = _d.create() .> reserve_current(10485760)
  var _i: U128 = 0

  fun name(): String =>
    "_WriterU128LE"

  fun ref apply() =>
    DoNotOptimise[None](LittleEndianEncoder.u128(_d, _i))
    DoNotOptimise.observe()
    _i = _i + 1
    if _d.size() > 10485760 then
      _d.done()
      _d.reserve_current(10485760)
    end

class iso _WriterI128LE is MicroBenchmark
  // Benchmark writing Little Endian I128
  let _d: Writer = _d.create() .> reserve_current(10485760)
  var _i: I128 = 0

  fun name(): String =>
    "_WriterI128LE"

  fun ref apply() =>
    DoNotOptimise[None](LittleEndianEncoder.i128(_d, _i))
    DoNotOptimise.observe()
    _i = _i + 1
    if _d.size() > 10485760 then
      _d.done()
      _d.reserve_current(10485760)
    end
