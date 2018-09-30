use ".."
use "../.."
use "ponybench"
use "collections"

class iso _IsoReaderU8BE is MicroBenchmark
  // Benchmark reading Big Endian U8
  let _d: IsoReader = _d.create()

  new iso create() =>
    _d.append(recover Array[U8].>undefined(10485760) end)

  fun name(): String =>
    "_IsoReaderU8BE"

  fun ref apply()? =>
    DoNotOptimise[U8](BigEndianDecoder.u8(_d)?)
    DoNotOptimise.observe()
    if _d.size() < 128 then
      _d.clear()
      _d.append(recover Array[U8].>undefined(10485760) end)
    end

class iso _IsoReaderI8BE is MicroBenchmark
  // Benchmark reading Big Endian I8
  let _d: IsoReader = _d.create()

  new iso create() =>
    _d.append(recover Array[U8].>undefined(10485760) end)

  fun name(): String =>
    "_IsoReaderI8BE"

  fun ref apply()? =>
    DoNotOptimise[I8](BigEndianDecoder.i8(_d)?)
    DoNotOptimise.observe()
    if _d.size() < 128 then
      _d.clear()
      _d.append(recover Array[U8].>undefined(10485760) end)
    end

class iso _IsoReaderBoolBE is MicroBenchmark
  // Benchmark reading Big Endian Boolean
  let _d: IsoReader = _d.create()

  new iso create() =>
    _d.append(recover Array[U8].>undefined(10485760) end)

  fun name(): String =>
    "_IsoReaderBoolBE"

  fun ref apply()? =>
    DoNotOptimise[Bool](BigEndianDecoder.bool(_d)?)
    DoNotOptimise.observe()
    if _d.size() < 128 then
      _d.clear()
      _d.append(recover Array[U8].>undefined(10485760) end)
    end

class iso _IsoReaderU16BE is MicroBenchmark
  // Benchmark reading Big Endian U16
  let _d: IsoReader = _d.create()

  new iso create() =>
    _d.append(recover Array[U8].>undefined(10485760) end)

  fun name(): String =>
    "_IsoReaderU16BE"

  fun ref apply()? =>
    DoNotOptimise[U16](BigEndianDecoder.u16(_d)?)
    DoNotOptimise.observe()
    if _d.size() < 128 then
      _d.clear()
      _d.append(recover Array[U8].>undefined(10485760) end)
    end

class iso _IsoReaderI16BE is MicroBenchmark
  // Benchmark reading Big Endian I16
  let _d: IsoReader = _d.create()

  new iso create() =>
    _d.append(recover Array[U8].>undefined(10485760) end)

  fun name(): String =>
    "_IsoReaderI16BE"

  fun ref apply()? =>
    DoNotOptimise[I16](BigEndianDecoder.i16(_d)?)
    DoNotOptimise.observe()
    if _d.size() < 128 then
      _d.clear()
      _d.append(recover Array[U8].>undefined(10485760) end)
    end

class iso _IsoReaderU32BE is MicroBenchmark
  // Benchmark reading Big Endian U32
  let _d: IsoReader = _d.create()

  new iso create() =>
    _d.append(recover Array[U8].>undefined(10485760) end)

  fun name(): String =>
    "_IsoReaderU32BE"

  fun ref apply()? =>
    DoNotOptimise[U32](BigEndianDecoder.u32(_d)?)
    DoNotOptimise.observe()
    if _d.size() < 128 then
      _d.clear()
      _d.append(recover Array[U8].>undefined(10485760) end)
    end

class iso _IsoReaderI32BE is MicroBenchmark
  // Benchmark reading Big Endian I32
  let _d: IsoReader = _d.create()

  new iso create() =>
    _d.append(recover Array[U8].>undefined(10485760) end)

  fun name(): String =>
    "_IsoReaderI32BE"

  fun ref apply()? =>
    DoNotOptimise[I32](BigEndianDecoder.i32(_d)?)
    DoNotOptimise.observe()
    if _d.size() < 128 then
      _d.clear()
      _d.append(recover Array[U8].>undefined(10485760) end)
    end

class iso _IsoReaderF32BE is MicroBenchmark
  // Benchmark reading Big Endian F32
  let _d: IsoReader = _d.create()

  new iso create() =>
    _d.append(recover Array[U8].>undefined(10485760) end)

  fun name(): String =>
    "_IsoReaderF32BE"

  fun ref apply()? =>
    DoNotOptimise[F32](BigEndianDecoder.f32(_d)?)
    DoNotOptimise.observe()
    if _d.size() < 128 then
      _d.clear()
      _d.append(recover Array[U8].>undefined(10485760) end)
    end

class iso _IsoReaderU64BE is MicroBenchmark
  // Benchmark reading Big Endian U64
  let _d: IsoReader = _d.create()

  new iso create() =>
    _d.append(recover Array[U8].>undefined(10485760) end)

  fun name(): String =>
    "_IsoReaderU64BE"

  fun ref apply()? =>
    DoNotOptimise[U64](BigEndianDecoder.u64(_d)?)
    DoNotOptimise.observe()
    if _d.size() < 128 then
      _d.clear()
      _d.append(recover Array[U8].>undefined(10485760) end)
    end

class iso _IsoReaderI64BE is MicroBenchmark
  // Benchmark reading Big Endian I64
  let _d: IsoReader = _d.create()

  new iso create() =>
    _d.append(recover Array[U8].>undefined(10485760) end)

  fun name(): String =>
    "_IsoReaderI64BE"

  fun ref apply()? =>
    DoNotOptimise[I64](BigEndianDecoder.i64(_d)?)
    DoNotOptimise.observe()
    if _d.size() < 128 then
      _d.clear()
      _d.append(recover Array[U8].>undefined(10485760) end)
    end

class iso _IsoReaderF64BE is MicroBenchmark
  // Benchmark reading Big Endian F64
  let _d: IsoReader = _d.create()

  new iso create() =>
    _d.append(recover Array[U8].>undefined(10485760) end)

  fun name(): String =>
    "_IsoReaderF64BE"

  fun ref apply()? =>
    DoNotOptimise[F64](BigEndianDecoder.f64(_d)?)
    DoNotOptimise.observe()
    if _d.size() < 128 then
      _d.clear()
      _d.append(recover Array[U8].>undefined(10485760) end)
    end

class iso _IsoReaderU128BE is MicroBenchmark
  // Benchmark reading Big Endian U128
  let _d: IsoReader = _d.create()

  new iso create() =>
    _d.append(recover Array[U8].>undefined(10485760) end)

  fun name(): String =>
    "_IsoReaderU128BE"

  fun ref apply()? =>
    DoNotOptimise[U128](BigEndianDecoder.u128(_d)?)
    DoNotOptimise.observe()
    if _d.size() < 128 then
      _d.clear()
      _d.append(recover Array[U8].>undefined(10485760) end)
    end

class iso _IsoReaderI128BE is MicroBenchmark
  // Benchmark reading Big Endian I128
  let _d: IsoReader = _d.create()

  new iso create() =>
    _d.append(recover Array[U8].>undefined(10485760) end)

  fun name(): String =>
    "_IsoReaderI128BE"

  fun ref apply()? =>
    DoNotOptimise[I128](BigEndianDecoder.i128(_d)?)
    DoNotOptimise.observe()
    if _d.size() < 128 then
      _d.clear()
      _d.append(recover Array[U8].>undefined(10485760) end)
    end

class iso _ValReaderU8BE is MicroBenchmark
  // Benchmark reading U8
  let _d: ValReader = _d.create()

  new iso create() =>
    _d.append(recover Array[U8].>undefined(10485760) end)

  fun name(): String =>
    "_ValReaderU8BE"

  fun ref apply()? =>
    DoNotOptimise[U8](BigEndianDecoder.u8(_d)?)
    DoNotOptimise.observe()
    if _d.size() < 128 then
      _d.set_position(0)?
    end

class iso _ValReaderI8BE is MicroBenchmark
  // Benchmark reading I8
  let _d: ValReader = _d.create()

  new iso create() =>
    _d.append(recover Array[U8].>undefined(10485760) end)

  fun name(): String =>
    "_ValReaderI8BE"

  fun ref apply()? =>
    DoNotOptimise[I8](BigEndianDecoder.i8(_d)?)
    DoNotOptimise.observe()
    if _d.size() < 128 then
      _d.set_position(0)?
    end

class iso _ValReaderBoolBE is MicroBenchmark
  // Benchmark reading Boolean
  let _d: ValReader = _d.create()

  new iso create() =>
    _d.append(recover Array[U8].>undefined(10485760) end)

  fun name(): String =>
    "_ValReaderBoolBE"

  fun ref apply()? =>
    DoNotOptimise[Bool](BigEndianDecoder.bool(_d)?)
    DoNotOptimise.observe()
    if _d.size() < 128 then
      _d.set_position(0)?
    end

class iso _ValReaderU16BE is MicroBenchmark
  // Benchmark reading Big Endian U16
  let _d: ValReader = _d.create()

  new iso create() =>
    _d.append(recover Array[U8].>undefined(10485760) end)

  fun name(): String =>
    "_ValReaderU16BE"

  fun ref apply()? =>
    DoNotOptimise[U16](BigEndianDecoder.u16(_d)?)
    DoNotOptimise.observe()
    if _d.size() < 128 then
      _d.set_position(0)?
    end

class iso _ValReaderI16BE is MicroBenchmark
  // Benchmark reading Big Endian I16
  let _d: ValReader = _d.create()

  new iso create() =>
    _d.append(recover Array[U8].>undefined(10485760) end)

  fun name(): String =>
    "_ValReaderI16BE"

  fun ref apply()? =>
    DoNotOptimise[I16](BigEndianDecoder.i16(_d)?)
    DoNotOptimise.observe()
    if _d.size() < 128 then
      _d.set_position(0)?
    end

class iso _ValReaderU32BE is MicroBenchmark
  // Benchmark reading Big Endian U32
  let _d: ValReader = _d.create()

  new iso create() =>
    _d.append(recover Array[U8].>undefined(10485760) end)

  fun name(): String =>
    "_ValReaderU32BE"

  fun ref apply()? =>
    DoNotOptimise[U32](BigEndianDecoder.u32(_d)?)
    DoNotOptimise.observe()
    if _d.size() < 128 then
      _d.set_position(0)?
    end

class iso _ValReaderI32BE is MicroBenchmark
  // Benchmark reading Big Endian I32
  let _d: ValReader = _d.create()

  new iso create() =>
    _d.append(recover Array[U8].>undefined(10485760) end)

  fun name(): String =>
    "_ValReaderI32BE"

  fun ref apply()? =>
    DoNotOptimise[I32](BigEndianDecoder.i32(_d)?)
    DoNotOptimise.observe()
    if _d.size() < 128 then
      _d.set_position(0)?
    end

class iso _ValReaderF32BE is MicroBenchmark
  // Benchmark reading Big Endian F32
  let _d: ValReader = _d.create()

  new iso create() =>
    _d.append(recover Array[U8].>undefined(10485760) end)

  fun name(): String =>
    "_ValReaderF32BE"

  fun ref apply()? =>
    DoNotOptimise[F32](BigEndianDecoder.f32(_d)?)
    DoNotOptimise.observe()
    if _d.size() < 128 then
      _d.set_position(0)?
    end

class iso _ValReaderU64BE is MicroBenchmark
  // Benchmark reading Big Endian U64
  let _d: ValReader = _d.create()

  new iso create() =>
    _d.append(recover Array[U8].>undefined(10485760) end)

  fun name(): String =>
    "_ValReaderU64BE"

  fun ref apply()? =>
    DoNotOptimise[U64](BigEndianDecoder.u64(_d)?)
    DoNotOptimise.observe()
    if _d.size() < 128 then
      _d.set_position(0)?
    end

class iso _ValReaderI64BE is MicroBenchmark
  // Benchmark reading Big Endian I64
  let _d: ValReader = _d.create()

  new iso create() =>
    _d.append(recover Array[U8].>undefined(10485760) end)

  fun name(): String =>
    "_ValReaderI64BE"

  fun ref apply()? =>
    DoNotOptimise[I64](BigEndianDecoder.i64(_d)?)
    DoNotOptimise.observe()
    if _d.size() < 128 then
      _d.set_position(0)?
    end

class iso _ValReaderF64BE is MicroBenchmark
  // Benchmark reading Big Endian F64
  let _d: ValReader = _d.create()

  new iso create() =>
    _d.append(recover Array[U8].>undefined(10485760) end)

  fun name(): String =>
    "_ValReaderF64BE"

  fun ref apply()? =>
    DoNotOptimise[F64](BigEndianDecoder.f64(_d)?)
    DoNotOptimise.observe()
    if _d.size() < 128 then
      _d.set_position(0)?
    end

class iso _ValReaderU128BE is MicroBenchmark
  // Benchmark reading Big Endian U128
  let _d: ValReader = _d.create()

  new iso create() =>
    _d.append(recover Array[U8].>undefined(10485760) end)

  fun name(): String =>
    "_ValReaderU128BE"

  fun ref apply()? =>
    DoNotOptimise[U128](BigEndianDecoder.u128(_d)?)
    DoNotOptimise.observe()
    if _d.size() < 128 then
      _d.set_position(0)?
    end

class iso _ValReaderI128BE is MicroBenchmark
  // Benchmark reading Big Endian I128
  let _d: ValReader = _d.create()

  new iso create() =>
    _d.append(recover Array[U8].>undefined(10485760) end)

  fun name(): String =>
    "_ValReaderI128BE"

  fun ref apply()? =>
    DoNotOptimise[I128](BigEndianDecoder.i128(_d)?)
    DoNotOptimise.observe()
    if _d.size() < 128 then
      _d.set_position(0)?
    end

class iso _WriterU8BE is MicroBenchmark
  // Benchmark writing Big Endian U8
  let _d: Writer = _d.create() .> reserve_current(10485760)
  var _i: U8 = 0

  fun name(): String =>
    "_WriterU8BE"

  fun ref apply() =>
    DoNotOptimise[None](BigEndianEncoder.u8(_d, _i))
    DoNotOptimise.observe()
    _i = _i + 1
    if _d.size() > 10485760 then
      _d.done()
      _d.reserve_current(10485760)
    end

class iso _WriterI8BE is MicroBenchmark
  // Benchmark writing Big Endian I8
  let _d: Writer = _d.create() .> reserve_current(10485760)
  var _i: I8 = 0

  fun name(): String =>
    "_WriterI8BE"

  fun ref apply() =>
    DoNotOptimise[None](BigEndianEncoder.i8(_d, _i))
    DoNotOptimise.observe()
    _i = _i + 1
    if _d.size() > 10485760 then
      _d.done()
      _d.reserve_current(10485760)
    end

class iso _WriterBoolBE is MicroBenchmark
  // Benchmark writing Big Endian Boolean
  let _d: Writer = _d.create() .> reserve_current(10485760)
  var _i: U8 = 0

  fun name(): String =>
    "_WriterBoolBE"

  fun ref apply() =>
    DoNotOptimise[None](BigEndianEncoder.bool(_d, (_i % 2) == 0))
    DoNotOptimise.observe()
    _i = _i + 1
    if _d.size() > 10485760 then
      _d.done()
      _d.reserve_current(10485760)
    end

class iso _WriterU16BE is MicroBenchmark
  // Benchmark writing Big Endian U16
  let _d: Writer = _d.create() .> reserve_current(10485760)
  var _i: U16 = 0

  fun name(): String =>
    "_WriterU16BE"

  fun ref apply() =>
    DoNotOptimise[None](BigEndianEncoder.u16(_d, _i))
    DoNotOptimise.observe()
    _i = _i + 1
    if _d.size() > 10485760 then
      _d.done()
      _d.reserve_current(10485760)
    end

class iso _WriterI16BE is MicroBenchmark
  // Benchmark writing Big Endian I16
  let _d: Writer = _d.create() .> reserve_current(10485760)
  var _i: I16 = 0

  fun name(): String =>
    "_WriterI16BE"

  fun ref apply() =>
    DoNotOptimise[None](BigEndianEncoder.i16(_d, _i))
    DoNotOptimise.observe()
    _i = _i + 1
    if _d.size() > 10485760 then
      _d.done()
      _d.reserve_current(10485760)
    end

class iso _WriterU32BE is MicroBenchmark
  // Benchmark writing Big Endian U32
  let _d: Writer = _d.create() .> reserve_current(10485760)
  var _i: U32 = 0

  fun name(): String =>
    "_WriterU32BE"

  fun ref apply() =>
    DoNotOptimise[None](BigEndianEncoder.u32(_d, _i))
    DoNotOptimise.observe()
    _i = _i + 1
    if _d.size() > 10485760 then
      _d.done()
      _d.reserve_current(10485760)
    end

class iso _WriterI32BE is MicroBenchmark
  // Benchmark writing Big Endian I32
  let _d: Writer = _d.create() .> reserve_current(10485760)
  var _i: I32 = 0

  fun name(): String =>
    "_WriterI32BE"

  fun ref apply() =>
    DoNotOptimise[None](BigEndianEncoder.i32(_d, _i))
    DoNotOptimise.observe()
    _i = _i + 1
    if _d.size() > 10485760 then
      _d.done()
      _d.reserve_current(10485760)
    end

class iso _WriterF32BE is MicroBenchmark
  // Benchmark writing Big Endian F32
  let _d: Writer = _d.create() .> reserve_current(10485760)
  var _i: F32 = 0

  fun name(): String =>
    "_WriterF32BE"

  fun ref apply() =>
    DoNotOptimise[None](BigEndianEncoder.f32(_d, _i))
    DoNotOptimise.observe()
    _i = _i + 1
    if _d.size() > 10485760 then
      _d.done()
      _d.reserve_current(10485760)
    end

class iso _WriterU64BE is MicroBenchmark
  // Benchmark writing Big Endian U64
  let _d: Writer = _d.create() .> reserve_current(10485760)
  var _i: U64 = 0

  fun name(): String =>
    "_WriterU64BE"

  fun ref apply() =>
    DoNotOptimise[None](BigEndianEncoder.u64(_d, _i))
    DoNotOptimise.observe()
    _i = _i + 1
    if _d.size() > 10485760 then
      _d.done()
      _d.reserve_current(10485760)
    end

class iso _WriterI64BE is MicroBenchmark
  // Benchmark writing Big Endian I64
  let _d: Writer = _d.create() .> reserve_current(10485760)
  var _i: I64 = 0

  fun name(): String =>
    "_WriterI64BE"

  fun ref apply() =>
    DoNotOptimise[None](BigEndianEncoder.i64(_d, _i))
    DoNotOptimise.observe()
    _i = _i + 1
    if _d.size() > 10485760 then
      _d.done()
      _d.reserve_current(10485760)
    end

class iso _WriterF64BE is MicroBenchmark
  // Benchmark writing Big Endian F64
  let _d: Writer = _d.create() .> reserve_current(10485760)
  var _i: F64 = 0

  fun name(): String =>
    "_WriterF64BE"

  fun ref apply() =>
    DoNotOptimise[None](BigEndianEncoder.f64(_d, _i))
    DoNotOptimise.observe()
    _i = _i + 1
    if _d.size() > 10485760 then
      _d.done()
      _d.reserve_current(10485760)
    end

class iso _WriterU128BE is MicroBenchmark
  // Benchmark writing Big Endian U128
  let _d: Writer = _d.create() .> reserve_current(10485760)
  var _i: U128 = 0

  fun name(): String =>
    "_WriterU128BE"

  fun ref apply() =>
    DoNotOptimise[None](BigEndianEncoder.u128(_d, _i))
    DoNotOptimise.observe()
    _i = _i + 1
    if _d.size() > 10485760 then
      _d.done()
      _d.reserve_current(10485760)
    end

class iso _WriterI128BE is MicroBenchmark
  // Benchmark writing Big Endian I128
  let _d: Writer = _d.create() .> reserve_current(10485760)
  var _i: I128 = 0

  fun name(): String =>
    "_WriterI128BE"

  fun ref apply() =>
    DoNotOptimise[None](BigEndianEncoder.i128(_d, _i))
    DoNotOptimise.observe()
    _i = _i + 1
    if _d.size() > 10485760 then
      _d.done()
      _d.reserve_current(10485760)
    end
