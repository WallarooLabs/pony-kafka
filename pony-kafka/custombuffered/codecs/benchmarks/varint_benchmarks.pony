use ".."
use "../.."
use "ponybench"
use "collections"

class iso _IsoReaderU8VarInt is MicroBenchmark
  // Benchmark reading VarInt Endian U8
  var _a: Array[U8] val = recover _a.create() end
  let _d: IsoReader = _d.create()

  new iso create() =>
    let wb: Writer = Writer
    var  i: U8 = 0
    while wb.size() < 10485760 do
      VarIntEncoder.u8(wb, i)
      i = i + 1
    end
    let bf = wb.done()
    try
      _a = bf(0)? as Array[U8] val
      _d.append(recover _a.clone() end)
    end

  fun name(): String =>
    "_IsoReaderU8VarInt"

  fun ref apply()? =>
    DoNotOptimise[U8](VarIntDecoder.u8(_d)?)
    DoNotOptimise.observe()
    if _d.size() < 128 then
      _d.clear()
      _d.append(recover _a.clone() end)
    end

class iso _IsoReaderI8VarInt is MicroBenchmark
  // Benchmark reading VarInt Endian I8
  var _a: Array[U8] val = recover _a.create() end
  let _d: IsoReader = _d.create()

  new iso create() =>
    let wb: Writer = Writer
    var  i: I8 = 0
    while wb.size() < 10485760 do
      VarIntEncoder.i8(wb, i)
      i = i + 1
    end
    let bf = wb.done()
    try
      _a = bf(0)? as Array[U8] val
      _d.append(recover _a.clone() end)
    end

  fun name(): String =>
    "_IsoReaderI8VarInt"

  fun ref apply()? =>
    DoNotOptimise[I8](VarIntDecoder.i8(_d)?)
    DoNotOptimise.observe()
    if _d.size() < 128 then
      _d.clear()
      _d.append(recover _a.clone() end)
    end

class iso _IsoReaderBoolVarInt is MicroBenchmark
  // Benchmark reading VarInt Endian Boolean
  var _a: Array[U8] val = recover _a.create() end
  let _d: IsoReader = _d.create()

  new iso create() =>
    let wb: Writer = Writer
    var  i: U8 = 0
    while wb.size() < 10485760 do
      VarIntEncoder.bool(wb, (i % 2) == 0)
      i = i + 1
    end
    let bf = wb.done()
    try
      _a = bf(0)? as Array[U8] val
      _d.append(recover _a.clone() end)
    end

  fun name(): String =>
    "_IsoReaderBoolVarInt"

  fun ref apply()? =>
    DoNotOptimise[Bool](VarIntDecoder.bool(_d)?)
    DoNotOptimise.observe()
    if _d.size() < 128 then
      _d.clear()
      _d.append(recover _a.clone() end)
    end

class iso _IsoReaderU16VarInt is MicroBenchmark
  // Benchmark reading VarInt Endian U16
  var _a: Array[U8] val = recover _a.create() end
  let _d: IsoReader = _d.create()

  new iso create() =>
    let wb: Writer = Writer
    var  i: U16 = 0
    while wb.size() < 10485760 do
      VarIntEncoder.u16(wb, i)
      i = i + 1
    end
    let bf = wb.done()
    try
      _a = bf(0)? as Array[U8] val
      _d.append(recover _a.clone() end)
    end

  fun name(): String =>
    "_IsoReaderU16VarInt"

  fun ref apply()? =>
    DoNotOptimise[U16](VarIntDecoder.u16(_d)?)
    DoNotOptimise.observe()
    if _d.size() < 128 then
      _d.clear()
      _d.append(recover _a.clone() end)
    end

class iso _IsoReaderI16VarInt is MicroBenchmark
  // Benchmark reading VarInt Endian I16
  var _a: Array[U8] val = recover _a.create() end
  let _d: IsoReader = _d.create()

  new iso create() =>
    let wb: Writer = Writer
    var  i: I16 = 0
    while wb.size() < 10485760 do
      VarIntEncoder.i16(wb, i)
      i = i + 1
    end
    let bf = wb.done()
    try
      _a = bf(0)? as Array[U8] val
      _d.append(recover _a.clone() end)
    end

  fun name(): String =>
    "_IsoReaderI16VarInt"

  fun ref apply()? =>
    DoNotOptimise[I16](VarIntDecoder.i16(_d)?)
    DoNotOptimise.observe()
    if _d.size() < 128 then
      _d.clear()
      _d.append(recover _a.clone() end)
    end

class iso _IsoReaderU32VarInt is MicroBenchmark
  // Benchmark reading VarInt Endian U32
  var _a: Array[U8] val = recover _a.create() end
  let _d: IsoReader = _d.create()

  new iso create() =>
    let wb: Writer = Writer
    var  i: U32 = 0
    while wb.size() < 10485760 do
      VarIntEncoder.u32(wb, i)
      i = i + 1
    end
    let bf = wb.done()
    try
      _a = bf(0)? as Array[U8] val
      _d.append(recover _a.clone() end)
    end

  fun name(): String =>
    "_IsoReaderU32VarInt"

  fun ref apply()? =>
    DoNotOptimise[U32](VarIntDecoder.u32(_d)?)
    DoNotOptimise.observe()
    if _d.size() < 128 then
      _d.clear()
      _d.append(recover _a.clone() end)
    end

class iso _IsoReaderI32VarInt is MicroBenchmark
  // Benchmark reading VarInt Endian I32
  var _a: Array[U8] val = recover _a.create() end
  let _d: IsoReader = _d.create()

  new iso create() =>
    let wb: Writer = Writer
    var  i: I32 = 0
    while wb.size() < 10485760 do
      VarIntEncoder.i32(wb, i)
      i = i + 1
    end
    let bf = wb.done()
    try
      _a = bf(0)? as Array[U8] val
      _d.append(recover _a.clone() end)
    end

  fun name(): String =>
    "_IsoReaderI32VarInt"

  fun ref apply()? =>
    DoNotOptimise[I32](VarIntDecoder.i32(_d)?)
    DoNotOptimise.observe()
    if _d.size() < 128 then
      _d.clear()
      _d.append(recover _a.clone() end)
    end

class iso _IsoReaderU64VarInt is MicroBenchmark
  // Benchmark reading VarInt Endian U64
  var _a: Array[U8] val = recover _a.create() end
  let _d: IsoReader = _d.create()

  new iso create() =>
    let wb: Writer = Writer
    var  i: U64 = 0
    while wb.size() < 10485760 do
      VarIntEncoder.u64(wb, i)
      i = i + 1
    end
    let bf = wb.done()
    try
      _a = bf(0)? as Array[U8] val
      _d.append(recover _a.clone() end)
    end

  fun name(): String =>
    "_IsoReaderU64VarInt"

  fun ref apply()? =>
    DoNotOptimise[U64](VarIntDecoder.u64(_d)?)
    DoNotOptimise.observe()
    if _d.size() < 128 then
      _d.clear()
      _d.append(recover _a.clone() end)
    end

class iso _IsoReaderI64VarInt is MicroBenchmark
  // Benchmark reading VarInt Endian I64
  var _a: Array[U8] val = recover _a.create() end
  let _d: IsoReader = _d.create()

  new iso create() =>
    let wb: Writer = Writer
    var  i: I64 = 0
    while wb.size() < 10485760 do
      VarIntEncoder.i64(wb, i)
      i = i + 1
    end
    let bf = wb.done()
    try
      _a = bf(0)? as Array[U8] val
      _d.append(recover _a.clone() end)
    end

  fun name(): String =>
    "_IsoReaderI64VarInt"

  fun ref apply()? =>
    DoNotOptimise[I64](VarIntDecoder.i64(_d)?)
    DoNotOptimise.observe()
    if _d.size() < 128 then
      _d.clear()
      _d.append(recover _a.clone() end)
    end

class iso _ValReaderU8VarInt is MicroBenchmark
  // Benchmark reading U8
  let _d: ValReader = _d.create()

  new iso create() =>
    let wb: Writer = Writer
    var  i: U8 = 0
    while wb.size() < 10485760 do
      VarIntEncoder.u8(wb, i)
      i = i + 1
    end
    let bf = wb.done()
    try
      let a = bf(0)? as Array[U8] val
      _d.append(a)
    end

  fun name(): String =>
    "_ValReaderU8VarInt"

  fun ref apply()? =>
    DoNotOptimise[U8](VarIntDecoder.u8(_d)?)
    DoNotOptimise.observe()
    if _d.size() < 128 then
      _d.set_position(0)?
    end

class iso _ValReaderI8VarInt is MicroBenchmark
  // Benchmark reading I8
  let _d: ValReader = _d.create()

  new iso create() =>
    let wb: Writer = Writer
    var  i: I8 = 0
    while wb.size() < 10485760 do
      VarIntEncoder.i8(wb, i)
      i = i + 1
    end
    let bf = wb.done()
    try
      let a = bf(0)? as Array[U8] val
      _d.append(a)
    end

  fun name(): String =>
    "_ValReaderI8VarInt"

  fun ref apply()? =>
    DoNotOptimise[I8](VarIntDecoder.i8(_d)?)
    DoNotOptimise.observe()
    if _d.size() < 128 then
      _d.set_position(0)?
    end

class iso _ValReaderBoolVarInt is MicroBenchmark
  // Benchmark reading Boolean
  let _d: ValReader = _d.create()

  new iso create() =>
    let wb: Writer = Writer
    var  i: U8 = 0
    while wb.size() < 10485760 do
      VarIntEncoder.bool(wb, (i % 2) == 0)
      i = i + 1
    end
    let bf = wb.done()
    try
      let a = bf(0)? as Array[U8] val
      _d.append(a)
    end

  fun name(): String =>
    "_ValReaderBoolVarInt"

  fun ref apply()? =>
    DoNotOptimise[Bool](VarIntDecoder.bool(_d)?)
    DoNotOptimise.observe()
    if _d.size() < 128 then
      _d.set_position(0)?
    end

class iso _ValReaderU16VarInt is MicroBenchmark
  // Benchmark reading VarInt Endian U16
  let _d: ValReader = _d.create()

  new iso create() =>
    let wb: Writer = Writer
    var  i: U16 = 0
    while wb.size() < 10485760 do
      VarIntEncoder.u16(wb, i)
      i = i + 1
    end
    let bf = wb.done()
    try
      let a = bf(0)? as Array[U8] val
      _d.append(a)
    end

  fun name(): String =>
    "_ValReaderU16VarInt"

  fun ref apply()? =>
    DoNotOptimise[U16](VarIntDecoder.u16(_d)?)
    DoNotOptimise.observe()
    if _d.size() < 128 then
      _d.set_position(0)?
    end

class iso _ValReaderI16VarInt is MicroBenchmark
  // Benchmark reading VarInt Endian I16
  let _d: ValReader = _d.create()

  new iso create() =>
    let wb: Writer = Writer
    var  i: I16 = 0
    while wb.size() < 10485760 do
      VarIntEncoder.i16(wb, i)
      i = i + 1
    end
    let bf = wb.done()
    try
      let a = bf(0)? as Array[U8] val
      _d.append(a)
    end

  fun name(): String =>
    "_ValReaderI16VarInt"

  fun ref apply()? =>
    DoNotOptimise[I16](VarIntDecoder.i16(_d)?)
    DoNotOptimise.observe()
    if _d.size() < 128 then
      _d.set_position(0)?
    end

class iso _ValReaderU32VarInt is MicroBenchmark
  // Benchmark reading VarInt Endian U32
  let _d: ValReader = _d.create()

  new iso create() =>
    let wb: Writer = Writer
    var  i: U32 = 0
    while wb.size() < 10485760 do
      VarIntEncoder.u32(wb, i)
      i = i + 1
    end
    let bf = wb.done()
    try
      let a = bf(0)? as Array[U8] val
      _d.append(a)
    end

  fun name(): String =>
    "_ValReaderU32VarInt"

  fun ref apply()? =>
    DoNotOptimise[U32](VarIntDecoder.u32(_d)?)
    DoNotOptimise.observe()
    if _d.size() < 128 then
      _d.set_position(0)?
    end

class iso _ValReaderI32VarInt is MicroBenchmark
  // Benchmark reading VarInt Endian I32
  let _d: ValReader = _d.create()

  new iso create() =>
    let wb: Writer = Writer
    var  i: I32 = 0
    while wb.size() < 10485760 do
      VarIntEncoder.i32(wb, i)
      i = i + 1
    end
    let bf = wb.done()
    try
      let a = bf(0)? as Array[U8] val
      _d.append(a)
    end

  fun name(): String =>
    "_ValReaderI32VarInt"

  fun ref apply()? =>
    DoNotOptimise[I32](VarIntDecoder.i32(_d)?)
    DoNotOptimise.observe()
    if _d.size() < 128 then
      _d.set_position(0)?
    end

class iso _ValReaderU64VarInt is MicroBenchmark
  // Benchmark reading VarInt Endian U64
  let _d: ValReader = _d.create()

  new iso create() =>
    let wb: Writer = Writer
    var  i: U64 = 0
    while wb.size() < 10485760 do
      VarIntEncoder.u64(wb, i)
      i = i + 1
    end
    let bf = wb.done()
    try
      let a = bf(0)? as Array[U8] val
      _d.append(a)
    end

  fun name(): String =>
    "_ValReaderU64VarInt"

  fun ref apply()? =>
    DoNotOptimise[U64](VarIntDecoder.u64(_d)?)
    DoNotOptimise.observe()
    if _d.size() < 128 then
      _d.set_position(0)?
    end

class iso _ValReaderI64VarInt is MicroBenchmark
  // Benchmark reading VarInt Endian I64
  let _d: ValReader = _d.create()

  new iso create() =>
    let wb: Writer = Writer
    var  i: I64 = 0
    while wb.size() < 10485760 do
      VarIntEncoder.i64(wb, i)
      i = i + 1
    end
    let bf = wb.done()
    try
      let a = bf(0)? as Array[U8] val
      _d.append(a)
    end

  fun name(): String =>
    "_ValReaderI64VarInt"

  fun ref apply()? =>
    DoNotOptimise[I64](VarIntDecoder.i64(_d)?)
    DoNotOptimise.observe()
    if _d.size() < 128 then
      _d.set_position(0)?
    end

class iso _WriterU8VarInt is MicroBenchmark
  // Benchmark writing VarInt Endian U8
  let _d: Writer = _d.create() .> reserve_current(10485760)
  var _i: U8 = 0

  fun name(): String =>
    "_WriterU8VarInt"

  fun ref apply() =>
    DoNotOptimise[None](VarIntEncoder.u8(_d, _i))
    DoNotOptimise.observe()
    _i = _i + 1
    if _d.size() > 10485760 then
      _d.done()
      _d.reserve_current(10485760)
    end

class iso _WriterI8VarInt is MicroBenchmark
  // Benchmark writing VarInt Endian I8
  let _d: Writer = _d.create() .> reserve_current(10485760)
  var _i: I8 = 0

  fun name(): String =>
    "_WriterI8VarInt"

  fun ref apply() =>
    DoNotOptimise[None](VarIntEncoder.i8(_d, _i))
    DoNotOptimise.observe()
    _i = _i + 1
    if _d.size() > 10485760 then
      _d.done()
      _d.reserve_current(10485760)
    end

class iso _WriterBoolVarInt is MicroBenchmark
  // Benchmark writing VarInt Endian Boolean
  let _d: Writer = _d.create() .> reserve_current(10485760)
  var _i: U8 = 0

  fun name(): String =>
    "_WriterBoolVarInt"

  fun ref apply() =>
    DoNotOptimise[None](VarIntEncoder.bool(_d, (_i % 2) == 0))
    DoNotOptimise.observe()
    _i = _i + 1
    if _d.size() > 10485760 then
      _d.done()
      _d.reserve_current(10485760)
    end

class iso _WriterU16VarInt is MicroBenchmark
  // Benchmark writing VarInt Endian U16
  let _d: Writer = _d.create() .> reserve_current(10485760)
  var _i: U16 = 0

  fun name(): String =>
    "_WriterU16VarInt"

  fun ref apply() =>
    DoNotOptimise[None](VarIntEncoder.u16(_d, _i))
    DoNotOptimise.observe()
    _i = _i + 1
    if _d.size() > 10485760 then
      _d.done()
      _d.reserve_current(10485760)
    end

class iso _WriterI16VarInt is MicroBenchmark
  // Benchmark writing VarInt Endian I16
  let _d: Writer = _d.create() .> reserve_current(10485760)
  var _i: I16 = 0

  fun name(): String =>
    "_WriterI16VarInt"

  fun ref apply() =>
    DoNotOptimise[None](VarIntEncoder.i16(_d, _i))
    DoNotOptimise.observe()
    _i = _i + 1
    if _d.size() > 10485760 then
      _d.done()
      _d.reserve_current(10485760)
    end

class iso _WriterU32VarInt is MicroBenchmark
  // Benchmark writing VarInt Endian U32
  let _d: Writer = _d.create() .> reserve_current(10485760)
  var _i: U32 = 0

  fun name(): String =>
    "_WriterU32VarInt"

  fun ref apply() =>
    DoNotOptimise[None](VarIntEncoder.u32(_d, _i))
    DoNotOptimise.observe()
    _i = _i + 1
    if _d.size() > 10485760 then
      _d.done()
      _d.reserve_current(10485760)
    end

class iso _WriterI32VarInt is MicroBenchmark
  // Benchmark writing VarInt Endian I32
  let _d: Writer = _d.create() .> reserve_current(10485760)
  var _i: I32 = 0

  fun name(): String =>
    "_WriterI32VarInt"

  fun ref apply() =>
    DoNotOptimise[None](VarIntEncoder.i32(_d, _i))
    DoNotOptimise.observe()
    _i = _i + 1
    if _d.size() > 10485760 then
      _d.done()
      _d.reserve_current(10485760)
    end

class iso _WriterU64VarInt is MicroBenchmark
  // Benchmark writing VarInt Endian U64
  let _d: Writer = _d.create() .> reserve_current(10485760)
  var _i: U64 = 0

  fun name(): String =>
    "_WriterU64VarInt"

  fun ref apply() =>
    DoNotOptimise[None](VarIntEncoder.u64(_d, _i))
    DoNotOptimise.observe()
    _i = _i + 1
    if _d.size() > 10485760 then
      _d.done()
      _d.reserve_current(10485760)
    end

class iso _WriterI64VarInt is MicroBenchmark
  // Benchmark writing VarInt Endian I64
  let _d: Writer = _d.create() .> reserve_current(10485760)
  var _i: I64 = 0

  fun name(): String =>
    "_WriterI64VarInt"

  fun ref apply() =>
    DoNotOptimise[None](VarIntEncoder.i64(_d, _i))
    DoNotOptimise.observe()
    _i = _i + 1
    if _d.size() > 10485760 then
      _d.done()
      _d.reserve_current(10485760)
    end
