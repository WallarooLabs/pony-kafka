/*

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

*/

use "../customlogger"
use "lib:z"

use @crc32[USize](crc: USize, buf: Pointer[U8] tag, len: U32)
use @deflateInit2_[I32](zstream: MaybePointer[_ZStream], level: I32,
  method: I32, window_bits: I32, mem_level: I32, strategy: I32,
  zlib_version: Pointer[U8] tag, size_zstream: I32)
use @deflateBound[ULong](zstream: MaybePointer[_ZStream], source_len: ULong)
use @deflate[I32](zstream: MaybePointer[_ZStream], flush: I32)
use @deflateEnd[I32](zstream: MaybePointer[_ZStream])
use @inflateInit2_[I32](zstream: MaybePointer[_ZStream], window_bits: I32,
  zlib_version: Pointer[U8] tag, size_zstream: I32)
use @inflateGetHeader[I32](zstream: MaybePointer[_ZStream],
  gz_header: MaybePointer[_GZHeader])
use @inflateEnd[I32](zstream: MaybePointer[_ZStream])
use @inflate[I32](zstream: MaybePointer[_ZStream], flush: I32)

struct _ZStream
  var next_in: Pointer[U8] tag = Pointer[U8] // next input byte
  var avail_in: U32 = 0 // number of bytes available at next input
  var total_in: ULong = 0 // total number of input bytes read so far
  var next_out: Pointer[U8] tag = Pointer[U8] // next output byte will go here
  var avail_out: U32 = 0 // remaining free space at next_out
  var total_out: ULong = 0 // total number of bytes output so far
  var msg: Pointer[U8] = Pointer[U8] // last error message, NULL if no error
  // internal state; not visible by applications
  var state: Pointer[U8] tag = Pointer[U8]
  // used to allocate the internal state
  var alloc_fn: Pointer[U8] tag = Pointer[U8]
  var free_fn: Pointer[U8] tag = Pointer[U8] // used to free the internal state
  // private data object passed to zalloc and zfree
  var opaque: Pointer[U8] tag = Pointer[U8]
  // best guess about the data type: binary or text for deflate, or the decoding
  // state for inflate
  var data_type: I32 = 0
  var adler: ULong = 0 // Adler-32 or CRC-32 value of the uncompressed data
  var reserved: ULong = 0 // reserved for future use

  new create() => None

  fun struct_size_bytes(): I32 =>
    ifdef ilp32 or llp64 then
      4 + 4 + 4 + 4 + 4 + 4 + 4 + 4 + 4 + 4 + 4 + 4 + 4 + 4
    else
      8 + 8 + 8 + 8 + 8 + 8 + 8 + 8 + 8 + 8 + 8 + 8 + 8 + 8
    end

struct _GZHeader
  var text: I32 = 0 // true if compressed data believed to be text
  var time: ULong = 0 // modification time
  var xflags: I32 = 0 // extra flags (not used when writing a gzip file)
  var os: I32 = 0 // operating system
  // pointer to extra field or Z_NULL if none
  var extra: Pointer[U8] = Pointer[U8]
  var extra_len: U32 = 0 // extra field length (valid if extra != Z_NULL)
  var extra_max: U32 = 0 // space at extra (only when reading header)
  // pointer to zero-terminated file name or Z_NULL
  var name: Pointer[U8] = Pointer[U8]
  var name_max: U32 = 0 // space at name (only when reading header)
  // pointer to zero-terminated comment or Z_NULL
  var comment: Pointer[U8] = Pointer[U8]
  var comm_max: U32 = 0 // space at comment (only when reading header)
  var hcrc: I32 = 0 // true if there was or will be a header crc
  // true when done reading gzip header (not used when writing a gzip file)
  var done: I32 = 0

  new create() => None

// compression levels
primitive ZNoCompression
  fun apply(): I32 => 0

primitive ZBestSpeed
  fun apply(): I32 => 1

primitive ZBestCompression
  fun apply(): I32 => 9

primitive ZDefaultCompression
  fun apply(): I32 => -1

// compression method
primitive ZDeflated
  fun apply(): I32 => 8

// null value
primitive ZNull
  fun apply(): I32 => 0


// compression strategies
primitive ZDefaultStrategy
  fun apply(): I32 => 0

primitive ZFiltered
  fun apply(): I32 => 1

primitive ZHuffmanOnly
  fun apply(): I32 => 2

primitive ZRle
  fun apply(): I32 => 3

primitive ZFixed
  fun apply(): I32 => 4

// possible data_type values
primitive ZBinary
  fun apply(): I32 => 0

primitive ZText
  fun apply(): I32 => 1

primitive ZUnknown
  fun apply(): I32 => 2

// possible flush values
primitive ZNoFlush
  fun apply(): I32 => 0

primitive ZPartialFlush
  fun apply(): I32 => 1

primitive ZSyncFlush
  fun apply(): I32 => 2

primitive ZFullFlush
  fun apply(): I32 => 3

primitive ZFinish
  fun apply(): I32 => 4

primitive ZBlock
  fun apply(): I32 => 5

primitive ZTrees
  fun apply(): I32 => 6

// possible return codes
primitive ZOk
  fun apply(): I32 => 0

primitive ZStreamEnd
  fun apply(): I32 => 1

primitive ZNeedDict
  fun apply(): I32 => 2

primitive ZErrno
  fun apply(): I32 => -1

primitive ZStreamError
  fun apply(): I32 => -2

primitive ZDataError
  fun apply(): I32 => -3

primitive ZMemError
  fun apply(): I32 => -4

primitive ZBufError
  fun apply(): I32 => -5

primitive ZVersionError
  fun apply(): I32 => -6

primitive Crc32
  fun crc32(data: ByteSeq box): USize =>
    @crc32(crc32_init(), data.cpointer(), data.size().u32())

  fun crc32_init(): USize =>
    @crc32(USize(0), Pointer[U8], U32(0))

  fun crc32_array(data: Array[ByteSeq] box): USize =>
    var crc = crc32_init()
    for d in data.values() do
      crc = @crc32(crc, d.cpointer(), d.size().u32())
    end

    crc

primitive ZlibCompressor
  fun compress(logger: Logger[String], data: ByteSeq): Array[U8] iso^ ? =>
    let zlib = Zlib.compressor(logger where window_bits = 15+16)?
    zlib.compress_array(recover val [data] end, data.size())?

  fun compress_array(logger: Logger[String], data: Array[ByteSeq] val,
    total_size: USize): Array[U8] iso^ ?
  =>
    let zlib = Zlib.compressor(logger where window_bits = 15+16)?
    zlib.compress_array(data, total_size)?

primitive ZlibDecompressor
  fun decompress(logger: Logger[String], data: ByteSeq): Array[U8] iso^ ? =>
    let zlib = Zlib.decompressor(logger where window_bits = 15+32)
    zlib.decompress(data)?

class Zlib
  var _stream: _ZStream = _ZStream
  var _stream_p: MaybePointer[_ZStream]
  let _logger: Logger[String]
  let _window_bits: I32

  let zlib_version: String = "1.2.8"

  new ref compressor(logger: Logger[String], level: I32 = ZDefaultCompression(),
    method: I32 = ZDeflated(), window_bits: I32 = 15, mem_level: I32 = 8,
    strategy: I32 = ZDefaultStrategy()) ?
  =>
    _logger = logger
    _stream_p = MaybePointer[_ZStream](_stream)
    _window_bits = window_bits

    let err = @deflateInit2_(_stream_p, level, method, window_bits, mem_level,
      strategy, zlib_version.cstring(), _stream.struct_size_bytes())

    if err != ZOk() then
      _check_error(err)?
    end

  new ref decompressor(logger: Logger[String], window_bits: I32 = 15) =>
    _logger = logger
    _stream_p = MaybePointer[_ZStream](_stream)
    _window_bits = window_bits


  fun ref _check_error(err: I32) ? =>
    // TODO: Any way to do this without copying the string? Doesn't seem
    // possible because GC will try and free data in pointer
    let err_str = String.copy_cstring(_stream.msg)
    match err
    | ZOk() =>
      _logger(Error) and
        _logger.log(Error, "Encountered ZOk (" + err.string() + ").")
    | ZStreamEnd() =>
      _logger(Error) and
        _logger.log(Error, "Zlib reached stream end (" + err.string() + ")!")
    | ZNeedDict() =>
      _logger(Error) and
        _logger.log(Error, "Zlib needs dictionary (" + err.string() +
        ")! zlib error message: " + err_str)
    | ZErrno() =>
      _logger(Error) and
        _logger.log(Error, "Zlib encountered error (" + err.string() +
        ")! zlib error message: " + err_str)
    | ZStreamError() =>
      _logger(Error) and
        _logger.log(Error, "Zlib encountered stream error (" + err.string() +
        ")! zlib error message: " + err_str)
    | ZDataError() =>
      _logger(Error) and
        _logger.log(Error, "Zlib encountered data error (" + err.string() +
        ")! zlib error message: " + err_str)
    | ZMemError() =>
      _logger(Error) and
        _logger.log(Error, "Zlib encountered memory error (" + err.string() +
        ")! zlib error message: " + err_str)
    | ZBufError() =>
      _logger(Error) and
        _logger.log(Error, "Zlib encountered buffer error (" + err.string() +
        ")! zlib error message: " + err_str)
    | ZVersionError() =>
      _logger(Error) and
        _logger.log(Error, "Zlib encountered version error (" + err.string() +
        ")! zlib error message: " + err_str + ", zlib version: " +
        String.copy_cstring(@zlibVersion[Pointer[U8]]()))
    else
      _logger(Error) and
        _logger.log(Error, "Zlib encountered unknown error ( " + err.string() +
        ")! zlib error message: " + err_str)
    end

    error

  // based on https://github.com/edenhill/librdkafka/blob/master/src/rdgz.c
  fun ref decompress(data: ByteSeq, decompressed_size: (USize | None) = None):
    Array[U8] iso^ ?
  =>
    let buffer_size = match decompressed_size
    | None => calculate_decompressed_size(data)?
    | let x: USize => x
    end

    _stream = _ZStream
    _stream_p = MaybePointer[_ZStream](_stream)
    let hdr = _GZHeader
    let hdr_p = MaybePointer[_GZHeader](hdr)

    var err = @inflateInit2_(_stream_p, _window_bits, zlib_version.cstring(),
      _stream.struct_size_bytes())

    if err != ZOk() then
      _check_error(err)?
    end

    let buffer = recover Array[U8](buffer_size) end
    buffer.undefined(buffer.space())

    _stream.next_in = data.cpointer()
    _stream.avail_in = data.size().u32()

    err = @inflateGetHeader(_stream_p, hdr_p)
    try
      if err != ZOk() then
        _check_error(err)?
      end
    else
      // clean up zlib internal state and end deflate stream
      @inflateEnd(_stream_p)
      error
    end

    _stream.next_out = buffer.cpointer()
    _stream.avail_out = buffer.size().u32()

    err = @inflate(_stream_p, ZFinish())
    try
      if err != ZStreamEnd() then
        _check_error(err)?
      end
      if _stream.avail_in != 0 then
        _logger(Error) and _logger.log(Error,
          "Zlib inflate didn't read all input!")
        error
      end
    else
      // clean up zlib internal state and end deflate stream
      @inflateEnd(_stream_p)
      error
    end

    buffer.truncate(_stream.total_out.usize())

    // clean up zlib internal state and end deflate stream
    @inflateEnd(_stream_p)

    buffer


  fun ref calculate_decompressed_size(data: ByteSeq): USize ? =>
    _stream_p = MaybePointer[_ZStream](_stream)
    let hdr = _GZHeader
    let hdr_p = MaybePointer[_GZHeader](hdr)

    var err = @inflateInit2_(_stream_p, _window_bits, zlib_version.cstring(),
      _stream.struct_size_bytes())

    if err != ZOk() then
      _check_error(err)?
    end

    let buffer_size: USize = 512
    let buffer = recover Array[U8](buffer_size) end
    buffer.undefined(buffer.space())

    _stream.next_in = data.cpointer()
    _stream.avail_in = data.size().u32()

    var len: USize = 0

    err = @inflateGetHeader(_stream_p, hdr_p)
    try
      if err != ZOk() then
        _check_error(err)?
      end
    else
      // clean up zlib internal state and end deflate stream
      @inflateEnd(_stream_p)
      error
    end

    var p = buffer.cpointer()
    var p_size = buffer.size()

    repeat
      _stream.next_out = p
      _stream.avail_out = p_size.u32()

      err = @inflate(_stream_p, ZNoFlush())
      try
        if (err != ZOk()) and (err != ZStreamEnd()) and
          (err != ZBufError()) then
          _check_error(err)?
        end
      else
        // clean up zlib internal state and end deflate stream
        @inflateEnd(_stream_p)
        error
      end

    until err == ZStreamEnd() end

    len = _stream.total_out.usize()

    // clean up zlib internal state and end deflate stream
    @inflateEnd(_stream_p)

    len


  // based on
  // https://github.com/edenhill/librdkafka/blob/master/src/rdkafka_msgset_writer.c#L723
  fun ref compress(data: ByteSeq): Array[U8] iso^ ? =>
    compress_array(recover val [data] end, data.size())?

  fun ref compress_array(data: Array[ByteSeq] val, total_size: USize):
    Array[U8] iso^ ?
  =>
    let max_len = @deflateBound(_stream_p, total_size.ulong()).u32()
    let buffer = recover Array[U8](max_len.usize()) end
    buffer.undefined(buffer.space())
    _stream.next_out = buffer.cpointer()
    _stream.avail_out = buffer.size().u32()

    for d in data.values() do
      _stream.next_in = d.cpointer()
      _stream.avail_in = d.size().u32()
      let err = @deflate(_stream_p, ZNoFlush())
      try
        if err != ZOk() then
          _check_error(err)?
        end

        if _stream.avail_in != 0 then
          _logger(Error) and _logger.log(Error,
            "Zlib deflate didn't read all input!")
          error
        end
      else
        // clean up zlib internal state and end deflate stream
        @deflateEnd(_stream_p)
        error
      end
    end

    let err = @deflate(_stream_p, ZFinish())
    try
      if err != ZStreamEnd() then
        _check_error(err)?
      end
    else
      // clean up zlib internal state and end deflate stream
      @deflateEnd(_stream_p)
      error
    end

    buffer.truncate(_stream.total_out.usize())

    // clean up zlib internal state and end deflate stream
    @deflateEnd(_stream_p)

    buffer
