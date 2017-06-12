use "../customlogger"
use "lib:lz4"

use @LZ4F_compressBound[USize](src_size: USize, prefs_ptr: Pointer[U8])
use @LZ4F_isError[U32](err: LZ4FError)
use @LZ4F_getErrorName[Pointer[U8]](err: LZ4FError)
use @LZ4F_createCompressionContext[LZ4FError](cctx_p: Pointer[LZ4FCompressionContext], version: U32)
use @LZ4F_compressBegin[USize](cctx: LZ4FCompressionContext, buf: Pointer[U8] tag, buf_size: USize, pref_p: MaybePointer[LZ4FPreferences])
use @LZ4F_freeCompressionContext[LZ4FError](cctx: LZ4FCompressionContext)
use @LZ4F_createDecompressionContext[LZ4FError](dctx_p: Pointer[LZ4FDecompressionContext], version: U32)
use @LZ4F_getFrameInfo[LZ4FError](dctx: LZ4FDecompressionContext, frame_info: MaybePointer[LZ4FFrameInfo], buf: Pointer[U8] tag, buf_size: Pointer[USize])
use @LZ4F_freeDecompressionContext[LZ4FError](dctx: LZ4FDecompressionContext)

// TODO: Remove USize from union for buf pointer
use @LZ4F_compressUpdate[USize](cctx: LZ4FCompressionContext, buf: (USize | Pointer[U8] tag), buf_size: USize, src_buf: Pointer[U8] tag, src_size: USize, copts_p: MaybePointer[LZ4FCompressOptions])
use @LZ4F_compressEnd[USize](cctx: LZ4FCompressionContext, buf: (USize | Pointer[U8] tag), buf_size: USize, copts_p: MaybePointer[LZ4FCompressOptions])
use @LZ4F_decompress[USize](dctx: LZ4FDecompressionContext, buf: (USize | Pointer[U8] tag), buf_size: Pointer[USize], src_buf: (USize | Pointer[U8] tag), src_size: Pointer[USize], copts_p: MaybePointer[LZ4FDecompressOptions])

type LZ4FError is USize

primitive LZ4FVersion
  fun apply(): U32 => 100

primitive LZ4FBlockLinked
  fun apply(): U32 => 0

primitive LZ4FBlockIndependent
  fun apply(): U32 => 1

type LZ4FCompressionContext is Pointer[U8]
type LZ4FDecompressionContext is Pointer[U8]

struct LZ4FDecompressOptions
  var stable_dst: U32 = 0
  var reserved_1: U32 = 0
  var reserved_2: U32 = 0
  var reserved_3: U32 = 0

  new create() => None

struct LZ4FCompressOptions
  var stable_src: U32 = 0
  var reserved_1: U32 = 0
  var reserved_2: U32 = 0
  var reserved_3: U32 = 0

  new create() => None

struct LZ4FFrameInfo
  var block_size: U32 = 0
  var block_mode: U32 = 0
  var content_checksum: U32 = 0
  var frame_type: U32 = 0
  var content_size: U64 = 0
  var reserved_1: U32 = 0
  var reserved_2: U32 = 0

  new create() => None

struct LZ4FPreferences
  embed frame_info: LZ4FFrameInfo = LZ4FFrameInfo
  var compression_level: I32 = 0
  var auto_flush: U32 = 0
  var reserved_1: U32 = 0
  var reserved_2: U32 = 0
  var reserved_3: U32 = 0
  var reserved_4: U32 = 0

  new create() => None

primitive LZ4Compressor
  fun compress(logger: Logger[String], data: ByteSeq, prefs: LZ4FPreferences = LZ4FPreferences, copts: LZ4FCompressOptions = LZ4FCompressOptions): Array[U8] iso ? =>
    LZ4.compress_array(logger, recover val [data] end, data.size(), prefs, copts)

  fun compress_array(logger: Logger[String], data: Array[ByteSeq] val, total_size: USize, prefs: LZ4FPreferences = LZ4FPreferences, copts: LZ4FCompressOptions = LZ4FCompressOptions): Array[U8] iso ? =>
    LZ4.compress_array(logger, data, total_size, prefs, copts)

primitive LZ4Decompressor
  fun decompress(logger: Logger[String], data: ByteSeq, dopts: LZ4FDecompressOptions = LZ4FDecompressOptions): Array[U8] iso ? =>
    LZ4.decompress(logger, data, dopts)

primitive LZ4
  // based on https://github.com/edenhill/librdkafka/blob/master/src/rdkafka_lz4.c
  fun decompress(logger: Logger[String], data: ByteSeq, dopts: LZ4FDecompressOptions = LZ4FDecompressOptions): Array[U8] iso ? =>
    var dctx: LZ4FDecompressionContext = LZ4FDecompressionContext
    var fi: LZ4FFrameInfo = LZ4FFrameInfo
    var fi_p = MaybePointer[LZ4FFrameInfo](fi)
    var data_offset = data.size()

    var err = @LZ4F_createDecompressionContext(addressof dctx, LZ4FVersion())
    if @LZ4F_isError(err) != 0 then
      logger(Error) and logger.log(Error, "LZ4 couldn't create decompression context! LZ4 error message: " + String.copy_cstring(@LZ4F_getErrorName(err)))
      error
    end

    err = @LZ4F_getFrameInfo(dctx, fi_p, data.cpointer(), addressof data_offset)
    if @LZ4F_isError(err) != 0 then
      logger(Error) and logger.log(Error, "LZ4 couldn't read frame info! LZ4 error message: " + String.copy_cstring(@LZ4F_getErrorName(err)))
      @LZ4F_freeDecompressionContext(dctx)
      error
    end

    let out_size = if (fi.content_size == 0) or (fi.content_size.usize() > (data.size() * 255)) then
        data.size() * 255
      else
        fi.content_size.usize()
      end

    let buffer = recover Array[U8](out_size) end
    buffer.undefined(buffer.space())

    var buf_offset: USize = 0
    var buf_size: USize = buffer.size()
    var data_size: USize = data.size()

    var dopts_p = MaybePointer[LZ4FDecompressOptions](dopts)

    while data_offset < data.size() do
      buf_size = buffer.size() - buf_offset
      data_size = data.size() - data_offset

      err = @LZ4F_decompress(dctx, buffer.cpointer().usize() + buf_offset, addressof buf_size, data.cpointer().usize() + data_offset, addressof data_size, dopts_p)
      if @LZ4F_isError(err) != 0 then
        logger(Error) and logger.log(Error, "LZ4 couldn't decompression data! LZ4 error message: " + String.copy_cstring(@LZ4F_getErrorName(err)))
        @LZ4F_freeDecompressionContext(dctx)
        error
      end

      buf_offset = buf_offset + buf_size
      data_offset = data_offset + data_size

      if buf_offset > buffer.size() then
        logger(Error) and logger.log(Error, "LZ4 buffer offset is larger than buffer size! This should never happen!")
        @LZ4F_freeDecompressionContext(dctx)
        error
      end

      if data_offset > data.size() then
        logger(Error) and logger.log(Error, "LZ4 input data offset is larger than input data size! This should never happen!")
        @LZ4F_freeDecompressionContext(dctx)
        error
      end

      // done decompressing
      if err == 0 then
        break
      end

      if (err > 0) and (buf_offset == buffer.size()) then
        // grow buffer
        buffer.undefined(buffer.size()*2)
      end
    end

    if data_offset < data.size() then
      logger(Error) and logger.log(Error, "LZ4 didn't decompress all data!")
      @LZ4F_freeDecompressionContext(dctx)
      error
    end

    @LZ4F_freeDecompressionContext(dctx)

    buffer.truncate(buf_offset)

    buffer

  // based on https://github.com/edenhill/librdkafka/blob/master/src/rdkafka_lz4.c
  fun compress(logger: Logger[String], data: ByteSeq, prefs: LZ4FPreferences = LZ4FPreferences, copts: LZ4FCompressOptions = LZ4FCompressOptions): Array[U8] iso ? =>
    compress_array(logger, recover val [data] end, data.size(), prefs, copts)

  // TODO: Figure out appropriate way to hide Structs in MaybePointers that isn't exposed to users
  // based on https://github.com/edenhill/librdkafka/blob/master/src/rdkafka_lz4.c#L321
  fun compress_array(logger: Logger[String], data: Array[ByteSeq] val, total_size: USize, prefs: LZ4FPreferences = LZ4FPreferences, copts: LZ4FCompressOptions = LZ4FCompressOptions): Array[U8] iso ? =>
    var bytes_written: USize = 0
    let max_len = @LZ4F_compressBound(total_size, Pointer[U8])
    if @LZ4F_isError(max_len) != 0 then
      logger(Error) and logger.log(Error, "LZ4 couldn't determine output size for compression! LZ4 error message: " + String.copy_cstring(@LZ4F_getErrorName(max_len)))
      error
    end
    let buffer = recover Array[U8](max_len) end
    buffer.undefined(buffer.space())

    var cctx: LZ4FCompressionContext = LZ4FCompressionContext

    var err = @LZ4F_createCompressionContext(addressof cctx, LZ4FVersion())
    if @LZ4F_isError(err) != 0 then
      logger(Error) and logger.log(Error, "LZ4 couldn't create compression context! LZ4 error message: " + String.copy_cstring(@LZ4F_getErrorName(err)))
      error
    end

    var prefs_p = MaybePointer[LZ4FPreferences](prefs)

    err = @LZ4F_compressBegin(cctx, buffer.cpointer(), buffer.size(), prefs_p)
    if @LZ4F_isError(err) != 0 then
      logger(Error) and logger.log(Error, "LZ4 couldn't begin compression! LZ4 error message: " + String.copy_cstring(@LZ4F_getErrorName(err)))
      @LZ4F_freeCompressionContext(cctx)
      error
    end

    bytes_written = bytes_written + err

    var copts_p = MaybePointer[LZ4FCompressOptions](copts)

    for d in data.values() do
      err = @LZ4F_compressUpdate(cctx, buffer.cpointer().usize() + bytes_written, buffer.size() - bytes_written, d.cpointer(), d.size(), copts_p)
      if @LZ4F_isError(err) != 0 then
        logger(Error) and logger.log(Error, "LZ4 compression error! LZ4 error message: " + String.copy_cstring(@LZ4F_getErrorName(err)))
        @LZ4F_freeCompressionContext(cctx)
        error
      end
      bytes_written = bytes_written + err
    end

    err = @LZ4F_compressEnd(cctx, buffer.cpointer().usize() + bytes_written, buffer.size() - bytes_written, copts_p)
    if @LZ4F_isError(err) != 0 then
      logger(Error) and logger.log(Error, "LZ4 couldn't end compression! LZ4 error message: " + String.copy_cstring(@LZ4F_getErrorName(err)))
      @LZ4F_freeCompressionContext(cctx)
      error
    end

    bytes_written = bytes_written + err

    @LZ4F_freeCompressionContext(cctx)

    buffer.truncate(bytes_written)

    buffer
