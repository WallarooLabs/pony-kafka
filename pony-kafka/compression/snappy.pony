use "../customlogger"
use "lib:snappy"

use @snappy_validate_compressed_buffer[SnappyStatus](data: Pointer[U8] tag, size: USize)
use @snappy_uncompressed_length[SnappyStatus](data: (USize | Pointer[U8] tag), size: USize, len: Pointer[USize])
use @snappy_max_compressed_length[USize](uncompressed_size: USize)
use @snappy_uncompress[SnappyStatus](data: (USize | Pointer[U8] tag), size: USize, output: (USize | Pointer[U8] tag), output_size: Pointer[USize])
use @snappy_compress[SnappyStatus](data: Pointer[U8] tag, size: USize, output: (USize | Pointer[U8] tag), output_size: Pointer[USize])

type SnappyStatus is I32

primitive SnappyCompressor
  fun compress(logger: Logger[String], data: ByteSeq): Array[U8] iso ? =>
    Snappy.compress(logger, data)

  fun compress_array(logger: Logger[String], data: Array[ByteSeq] val, total_size: USize): Array[U8] iso ? =>
    Snappy.compress_array(logger, data, total_size)

  fun compress_java(logger: Logger[String], data: ByteSeq, block_size: USize = 32*1024): Array[U8] iso ? =>
    Snappy.compress_java(logger, data, block_size)

  fun compress_array_java(logger: Logger[String], data: Array[ByteSeq] val, total_size: USize, block_size: USize = 32*1024): Array[U8] iso ? =>
    Snappy.compress_array_java(logger, data, total_size, block_size)

primitive SnappyDecompressor
  fun decompress(logger: Logger[String], data: ByteSeq): Array[U8] iso ? =>
    Snappy.decompress(logger, data)

  fun decompress_java(logger: Logger[String], data: ByteSeq): Array[U8] iso ? =>
    Snappy.decompress_java(logger, data)

primitive Snappy
  fun read32be(buffer: ByteSeq, offset: USize): U32 ? =>
    // TODO: figure out some way of detecting endianness; big endian needs byte swapping
    (buffer(offset + 0).u32() << 24) or (buffer(offset + 1).u32() << 16) or
    (buffer(offset + 2).u32() << 8) or buffer(offset + 3).u32()

  fun decompress_java(logger: Logger[String], data: ByteSeq): Array[U8] iso ? =>
    let snappy_java_hdr_size: USize = 16
    let snappy_java_magic = [ as U8: 0x82, 'S', 'N', 'A', 'P', 'P', 'Y', 0 ]


    if data.size() <= (snappy_java_hdr_size + 4) then
      logger(Info) and logger.log(Info, "Not snappy java compressed data (not enough data). Falling back to normal snappy decompression.")
      return decompress(logger, data)
    end

    if not ((data(0) == snappy_java_magic(0))
      and (data(1) == snappy_java_magic(1))
      and (data(2) == snappy_java_magic(2))
      and (data(3) == snappy_java_magic(3))
      and (data(4) == snappy_java_magic(4))
      and (data(5) == snappy_java_magic(5))
      and (data(6) == snappy_java_magic(6))
      and (data(7) == snappy_java_magic(7))) then
      logger(Info) and logger.log(Info, "Not snappy java compressed data (invalid magic). Falling back to normal snappy decompression.")
      return decompress(logger, data)
    end

    var offset: USize = snappy_java_hdr_size
    var err: SnappyStatus = 0
    var total_uncompressed_size: USize = 0

    while (offset + 4) < data.size() do
      var uncompressed_size: USize = 0
      var chunk_size = read32be(data, offset).usize()
      offset = offset + 4

      if (chunk_size + offset) > data.size() then
        logger(Error) and logger.log(Error, "Snappy Java deconding error! Invalid chunk length encountered.")
        error
      end

      err = @snappy_uncompressed_length(data.cpointer().usize() + offset, chunk_size, addressof uncompressed_size)
      if err != 0 then
        logger(Error) and logger.log(Error, "Error determining uncompressed size of snappy java compressed chunk.")
        error
      end

      offset = offset + chunk_size
      total_uncompressed_size = total_uncompressed_size + uncompressed_size
    end

    if offset != data.size() then
      logger(Error) and logger.log(Error, "Error processing all of snappy java compressed data.")
      error
    end

    if total_uncompressed_size == 0 then
      logger(Error) and logger.log(Error, "Error snappy java uncompressed data is empty.")
      error
    end

    let buffer = recover Array[U8](total_uncompressed_size) end
    buffer.undefined(buffer.space())

    offset = snappy_java_hdr_size
    total_uncompressed_size = 0

    while (offset + 4) < data.size() do
      var uncompressed_size: USize = 0
      var chunk_size = read32be(data, offset).usize()
      if (chunk_size + offset) > data.size() then
        logger(Error) and logger.log(Error, "Snappy Java deconding error! Invalid chunk length encountered.")
        error
      end
      offset = offset + 4

      err = @snappy_uncompressed_length(data.cpointer().usize() + offset, chunk_size, addressof uncompressed_size)
      if err != 0 then
        logger(Error) and logger.log(Error, "Error determining uncompressed size of snappy java compressed chunk.")
        error
      end

      err = @snappy_uncompress(data.cpointer().usize() + offset, chunk_size, buffer.cpointer().usize() + total_uncompressed_size, addressof uncompressed_size)
      if err != 0 then
        logger(Error) and logger.log(Error, "Error uncompressing snappy java compressed chunk. Error code: " + err.string())
        error
      end

      offset = offset + chunk_size
      total_uncompressed_size = total_uncompressed_size + uncompressed_size
    end

    if offset != data.size() then
      logger(Error) and logger.log(Error, "Error processing all of snappy java compressed data.")
      error
    end

    logger(Fine) and logger.log(Fine, "Snappy java uncompressed data. Uncompressed size: " + total_uncompressed_size.string() + ", compressed size: " + data.size().string())

    buffer.truncate(total_uncompressed_size)

    buffer

  fun decompress(logger: Logger[String], data: ByteSeq): Array[U8] iso ? =>
    var max_size: USize = 0
    var err = @snappy_uncompressed_length(data.cpointer().usize(), data.size(), addressof max_size)
    if err != 0 then
      logger(Error) and logger.log(Error, "Error determining uncompressed size of snappy compressed data.")
      error
    end

    let buffer = recover Array[U8](max_size) end
    buffer.undefined(buffer.space())
    var out_len = buffer.size()

    err = @snappy_uncompress(data.cpointer().usize(), data.size(), buffer.cpointer().usize(), addressof out_len)
    if err != 0 then
      logger(Error) and logger.log(Error, "Error uncompressing snappy compressed data.")
      error
    end

    buffer.truncate(out_len)

    buffer

  fun compress(logger: Logger[String], data: ByteSeq): Array[U8] iso ? =>
    let max_size = @snappy_max_compressed_length(data.size())
    let buffer = recover Array[U8](max_size) end
    buffer.undefined(buffer.space())
    var out_len = buffer.size()

    var err = @snappy_compress(data.cpointer(), data.size(), buffer.cpointer().usize(), addressof out_len)
    if err != 0 then
      logger(Error) and logger.log(Error, "Error compressing data with snappy.")
      error
    end

    buffer.truncate(out_len)

    buffer

  // TODO: Figure out a way to do this without copying all the data into a single buffer
  fun compress_array(logger: Logger[String], data: Array[ByteSeq] val, total_size: USize): Array[U8] iso ? =>
    let arr = recover iso
        let a = Array[U8](total_size)
        for d in data.values() do
          match d
          | let x: Array[U8] val => x.copy_to(a, 0, a.size(), x.size())
          | let s: String => s.array().copy_to(a, 0, a.size(), s.array().size())
          end
        end
        a
      end

    compress(logger, consume arr)

  fun compress_java(logger: Logger[String], data: ByteSeq, block_size: USize = 32*1024): Array[U8] iso ? =>
    let snappy_java_hdr_size: USize = 16
    let snappy_java_magic = [ as U8: 0x82, 'S', 'N', 'A', 'P', 'P', 'Y', 0 ]

    let max_size = @snappy_max_compressed_length(data.size())
    let buffer = recover Array[U8](max_size + 16) end
    buffer.undefined(buffer.space())
    var total_compressed_size: USize = 0
    var out_len = buffer.size()

    var offset: USize = 0

    // write header
    buffer(0) = snappy_java_magic(0)
    buffer(1) = snappy_java_magic(1)
    buffer(2) = snappy_java_magic(2)
    buffer(3) = snappy_java_magic(3)
    buffer(4) = snappy_java_magic(4)
    buffer(5) = snappy_java_magic(5)
    buffer(6) = snappy_java_magic(6)
    buffer(7) = snappy_java_magic(7)
    buffer(8) = 0
    buffer(9) = 0
    buffer(10) = 0
    buffer(11) = 1
    buffer(12) = 0
    buffer(13) = 0
    buffer(14) = 0
    buffer(15) = 1

    total_compressed_size = 16
    offset = 0

    while offset < data.size() do
      let d = match data
        | let s: String => s.trim(offset, offset + block_size)
        | let a: Array[U8] val => a.trim(offset, offset + block_size)
        else
          error
        end

      // write compressed data to current write offset + 4
      out_len = buffer.size() - total_compressed_size
      var err = @snappy_compress(d.cpointer(), d.size(), buffer.cpointer().usize() + total_compressed_size + 4, addressof out_len)
      if err != 0 then
        logger(Error) and logger.log(Error, "Error compressing chunk with snappy.")
        error
      end

      // write chunk size (big endian)
      buffer(total_compressed_size + 0) = (out_len >> 24).u8()
      buffer(total_compressed_size + 1) = (out_len >> 16).u8()
      buffer(total_compressed_size + 2) = (out_len >> 8).u8()
      buffer(total_compressed_size + 3) = (out_len >> 0).u8()

      total_compressed_size = total_compressed_size + out_len + 4
      offset = offset + d.size()
    end

    logger(Fine) and logger.log(Fine, "Snappy java compressed data. Uncompressed size: " + data.size().string() + ", compressed size: " + total_compressed_size.string())

    buffer.truncate(total_compressed_size)

    buffer

  // TODO: Figure out a way to do this without copying all the data into a single buffer
  fun compress_array_java(logger: Logger[String], data: Array[ByteSeq] val, total_size: USize, block_size: USize = 32*1024): Array[U8] iso ? =>
    let arr = recover iso
        let a = Array[U8](total_size)
        for d in data.values() do
          match d
          | let x: Array[U8] val => x.copy_to(a, 0, a.size(), x.size())
          | let s: String => s.array().copy_to(a, 0, a.size(), s.array().size())
          end
        end
        a
      end

    compress_java(logger, consume arr, block_size)

