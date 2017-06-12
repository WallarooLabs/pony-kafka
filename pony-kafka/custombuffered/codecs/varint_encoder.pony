use ".."

primitive VarIntEncoder
  fun u8(wb: Writer, data: U8) =>
    """
    Write a byte to the buffer in base 128 varint encoding..
    """
    _encode_varint(wb, data.u64())

  fun i8(wb: Writer, data: I8) =>
    """
    Write a i8 to the buffer in base 128 varint encoding.
    """
    _encode_svarint(wb, data.i64())

  fun bool(wb: Writer, data: Bool) =>
    """
    Write a Bool to the buffer in base 128 varint encoding.
    """
    _encode_varint(wb, data.u64())

  fun u16(wb: Writer, data: U16) =>
    """
    Write a U16 to the buffer in base 128 varint encoding.
    """
    _encode_varint(wb, data.u64())

  fun i16(wb: Writer, data: I16) =>
    """
    Write an I16 to the buffer in zig zag base 128 varint encoding.
    """
    _encode_svarint(wb, data.i64())

  fun u32(wb: Writer, data: U32) =>
    """
    Write a U32 to the buffer in base 128 varint encoding.
    """
    _encode_varint(wb, data.u64())

  fun i32(wb: Writer, data: I32) =>
    """
    Write an I32 to the buffer in zig zag base 128 varint encoding.
    """
    _encode_svarint(wb, data.i64())

  fun u64(wb: Writer, data: U64) =>
    """
    Write a U64 to the buffer in base 128 varint.
    """
    _encode_varint(wb, data.u64())

  fun i64(wb: Writer, data: I64) =>
    """
    Write an I64 to the buffer in zig zag base 128 varint encoding.
    """
    _encode_svarint(wb, data.i64())

  fun _encode_svarint(wb: Writer, data: I64) =>
    _encode_varint(wb, ((data << 1) xor (data >> 63)).u64())

  fun _encode_varint(wb: Writer, data: U64) =>
    var d = data
    repeat
      wb.write_byte((d.u8() and 0x7f) or (if (d > 0x7f) then 0x80 else 0 end))
      d = d >> 7
    until (d == 0) end

