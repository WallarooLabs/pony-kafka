trait Reader
  fun size(): USize
  fun ref clear()
  fun ref skip(n: USize) ?
  fun ref block(len: USize): Array[U8] iso^ ?
  fun ref read_byte(): U8 ?
  fun ref read_bytes(len: USize): (Array[U8] val | Array[Array[U8] val] val | Array[U8] iso^ | Array[Array[U8] iso] iso^) ?
  fun ref read_contiguous_bytes(len: USize): (Array[U8] val | Array[U8] iso^) ?

trait PeekableReader is Reader
  fun box peek_byte(offset: USize = 0): U8 ?
  fun box peek_bytes(len: USize, offset: USize = 0):
    (Array[U8] val | Array[Array[U8] val] val) ?

