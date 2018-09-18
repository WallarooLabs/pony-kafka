use ".."
use "ponybench"
use "collections"

actor Main is BenchmarkList
  new create(env: Env) =>
    PonyBench(env, this)

  fun tag benchmarks(bench: PonyBench) =>
    var n: USize = 1
    while n < 1025 do
      bench(_XXHash32(n-1))
      bench(_XXHash32(n))
      bench(_XXHash32(n+1))
      n = (n+2).next_pow2()
    end

class iso _XXHash32 is MicroBenchmark
  // Benchmark xxhash32
  let _d: Array[U8]
  let _n: USize

  new iso create(n: USize) =>
    _n = n
    _d = Array[U8].>undefined(n)

  fun name(): String =>
    "_XXHash32(" + _n.string() + ")"

  fun apply()? =>
    DoNotOptimise[U32](XXHash.hash32(_d, 0)?)
    DoNotOptimise.observe()
