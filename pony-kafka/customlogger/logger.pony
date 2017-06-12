"""
# Logger package

Provides basic logging facilities. For most use cases, the `StringLogger` class
will be used. On construction, it takes 2 parameters and a 3rd optional
parameter:

* LogLevel below which no output will be logged
* OutStream to log to
* Optional LogFormatter

If you need to log arbitrary objects, take a look at `ObjectLogger[A]` which
can log arbitrary objects so long as you provide it a lambda to covert from A
to String.

## API Philosophy

The API for using Logger is an attempt to abide by the Pony philosophy of first,
be correct and secondly, aim for performance. One of the ways that logging can
slow your system down is by having to evaluate expressions to be logged
whether they will be logged or not (based on the level of logging). For example:

`logger.log(Warn, name + ": " + reason)`

will construct a new String regardless of whether we will end up logging the
message or not.

The Logger API uses boolean short circuiting to avoid this.

`logger(Warn) and logger.log(name + ": " + reason)`

will not evaluate the expression to be logged unless the log level Warn is at
or above the overall log level for our logging. This is as close as we can get
to zero cost for items that aren't going to end up being logged.

## Example programs

### String Logger

The following program will output 'my warn message' and 'my error message' to
STDOUT in the standard default log format.

```pony
use "logger"

actor Main
  new create(env: Env) =>
    let logger = StringLogger(
      Warn,
      env.out)

    logger(Fine) and logger.log("my fine message")
    logger(Info) and logger.log("my info message")
    logger(Warn) and logger.log("my warn message")
    logger(Error) and logger.log("my error message")
```

### Logger[A]

The following program will output '42' to STDOUT in the standard default log
format.

```pony
use "logger"

actor Main
  new create(env: Env) =>
    let logger = Logger[U64](Fine,
    env.out,
    {(a: U64): String => a.string() })

    logger(Error) and logger.log(U64(42))
```

## Custom formatting your logs

The Logger package provides an interface for formatting logs. If you wish to
override the standard formatting, you can create an object that implements:

```pony
interface val LogFormatter
  fun apply(
    msg: String,
    file_name: String,
    file_linenum: String,
    file_linepos: String): String
```

This can either be a class or because the interface only has a single apply
method, can also be a lambda.
"""

use "time"

type LogLevel is
  ( Fine
  | Info
  | Warn
  | Error
  )

primitive Fine
  fun apply(): U32 => 0
  fun string(): String => "FINE"

primitive Info
  fun apply(): U32 => 1
  fun string(): String => "INFO"

primitive Warn
  fun apply(): U32 => 2
  fun string(): String => "WARN"

primitive Error
  fun apply(): U32 => 3
  fun string(): String => "ERROR"

class val Logger[A]
  let _level: LogLevel
  let _out: OutStream
  let _f: {(A): String} val
  let _formatter: LogFormatter
  let _verbose: Bool

  new val create(level: LogLevel,
    out: OutStream,
    f: {(A): String} val,
    verbose: Bool = ifdef debug then true else false end,
    formatter: LogFormatter = DefaultLogFormatter)
  =>
    _level = level
    _out = out
    _f = f
    _formatter = formatter
    _verbose = verbose

  fun apply(level: LogLevel): Bool =>
    level() >= _level()

  fun log(level: LogLevel, value: A, date: String = Date(Time.seconds()).format("%Y-%m-%d %H:%M:%S"), loc: SourceLoc = __loc): Bool =>
    _out.print(_formatter(level, _f(consume value), _verbose, date, loc))
    true

primitive StringLogger
  fun apply(level: LogLevel,
    out: OutStream,
    verbose: Bool = ifdef debug then true else false end,
    formatter: LogFormatter = DefaultLogFormatter): Logger[String]
  =>
    Logger[String](level, out, {(s: String): String => s }, verbose, formatter)

interface val LogFormatter
  """
  Interface required to implement custom log formatting.

  * `msg` is the logged message
  * `loc` is the location log was called from

  See `DefaultLogFormatter` for an example of how to implement a LogFormatter.
  """
  fun apply(level: LogLevel, msg: String, verbose: Bool, date: String, loc: SourceLoc): String

primitive DefaultLogFormatter is LogFormatter
  fun apply(level: LogLevel, msg: String, verbose: Bool, date: String, loc: SourceLoc): String =>
    let file_name: String = loc.file()
    let file_linenum: String  = loc.line().string()
    let file_linepos: String  = loc.pos().string()
    let level_name: String = level.string()

    let output = recover String(
      if verbose then
        file_name.size()
        + file_linenum.size()
        + file_linepos.size()
        + 4
      else
        0
      end
    + date.size()
    + level_name.size()
    + msg.size()
    + 4) end

    output.append(date)
    output.append(": ")
    output.append(level_name)
    output.append(": ")
    if verbose then
      output.append(file_name)
      output.append(":")
      output.append(file_linenum)
      output.append(":")
      output.append(file_linepos)
      output.append(": ")
    end
    output.append(msg)
    output
