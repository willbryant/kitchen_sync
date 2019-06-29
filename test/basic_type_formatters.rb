class Date
  def to_s
    strftime("%Y-%m-%d") # make the standard input format the output format
  end

  def to_msgpack(*args)
    to_s.to_msgpack(*args)
  end
end

class Time
  def to_s
    strftime("%Y-%m-%d %H:%M:%S") # not interested in %z for tests
  end

  def to_msgpack(*args)
    to_s.to_msgpack(*args)
  end
end

module NonScientificFormat
  def to_s
    super('F')
  end
end

class BigDecimal
  prepend NonScientificFormat

  def to_msgpack(*args)
    to_s.to_msgpack(*args)
  end
end

# we don't currently use the actual msgpack float types etc. in the real program's database-value encoders,
# but unfortunately we can't override the ruby msgpack gem's Float#to_msgpack as it's provided by a C extension
class HashAsStringWrapper
  def initialize(v)
    @v = v
  end

  def ==(other)
    @v.to_s == other.to_s
  end

  def to_s
    @v.to_s
  end

  def inspect
    @v.inspect
  end

  def to_msgpack(*args)
    to_s.to_msgpack(*args)
  end
end

class TimeOnlyWrapper
  def initialize(v)
    @v = v
  end

  def ==(other)
    @v.to_s == (other.is_a?(Time) ? other.strftime("%H:%M:%S") : to_s)
  end

  def to_s
    @v.to_s
  end

  def inspect
    @v.inspect
  end

  def to_msgpack(*args)
    to_s.to_msgpack(*args)
  end
end
