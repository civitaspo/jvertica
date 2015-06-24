class Jvertica
  class Row
    attr_reader :labels, :values, :rownum
    alias_method :keys, :labels

    include Enumerable

    def initialize(col_labels, col_labels_d, values, rownum)
      @labels = col_labels
      @labels_d = col_labels_d
      @values = values
      @rownum = rownum
    end

    def [](*idx)
      return @values[*idx] if idx.length > 1

      idx = idx.first
      case idx
      when Fixnum
        raise RangeError.new("Index out of bound") if idx >= @values.length
        @values[idx]
      when String, Symbol
        vidx = @labels_d.index(idx.to_s.downcase) or
          raise NameError.new("Unknown column label: #{idx}")
        @values[vidx]
      else
        @values[idx]
      end
    end

    def each(&blk)
      @values.each do |v|
        yield v
      end
    end

    def inspect
      strs = []
      @labels.each do |col|
        strs << "#{col}: #{self[col] || '(null)'}"
      end
      '[' + strs.join(', ') + ']'
    end

    def to_a
      @values
    end

    def join(sep = $OUTPUT_FIELD_SEPARATOR)
      to_a.join(sep)
    end

    def eql?(other)
      self.hash == other.hash
    end

    def hash
      @labels.zip(@values).sort.hash
    end

    def to_h
      Hash[@labels.zip(@values)]
    end

    alias :== :eql?

    def method_missing(symb, *args)
      if vidx = @labels_d.index(symb.to_s.downcase)
        @values[vidx]
      elsif @values.respond_to?(symb)
        @values.send(symb, *args)
      else
        raise NoMethodError.new("undefined method or attribute `#{symb}'")
      end
    end

    [:id, :tap, :gem, :display, :class, :method, :methods, :trust].select do |s|
      method_defined?(s)
    end.each do |m|
      undef_method(m)
    end
  end
end
