require 'java'
require 'thread'
require 'jvertica/version'
require 'jdbc-vertica'
require Jdbc::Vertica.driver_jar

class Jvertica
  unless %r{java} === RUBY_PLATFORM
    warn "only for use with JRuby"
  end

  @@default_option_value = {
    host: 'localhost',
    port: 5433,
    database: 'vdb',
    password: '',
    user: 'dbadmin',
    AutoCommit: false,
  }

  def self.connect options = {}
    new options
  end

  attr_reader :host, :port, :database

  def initialize options
    options = @@default_option_value.merge(options).to_sym
    @host = options[:host]
    @port = options[:port]
    @database = options[:database]
    %w(:host :port :database).map do |key|
      options.delete key
    end

    prop = Properties.new
    options.each do |key, value|
      prop.put key.to_s, value
    end

    @connection = begin
                    DriverManager.getConnection "jdbc:vertica://#{host}:#{port}/#{database}", prop
                  rescue => e
                    raise ConnectionError.new("Connection Failed.\n" +
                      "Error Message => #{e.message}\n" +
                      "see documentation => #{Constant::CONNECTION_PROPERTY_DOCUMENT_URL}\n")
                  end
    @closed = false
    @connection
  end

  def closed?
    @closed
  end

  def close
    @connection.close && @closed = true
  end

  def commit
    @connection.commit
  end

  def rollback
    @connection.rollback
  end

  #def prepare query
  #  @pstmt = @connection.prepareStatement query
  #end

  #def prepared?
  #  @pstmt.present?
  #end

  #def execute *args, &blk
  #  TODO
  #end

  def property key, value = nil
    key = key.to_s
    if value.nil?
      @connection.getProperty key
    else
      @connection.setProperty key, value
    end
  end

  def query query, &blk
    stmt = @connection.createStatement
    case query
    when %r{\A\s*copy}miu   then return stmt.execute query
    when %r{\A\s*insert}miu then return stmt.executeUpdate query
    when %r{\A\s*update}miu then return stmt.executeUpdate query
    when %r{\A\s*delete}miu then return stmt.executeUpdate query
    when %r{\A\s*drop}miu   then return stmt.execute query
    when %r{\A\s*create}miu then return stmt.execute query
    when %r{\A\s*set}miu    then return stmt.execute query
    else rs = stmt.executeQuery query
    end

    if block_given?
      ResultSet.new(rs).each do |row|
        yield row
      end
    else
      ResultSet.new rs
    end
  end

  def copy query, source = nil, &blk
    raise InvalidQuery.new('can use only "copy".') unless %r{\A\s*copy}miu === query
    if !source.nil?
      copy_stream(query, source, &blk)
    else
      [query(query), nil]
    end
  end

  private
  def copy_stream query, io = nil, &blk
    stream = com.vertica.jdbc.VerticaCopyStream.new @connection, query
    stream.start
    thread = nil
    begin

      if block_given?

        i, o = IO.pipe
        begin
          thread = Thread.new do
                     yield(o)
                     o.close
                   end
          stream.addStream org.jruby.util.IOInputStream.new(i)
        rescue => e
          raise e
        ensure
        end

      else

        if source.is_a? IO
          stream.addStream org.jruby.util.IOInputStream.new(source)
        else
          raise InvalidObject.new("source must be a IO.")
        end

      end

    rescue => e
      r = stream.finish
      raise e.class.new("[affected rows: #{r}] #{e.message}")
    end

    begin
      stream.execute
      rejects = stream.getRejects
      results = stream.finish
    rescue => e
      raise e
    ensure
      thread.join
    end

    [results, rejects.to_ary]
  end

  class ConnectionError < StandardError
  end

  class InvalidQuery < StandardError
  end

  class InvalidObject < StandardError
  end

  class InsufficientArgument < StandardError
  end

  class ResultSet
    include Enumerable

    def each
      return enum_for(:each) unless block_given?
      return if closed?

      while @nrow
        idx = 0
        row = Jvertica::Row.new(
          @col_labels,
          @col_labels_d,
          @getters.map{|gt|
            case gt
            when :getBigNum
              v = @rset.getBigDecimal idx+=1
              @rset.was_null ? nil : v.toPlainString.to_i
            when :getBigDecimal
              v = @rset.getBigDecimal idx+=1
              @rset.was_null ? nil : BigDecimal.new(v.toPlainString)
            else
              v = @rset.send gt, idx+=1
              @rset.was_null ? nil : v
            end
          },
          @rownum += 1
        )
        close unless @nrow = @rset.next
        yield row
      end
      close
    end

    def close
      return if closed?

      @rset.close
      @close_callback.call if @close_callback
      @closed = true
    end

    def closed?
      @closed
    end

    def initialize rset, &close_callback
      unless rset.respond_to? :get_meta_data
        rset.close if rset
        @closed = true
        return
      end

      @close_callback = close_callback
      @rset           = rset
      @rsmd           = @rset.get_meta_data
      @num_col        = @rsmd.get_column_count
      @getters        = []
      @col_labels     = []
      @col_labels_d   = []
      (1..@num_col).each do |i|
        type = @rsmd.get_column_type i

        @getters <<
          case type
          when java.sql.Types::NUMERIC, java.sql.Types::DECIMAL
            precision = @rsmd.get_precision i
            scale = @rsmd.get_scale i

            if precision > 0 and scale >= 0
              if scale > 0 then :getBigDecimal
              else

                if precision <= 9     then :getInt
                elsif precision <= 18 then :getLong
                else                       :getBigNum
                end

              end
            else :getBigDecimal
            end

          else Jvertica::Constant::GETTER_MAP.fetch type, :get_string
          end

        label = @rsmd.get_column_label i
        @col_labels << label
        @col_labels_d << label.downcase
      end

      @rownum = -1
      @nrow   = @rset.next
      @closed = false
    end
  end

  class Row
    attr_reader :labels, :values, :rownum
    alias_method :keys, :labels

    include Enumerable

    def [] *idx
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

    def join sep = $OUTPUT_FIELD_SEPARATOR
      to_a.join sep
    end

    def eql? other
      self.hash == other.hash
    end

    def hash
      @labels.zip(@values).sort.hash
    end

    def to_h
      Hash[@labels.zip @values]
    end

    alias :== :eql?

    def initialize col_labels, col_labels_d, values, rownum
      @labels = col_labels
      @labels_d = col_labels_d
      @values = values
      @rownum = rownum
    end

    def method_missing symb, *args
      if vidx = @labels_d.index(symb.to_s.downcase)
        @values[vidx]
      elsif @values.respond_to? symb
        @values.send symb, *args
      else
        raise NoMethodError.new("undefined method or attribute `#{symb}'")
      end
    end

    [:id, :tap, :gem, :display, :class, :method, :methods, :trust].select do |s|
      method_defined? s
    end.each do |m|
      undef_method m
    end
  end

  class DriverManager < java.sql.DriverManager
  end

  class Properties < java.util.Properties
  end

  class DataSource < com.vertica.jdbc.DataSource
  end

  module Constant
    CONNECTION_PROPERTY_DOCUMENT_URL =
      'http://my.vertica.com/docs/7.1.x/HTML/index.htm#Authoring/ProgrammersGuide/ClientJDBC/JDBCConnectionProperties.htm'

    RUBY_SQL_TYPE_MAP = {
      Fixnum => java.sql.Types::INTEGER,
      Bignum => java.sql.Types::BIGINT,
      String => java.sql.Types::VARCHAR,
      Float => java.sql.Types::DOUBLE,
      Time => java.sql.Types::TIMESTAMP
    }
    GETTER_MAP = {
      java.sql.Types::TINYINT => :getInt,
      java.sql.Types::SMALLINT => :getInt,
      java.sql.Types::INTEGER => :getInt,
      java.sql.Types::BIGINT => :getLong,
      java.sql.Types::CHAR => :getString,
      java.sql.Types::VARCHAR => :getString,
      java.sql.Types::LONGVARCHAR => :getString,
      (java.sql.Types::NCHAR rescue nil) => :getString,
      (java.sql.Types::NVARCHAR rescue nil) => :getString,
      (java.sql.Types::LONGNVARCHAR rescue nil) => :getString,
      java.sql.Types::BINARY => :getBinaryStream,
      java.sql.Types::VARBINARY => :getBinaryStream,
      java.sql.Types::LONGVARBINARY => :getBinaryStream,
      java.sql.Types::REAL => :getDouble,
      java.sql.Types::FLOAT => :getFloat,
      java.sql.Types::DOUBLE => :getDouble,
      java.sql.Types::DATE => :getDate,
      java.sql.Types::TIME => :getTime,
      java.sql.Types::TIMESTAMP => :getTimestamp,
      java.sql.Types::BLOB => :getBlob,
      java.sql.Types::CLOB => :getString,
      (java.sql.Types::NCLOB rescue nil) => :getString,
      java.sql.Types::BOOLEAN => :getBoolean
    }
  end
end

class Hash
  def to_sym
    self.inject self.class.new do |h, (k, v)|
      h[k.to_sym] = if v.is_a? self.class
                      v.to_sym
                    else
                      v
                    end
      h
    end
  end
end
