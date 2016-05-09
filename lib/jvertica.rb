require 'java'
require 'thread'
require 'jdbc-vertica'
require Jdbc::Vertica.driver_jar
require 'jvertica/version'
require 'jvertica/result_set'
require 'jvertica/row'
require 'jvertica/error'
require 'jvertica/constant'

class Jvertica
  unless %r{java} === RUBY_PLATFORM
    warn "only for use with JRuby"
  end

  DEFAULT_OPTION_VALUES = {
    host: 'localhost',
    port: 5433,
    database: 'vdb',
    password: '',
    user: 'dbadmin',
    AutoCommit: false,
  }

  def self.connect(options = {})
    new(options)
  end

  # Properly quotes a value for safe usage in SQL queries.
  #
  # This method has quoting rules for common types. Any other object will be converted to
  # a string using +:to_s+ and then quoted as a string.
  #
  # @param [Object] value The value to quote.
  # @return [String] The quoted value that can be safely included in SQL queries.
  def self.quote(value)
    case value
    when nil        then 'NULL'
    when false      then 'FALSE'
    when true       then 'TRUE'
    when DateTime   then value.strftime("'%Y-%m-%d %H:%M:%S'::timestamp")
    when Time       then value.strftime("'%Y-%m-%d %H:%M:%S'::timestamp")
    when Date       then value.strftime("'%Y-%m-%d'::date")
    when String     then "'#{value.gsub(/'/, "''")}'"
    when Numeric    then value.to_s
    when Array      then value.map { |v| self.quote(v) }.join(', ')
    else
      if defined?(BigDecimal) and BigDecimal === value
        value.to_s('F')
      else
        self.quote(value.to_s)
      end
    end
  end

  # Quotes an identifier for safe use within SQL queries, using double quotes.
  # @param [:to_s] identifier The identifier to quote.
  # @return [String] The quoted identifier that can be safely included in SQL queries.
  def self.quote_identifier(identifier)
    "\"#{identifier.to_s.gsub(/"/, '""')}\""
  end

  attr_reader :host, :port, :database

  def initialize(options)
    options   = options.inject({}) {|h, (k, v)| h[k.to_sym] = v; h } # symbolize_keys
    options   = DEFAULT_OPTION_VALUES.merge(options)
    @host     = options.delete(:host)
    @port     = options.delete(:port)
    @database = options.delete(:database)

    prop = Properties.new
    options.each do |key, value|
      prop.put(key.to_s, value) unless value.nil?
    end

    @connection =
      begin
        com.vertica.jdbc.Driver.new.connect("jdbc:vertica://#{host}:#{port}/#{database}", prop)
      rescue => e
        raise ConnectionError.new(
          "Connection Failed.\n" <<
          "Error Message => #{e.message}\n" <<
          "see documentation => #{Constant::CONNECTION_PROPERTY_DOCUMENT_URL}\n"
        )
      end

    @closed = false
    @connection
  end

  def closed?
    @closed
  end

  def close
    @connection.close
  ensure
    @connection = nil
    @closed = true
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

  def property(key, value = nil)
    key = key.to_s
    if value.nil?
      @connection.getProperty(key)
    else
      @connection.setProperty(key, value)
    end
  end

  def query(query, &blk)
    stmt = @connection.createStatement
    case query
    when %r{\A\s*copy}miu   then return stmt.execute(query)
    when %r{\A\s*insert}miu then return stmt.executeUpdate(query)
    when %r{\A\s*update}miu then return stmt.executeUpdate(query)
    when %r{\A\s*delete}miu then return stmt.executeUpdate(query)
    when %r{\A\s*alter}miu  then return stmt.executeUpdate(query)
    when %r{\A\s*drop}miu   then return stmt.execute(query)
    when %r{\A\s*create}miu then return stmt.execute(query)
    when %r{\A\s*set}miu    then return stmt.execute(query)
    when %r{\A\s*grant}miu  then return stmt.execute(query)
    else rs = stmt.executeQuery(query)
    end

    if block_given?
      ResultSet.new(rs).each do |row|
        yield row
      end
    else
      ResultSet.new(rs)
    end
  end

  def copy(query, source = nil, &blk)
    raise InvalidQuery.new('can use only "copy".') unless %r{\A\s*copy}miu === query
    if source or block_given?
      copy_stream(query, source, &blk)
    else
      [query(query), nil]
    end
  end

  private

  class Properties < java.util.Properties
  end

  class DataSource < com.vertica.jdbc.DataSource
  end

  def copy_stream(query, io = nil, &blk)
    unless ((io and io.is_a?(IO)) or block_given?)
      raise InvalidObject.new("block or IO object is required.")
    end

    stream = com.vertica.jdbc.VerticaCopyStream.new(@connection, query)
    stream.start
    thread = i = nil

    begin

      if block_given?
        i, o = IO.pipe
        copy_stream_thread = Thread.current
        thread = Thread.new do
          begin
            yield(o)
          rescue => e
            copy_stream_thread.raise e
          ensure
            o.close rescue nil
          end
        end
        stream.addStream(org.jruby.util.IOInputStream.new(i))
      else
        stream.addStream(org.jruby.util.IOInputStream.new(io))
      end

    rescue => e
      r = stream.finish
      raise e.class.new("[affected rows: #{r}] #{e.message}")
    end

    begin
      stream.execute
      rejects = stream.getRejects
      results = stream.finish
    ensure
      thread.join unless thread.nil?
      i.close rescue nil
    end

    [results, rejects.to_ary]
  end
end
