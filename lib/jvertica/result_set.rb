require 'jvertica/row'

class Jvertica
  class ResultSet
    include Enumerable

    def initialize(rset, &close_callback)
      unless rset.respond_to?(:get_meta_data)
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
        type = @rsmd.get_column_type(i)

        @getters <<
          case type
          when java.sql.Types::NUMERIC, java.sql.Types::DECIMAL
            precision = @rsmd.get_precision(i)
            scale = @rsmd.get_scale(i)

            if precision > 0 and scale >= 0
              if scale > 0
                :getBigDecimal
              elsif precision <= 9
                :getInt
              elsif precision <= 18
                :getLong
              else
                :getBigNum
              end
            else
              :getBigDecimal
            end

          else Jvertica::Constant::GETTER_MAP.fetch type
            :get_string
          end

        label = @rsmd.get_column_label(i)
        @col_labels << label
        @col_labels_d << label.downcase
      end

      @rownum = -1
      @nrow   = @rset.next
      @closed = false
    end

    def each
      return enum_for(:each) unless block_given?
      return if closed?

      while @nrow
        idx = 0
        row = Jvertica::Row.new(
          @col_labels,
          @col_labels_d,
          @getters.map {|gt|
            case gt
            when :getBigNum
              v = @rset.getBigDecimal(idx+=1)
              @rset.was_null ? nil : v.toPlainString.to_i
            when :getBigDecimal
              v = @rset.getBigDecimal(idx+=1)
              @rset.was_null ? nil : BigDecimal.new(v.toPlainString)
            else
              v = @rset.send(gt, idx+=1)
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
      @rset.close rescue nil
      @close_callback.call if @close_callback
    ensure
      @closed = true
    end

    def closed?
      @closed
    end
  end
end
