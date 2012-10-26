module Pmux
  class PipeIO < Coolio::IO
    def initialize cmd, mode='r'
      io = IO.popen cmd, mode
      super io
      @on_receive = nil
    end

    def on_receive &block
      @on_receive = block
    end

    def on_read data
      if @on_receive
        @on_receive.call data
      end
    end
  end
end
