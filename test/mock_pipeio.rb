require 'mock_world'
require 'mock_mros'

module Pmux
  class PipeIO
    def initialize cmd, mode='r'
      @io = IO.popen cmd, mode
      @on_receive = nil
      @close_flag = false
    end

    def flush_out_buffer
      data = @io.read 100
      if data
        on_read data
      else
        on_close unless @close_flag
        @close_flag = true
      end
    end

    def attach loop
      loop.world.push_connection self
    end
  end
end
