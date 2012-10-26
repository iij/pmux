# msgpack-rpc over ssh

require 'net/ssh'
if require 'msgpack/rpc'
  require 'msgpack/rpc/transport/unix'
  MR = MessagePack::RPC
end
if defined? Rev
  Coolio = Rev
end
require 'net/scp'

class Net::SSH::Compat
  class Watcher < Coolio::IOWatcher
    def initialize ruby_io, flags, fiber, watchers
      @ruby_io = ruby_io
      @fiber = fiber
      @watchers = watchers
      super ruby_io, flags
    end

    def on_readable
      cleanup
      @fiber.resume([[@ruby_io]])
    end
    def on_writable
      cleanup
      @fiber.resume([[], [@ruby_io]])
    end
    def cleanup
      for w in @watchers
        w.detach
      end
    end
  end

  class TimerWatcher < Coolio::TimerWatcher
    def initialize interval, fiber, watchers
      @fiber = fiber
      @watchers = watchers
      super(interval, false)
    end

    def on_timer
      for w in @watchers; w.detach; end
      @fiber.resume(nil)
    end
  end

  class <<self
    @@loop = Coolio::Loop.default

    def coolio_loop
      @@loop
    end
    def coolio_loop=(loop)
      @@loop = loop
    end

    alias :io_select0 :io_select
    def io_select reads, writes=nil, excepts=nil, timeout=nil
      if timeout and timeout.zero?
        io_select0 reads, writes, excepts, 0
      else
        writes ||= []
        loop = @@loop

        watchers = []
        rw = reads & writes
        for io in (reads | writes)
          if rw.include? io
            flag = :rw
          elsif reads.include? io
            flag = :r
          else
            flag = :w
          end
          watcher = Watcher.new io, flag, Fiber.current, watchers
          watchers.push watcher
          watcher.attach loop
        end
        if timeout
          watcher = TimerWatcher.new timeout, Fiber.current, watchers
          watchers.push watcher
          watcher.attach loop
        end
        Fiber.yield
      end
    end
  end
end

class Net::SSH::Connection::Session
  attr_accessor :coolio_loop

  def floop wait=nil
    while true
      break if closed?
      #break unless busy?
      loop wait
      @coolio_loop.waitings[Fiber.current] = self
      Fiber.yield
    end
  end
end

module Coolio
  class Loop
    attr_reader :waitings
    def start_ssh *args, &block
      @waitings ||= {}
      fiber = Fiber.new {
        begin
          ssh = Net::SSH.start *args
          ssh.coolio_loop = self
          block.call ssh
        rescue => e
          block.call e
        end
      }
      fiber.resume
    end

    def run
      raise RuntimeError, "no watchers for this loop" if @watchers.empty?

      @running = true
      while @running and not @active_watchers.zero?
        run_once
        if @waitings and !@waitings.empty?
          busy_sessions = @waitings.select {|f, s| s.busy?}
          for fiber, ssh in busy_sessions
            @waitings.delete fiber
            fiber.resume
          end
        end
      end
      @running = false
    end

    def set_timer(interval, repeating=false, &block)
      timer = TimerWatcher.new interval, repeating
      timer.on_timer {
        block.call
        timer.detach unless repeating
      }
      timer.attach self
    end
  end
end

module MR
  class PipeTransport
    def initialize ruby_in, ruby_out, ruby_err
      @ruby_in = ruby_in
      @ruby_out = ruby_out
      @ruby_err = ruby_err
    end

    def listen server
      pout = PipeOut.new @ruby_out
      pin = PipeIn.new @ruby_in, server, pout
      perr = PipeOut.new @ruby_err
      server.loop.attach pout
      server.loop.attach pin
      server.loop.attach perr
    end

    def close
    end

    class PipeOut < Coolio::IO
      def initialize ruby_io
        @ruby_io = ruby_io
        super ruby_io
      end

      def on_readable
      end

      def send_data data
        @ruby_io.write data
      end
    end

    class PipeIn < Coolio::IO
      include MR::MessageReceiver

      def initialize ruby_io, server, pout
        @ruby_io = ruby_io
        super ruby_io
        @server = server
        @pout = pout
        @pac = MessagePack::Unpacker.new
      end
      def on_read data
        @pac.feed_each(data) {|obj| on_message obj}
      end
      def on_request msgid, method, param
        @server.on_request @pout, msgid, method, param
      end
      def on_close
        on_request 0, 'quit', []
      end
    end
  end
end
