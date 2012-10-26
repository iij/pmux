require 'fileutils'
#require 'mros'
#require 'net/scp'

module Net::SSH
  class MockChannel
    @@handler_class = nil
    def self.handler_class=(klass)
      @@handler_class = klass
    end

    include MockWorld::BufferedSendable
    attr_accessor :host

    alias :send_data :buffered_send_data

    def initialize
      init_buffer
    end

    def exec cmd
      start_mock_cmd cmd
      @out_buffer = []
      yield self, true
    end

    def receive_data data
      @cb_on_data.call self, data
    end

    def on_data &block
      @cb_on_data = block
    end

    def on_extended_data &block
      @cb_on_extended_data = block
    end

    def on_close &block
      @cb_on_close = block
    end

    def on_eof &block
      @cb_on_eof = block
    end

    def do_eof
      @cb_on_eof.call(self) if @cb_on_eof
    end

    def on_open_failed &block
    end

    def start_mock_cmd cmd
      if cmd =~ /^pmux/ #XXX
        tmp_dir = "#{$test_dir}/.t"
        options = {:tmp_dir=>"#{tmp_dir}/#{host}",
          :log_dir=>"#{tmp_dir}/#{host}",
          :fs_dir=>"#{tmp_dir}/#{host}/fs"}
        FileUtils.mkdir_p options[:tmp_dir]
        FileUtils.mkdir_p options[:fs_dir]
        server = MR::Server.new
        handler_class = @@handler_class || Pmux::Handler
        handler = handler_class.new server, options

        pipe_transport = @world.new_connection MR::MockPipeTransport
        server.listen pipe_transport, handler
        pipe_transport.mock_receiver = self
        @mock_receiver = pipe_transport

        unix_transport = @world.new_connection MR::MockTransport
        unix_transport.address = "/unixsocket"
        server.listen unix_transport, handler
        server
      end
    end
  end

  class MockSession
    attr_reader :host
    attr_accessor :coolio_loop

    def initialize args
      host, user = args
      @host = host
    end

    def open_channel
      world = Net::SSH::Compat.coolio_loop.world
      ch = world.new_connection MockChannel
      ch.host = host
      yield ch
    end

    def scp
      @scp ||= Net::MockSCP.new(self)
    end

    def floop interval
      Net::SSH::Compat.coolio_loop.world.run_once
    end
  end

  def self.start *args, &block
    MockSession.new args
  end
end

module Net
  class MockSCP
    def initialize session
      @session = session
      @block = nil
      world = Net::SSH::Compat.coolio_loop.world
      world.run_queue.push self
      @dls = []
    end

    def download remote, local, options, &block
      ch = Net::SSH::MockChannel.new
      @dls.push [remote, local, options, ch]
      ch
    end

    def run_once
      return if @dls.empty?
      remote, local, options, ch = @dls.shift
      if ch
        str = File.read remote
        open(local, 'w') {|f| f.write str}
        ch.do_eof
      end
    end
  end
end

module Coolio
  class Loop
    @@_mock_loop = nil
    def self.default
      @@_mock_loop ||= Coolio::MockLoop.new
    end

    def set_timer(interval, repeating=false, &block)
    end
  end

  class MockLoop < Loop
    attr_accessor :world

    def run_once
      @world.run_once
    end

    def stop
    end
  end

  class Listener
    def initialize listen_socket
      @listen_socket = listen_socket
    end
  end

  class TCPServer
    def initialize(host, port = nil, klass = TCPSocket, *args, &block)
      @host, @port, @server = host, port, args.first
      super(nil, klass, *args, &block)
    end

    def attach loop
      loop.world.bind_socket self, @host, @port
      #loop.world.bind @server, [@host, @port]
      self
    end

    def on_connection socket
      connection = @klass.new(nil, *@args)
      connection.__send__(:on_connect)
      connection
    end
  end
end

module MR
  class MockTransport
    include MockWorld::BufferedSendable
    include MessageReceiver

    attr_accessor :address

    def initialize address=nil
      @address = address
      init_buffer
    end

    def listen server
      @pac = MessagePack::Unpacker.new
      @server = server
      init_buffer
      @world.bind_socket server, @address
    end

    def build_transport session, address
      @session = session
      @pac = MessagePack::Unpacker.new
      self
    end

    def receive_data data
      @pac.feed_each(data) {|obj| on_message obj}
    end

    def on_request msgid, method, param
      @server.on_request self, msgid, method, param
    end

    def on_response msgid, error, result
      @session.on_response self, msgid, error, result
    end
  end

  class TCPServerTransport::ServerSocket
    include MockWorld::BufferedSendable

    def initialize io, server
      @server = server
      init_buffer
      @pac = MessagePack::Unpacker.new
    end

    def send_data data
      buffered_send_data data
    end

    def receive_data data
      @pac.feed_each(data) {|obj| on_message obj}
    end
  end

  class MockPipeTransport < PipeTransport
    include MockWorld::BufferedSendable
    include MessageReceiver
    #attr_accessor :world, :mock_receiver

    def initialize
    end

    def listen server
      @pac = MessagePack::Unpacker.new
      @server = server
      init_buffer
    end

    def send_data data
      buffered_send_data data
    end

    def receive_data data
      @pac.feed_each(data) {|obj| on_message obj}
    end

    def on_request msgid, method, param
      @server.on_request self, msgid, method, param
    end
  end

  class Server
    attr_reader :listeners

    def initialize loop=Coolio::Loop.default#Coolio::MockLoop.new
      @loop = loop
      @listeners = []
    end

#    alias :listen0 :listen
#    def listen arg1, arg2=nil, arg3=nil, arg4=nil
#      listen0 arg1, arg2, arg3, arg4
#    end

    def run
    end
  end
end
