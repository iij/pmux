require 'fileutils'

class MockError < RuntimeError; end
class MockBlockedError < MockError; end

class MockWorld
  ROOT_PREFIX = "#{$test_dir}/.t"

  module BufferedSendable
    attr_accessor :mock_receiver, :world

    def init_buffer
      @mock_receiver = nil
      @out_buffer = []
    end

    def buffered_send_data data
      @out_buffer << data.dup
    end

    alias :send_data :buffered_send_data

    def flush_out_buffer
      if @out_buffer.empty?
        data = nil
      else
        data = @out_buffer.join ''
        @out_buffer.clear
      end
      if @mock_receiver
        @mock_receiver.receive_data data if data
      end
    end

    def data
      @out_buffer.join ''
    end

    def clear
      @out_buffer.clear
    end
  end

  attr_reader :connections, :run_queue, :hosts

  def self.cleanup root_prefix=ROOT_PREFIX
    Dir.glob("#{root_prefix}/{*,.err*}").each {|e| FileUtils.rm_rf e}
  end

  def initialize root_prefix=ROOT_PREFIX
    @root_prefix = root_prefix
    @dcount = 1

    @hosts = []
    @server_sockets = {}
    @connections = []
    @run_queue = []
    MockTCPSocket.world = self
    # テスト用rootディレクトリ生成
    FileUtils.mkdir_p root_prefix
  end

  #
  def bind_socket socket, arg1, arg2=nil
    if arg2
      @server_sockets["#{arg1},#{arg2}"] = socket
    else
      @server_sockets[arg1] = socket
    end
  end

  #
  def push_connection connection
    @connections.push connection
  end

  # 新しいconnectionを生成して返す。生成したconnectionは記憶される。
  def new_connection conn_class
    connection = conn_class.new
    connection.world = self
    @connections.push connection
    connection
  end

  def delete_connection connection
    @connections.delete connection
  end

  # 指定host,portのserverに疑似接続してclient用socketを返す
  def connect_to_server host, port
    server = get_server host, port
    raise MockError, "#{host},#{port}: connection refused" unless server
    server.get_client_connection
  end

  #
  def get_server_socket host, port
    @server_sockets["#{host},#{port}"]
  end

  def run_once
    #if @connections.empty? and @run_queue.empty?
    #  raise RuntimeError, "both @connections and @run_queue are empty"
    #end
    for c in @connections
      c.flush_out_buffer
    end
    for obj in @run_queue
      obj.run_once
    end
  end
end

# 擬似的な TCPSocket。
# 接続相手はConnection経由のserverオブジェクト。
class MockTCPSocket
  attr_reader :received_data
  attr_accessor :mock_receiver

  class <<self
    attr_accessor :world
    alias :open :new
  end

  def initialize host, port, lhost=nil, lport=nil
    @host = host
    @port = port
    @received_data = ''

    @socket = self.class.world.get_server_socket host, port
    @mock_receiver = @socket.on_connection nil
    self.class.world.push_connection @mock_receiver
    @mock_receiver.mock_receiver = self
  end

  # 送信。すぐ相手に届く。
  def write data
    if @mock_receiver
      @mock_receiver.receive_data data
    end
  end

  def puts line
    self.write "#{line}\n"
  end

  # 受信。一旦、受信バッファに入れる。
  def receive_data data
    @received_data << data
  end

  # 受信バッファから1行分のデータを取り出して返す。
  # 1行分受信していなかったら、接続先サーバをrun_tickして、それでも
  # 受信しなかったらBLOCKEDエラー。
  def gets
    count = 0
    while true
      line, rbuf = @received_data.split(/^/, 2)
      break if line =~ /\n/
      #self.class.world.run_tick 1, 1
      self.class.world.run_once
      raise MockBlockedError, 'BLOCKED' if (count += 1) > 5
    end
    @received_data = (rbuf || '')
    line
  end

  def read n=nil
    count = 0
    size = 0
    res = ''
    while true
      if n
        if size + @received_data.size >= n
          res << @received_data.slice!(0, n-size)
          break
        end
      elsif !@received_data.empty?
        res << @received_data
        @received_data.replace ''
        break
      end
      #self.class.world.run_tick 1, 1
      self.class.world.run_once
      raise MockBlockedError, 'BLOCKED' if (count += 1) > 5
    end
    res
  end

  def close
    @mock_receiver.close
  end
end
