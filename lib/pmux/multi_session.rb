require 'net/scp'

class MR::Future
  attr_accessor :addr
end

module Pmux
  class SessionWrapper
    attr_reader :addr
    attr_accessor :ssh, :scp_session_count

    def initialize addr
      @addr = addr
      @ssh = nil
      @scp = nil
      @scp_session_count = 0
    end

    def scp
      @ssh ? (@scp ||= @ssh.scp) : nil
    end
  end

  class MultiSession
    attr_reader :loop
    attr_accessor :timeout

    def initialize addrs, options={}, loop=nil
      @addrs = addrs
      @options = options
      @user = options[:user]
      @loop = loop || Coolio::Loop.default
      Net::SSH::Compat.coolio_loop = @loop
      @timeout = 3600 #FIXME

      @err_addrs = []
      @sessions = {}
      @channels = {}

      @scptable = {}
      @scpid = 0
      @scp_queue = {}
      @buffers = {}

      @on_error = nil
    end

    def connect_to_addr addr, cmd=nil
      return if @sessions[addr]
      @sessions[addr] = SessionWrapper.new addr
      @loop.start_ssh(addr, @user) {|ssh|
        if ssh.respond_to? :open_channel
          host = ssh.host
          @sessions[host].ssh = ssh
          if cmd
            channel = ssh.open_channel {|ch|
              ch.exec(cmd) {|ch, success|
                raise RuntimeError unless success
                @channels[host] = ch

                if (ary = @buffers[host]) and !ary.empty?
                  ch.send_data ary.join('')
                  ary.clear
                end

                setup_channel ch
              }
            }
          end

          if (queue = @scp_queue[host]) and !queue.empty?
            queue2 = queue.dup
            queue.clear
            scp = @sessions[host].scp
            queue2.each {|updown, qf, qaddr, qremote, qlocal, qoptions|
                scp_download_sub scp, qaddr, qf, qremote, qlocal, qoptions}
          end

          ssh.floop 0.1
        elsif ssh.kind_of? Exception
          e = ssh
          error_on_addr addr, e.inspect
        else
          error_on_addr addr, 'failed'
        end
      }
    end

    def setup_channel ch
      ch.on_data {|c, data|}
      ch.on_extended_data {|c, type, data|
        #STDERR.puts c.connection.host+': '+data
      }
      #ch.on_close {|c| error_on_addr ch.connection.host}
    end

    def error_on_addr addr, err=nil
      @err_addrs.push addr
      @addrs.delete addr
      @on_error.call addr, err if @on_error
    end

    def close_channel addr=nil
      if addr
        if (ch = @channels[addr])
          ch.close
          ch.wait
        else
        end
      else
        @channels.each {|addr, ch| ch.close}
      end
    end

    def on_error &block
      @on_error = block
    end

    def scp_upload_files addr, files, remote, options={}, &block
      mf = MR::MultiFuture.new
      for file in files
        future = scp_upload addr, file, remote, options
        mf.add future
      end
      mf.on_all &block if block
      mf
    end

    def scp_upload addr, local, remote, options={}
      future = MR::Future.new self, @loop
      queue = (@scp_queue[addr] ||= [])

      if (scp = @sessions[addr].scp)
        scp_upload_sub scp, addr, future, local, remote, options
      else
        queue.push [:up, future, addr, remote, local, options]
      end
      future
    end

    def scp_upload_sub scp, addr, future, local, remote, options
      scpid = @scpid
      @scpid += 1
      @scptable[scpid] = future
      scp.upload(local, remote, options) {|ch, name, sent, total|
        if sent >= total
          future.set_result nil, sent
          @scptable.delete scpid
        end
      }
    end

    def scp_download addr, remote, local, options={}
      future = MR::Future.new self, @loop
      queue = (@scp_queue[addr] ||= [])

      if (scp = @sessions[addr].scp)
        scp_download_sub scp, addr, future, remote, local, options
      else
        queue.push [:down, future, addr, remote, local, options]
      end
      future
    end

    def scp_download_sub scp, addr, future, remote, local, options
      session = @sessions[addr]
      if !session or session.scp_session_count > 5
        queue = (@scp_queue[addr] ||= [])
        queue.push [:down, future, addr, remote, local, options]
        return
      end
      session.scp_session_count += 1

      scpid = @scpid
      @scpid += 1
      @scptable[scpid] = future
      channel = scp.download(remote, local, options)
      channel.on_eof {|ch|
        session.scp_session_count -= 1
        @loop.set_timer(0) {process_scp_queue_once addr}

        future.set_result(nil, options[:set_result])
        @scptable.delete scpid
      }
      channel.on_open_failed {|ch, code, desc|
        Log.error "#{addr}: scp error: #{desc}"
        err = RuntimeError.new "scp error: #{desc}"
        @on_error.call addr, err
        session.scp_session_count -= 1
        @loop.set_timer(0) {process_scp_queue_once addr}

        future.set_result(nil, options[:set_result])
        @scptable.delete scpid
      }
    end

    def process_scp_queue_once addr
      scp = @sessions[addr].scp
      queue = (@scp_queue[addr] ||= [])
      if scp and !queue.empty?
        updown, future, addr, remote, local, options = queue.shift
        case updown
        when :down
          scp_download_sub scp, addr, future, remote, local, options
        when :up
          scp_upload_sub scp, addr, future, local, remote, options
        end
      end
    end
  end

  class MRSession < MultiSession
    include MR::MessageReceiver

    def initialize addrs, options={}, loop=nil
      super

      @reqtable = {}
      @seqid = 0

      program_name = options[:program_name] || 'pmux'
      @cmd = "#{program_name} --server"
    end

    def connect
      for addr in @addrs
        connect_to_addr addr, @cmd + " --ipaddr=#{addr}"
      end
    end

    def setup_channel ch
      pac = MessagePack::Unpacker.new
      ch.on_data {|c, data|
        pac.feed_each(data) {|obj| on_message obj}
      }
      ch.on_extended_data {|c, type, data|
        #STDERR.puts c.connection.host+': '+data
      }
      ch.on_close {|c| error_on_addr ch.connection.host}
    end

    def error_on_addr addr, err=nil
      super
      err ||= 'closed'
      @reqtable.select {|msgid, f| f.addr == addr}.each {|msgid, f|
        f.set_result err, nil
      }
    end

    def call_async addr, method, *args
      send_request addr, method, args
    end

    def multicast_call_async method, *args
      mf = MR::MultiFuture.new
      for addr in @addrs
        future = send_request addr, method, args
        mf.add future
      end
      mf
    end

    def on_response msgid, error, result
      if (future = @reqtable.delete msgid)
        future.set_result error, result
      end
    end

    private
    def send_request addr, method, param
      method = method.to_s
      msgid = @seqid
      @seqid += 1; if @seqid >= 1<<31 then @seqid = 0 end
      data = [MR::REQUEST, msgid, method, param].to_msgpack
      if (ch = @channels[addr])
        ch.send_data data
      else
        (@buffers[addr] ||= []).push data
      end
      future = MR::Future.new self, @loop
      future.addr = addr
      @reqtable[msgid] = future
    end
  end
end

class MR::MultiFuture
  # monkey patch for MR::MultiFuture#join_all
  def join_all
    @not_joined.dup.each {|future|
      future.join
    }
    @all
  end
end

class MR::UNIXClientTransport::ClientSocket
  def on_close
    raise MR::TransportError, 'MR::UNIXClientTransport::ClientSocket on close'
  end
end
