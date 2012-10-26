require 'socket'
require 'forwardable'

module Pmux
  class StorageAdapter
    Adapters = {}
    extend Forwardable
    def_delegators :@h, :each, :size, :[], :[]=, :update, :delete

    def self.create name, addrs, options={}
      adapter_class = Adapters[name] || GlusterFSAdapter
      adapter_class.new addrs, options
    end

    attr_reader :addrs

    def initialize addrs=[], options={}
      @addrs = addrs
      @options = options
      @h = init_node_hash addrs
    end

    def init_node_hash addrs
      Hash[*(addrs.map {|addr| [addr, {}]}).flatten]
    end

    def connect_to_storage locator_host, locator_port
    end
  end
end

module Pmux
  class LocalAdapter < StorageAdapter
    Adapters['local'] = self

    def get_files args, glob_flag=false
      if glob_flag
        Dir.glob args.join("\0")
      else
        args
      end
    end

    def _get_file_locations files, glob_flag=false
      files = Dir.glob files.join("\0") if glob_flag
      files.map {|file|
        a = addrs.map {|addr| [addr, file]}
        addrs.push addrs.shift
        [file, a]
      }
    end

    def lookup_file file
      res = addrs.map {|addr| [addr, file]}
      addrs.push addrs.shift
      res
    end
  end
end

module Pmux
  class GlusterFSAdapter < StorageAdapter
    Adapters['glusterfs'] = self

    def getaddr host
      sa = Socket.pack_sockaddr_in 0, host
      port, addr = Socket.unpack_sockaddr_in sa
      addr
    end

    def connect_to_storage locator_host, locator_port
      locator_port ||= 7076
      @client = MR::Client.new locator_host, locator_port
      @client.timeout = 3600 #FIXME
      @client
    end

    def get_files args, glob_flag=false
      raise RuntimeError, 'not connected' unless @client
      result = @client.call :get_locations, args, glob_flag
      @locations = result

      # hostname -> addr
      hosts = {}
      for vs in result.values
        for host, path in vs
          hosts[host] = true
        end
      end
      @host2addr = {}
      @addrs = hosts.keys.map {|host| addr = getaddr host
        @host2addr[host] = addr}
      @h = init_node_hash @addrs
      result.keys # files
    end

    def lookup_file file
      if (res = @locations[file])
        res.map {|host, path| [@host2addr[host], path]}
      else
        nil
      end
    end
  end
end
