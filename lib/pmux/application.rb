require 'optparse'
require 'yaml'

module Pmux
  class Application
    OPTS = {}

    def run options=OPTS
      optparser = optparse options
      optparser.parse!
      load_config options
      options[:program_name] = optparser.program_name
      options[:user] ||=
        (ENV['USER'] || ENV['LOGNAME'] || Etc.getlogin || Etc.getpwuid.name)

      root_dir = (options[:root_dir] ||=
                  File.expand_path "~/.#{options[:program_name]}")
      tmp_dir = (options[:tmp_dir] ||= root_dir + '/tmp')
      log_dir = (options[:log_dir] ||= root_dir + '/log')
      [root_dir, tmp_dir, log_dir].each {|dir|
        Dir.mkdir dir unless File.exist? dir
      }

      Plugin.load_plugins unless options[:disable_plugins]
      addrs = (options[:hosts] || '').split(',').map {|host| getaddr host}

      case
      when options[:server]
        (options[:sock_path] ||= '/tmp/.pmuxsock') << ".#{$$}"
        run_server options
      when options[:status]
        show_status addrs, options
      when options[:lookup]
        lookup addrs, options
      when options[:show_joblog]
        show_joblog options
      else
        run_mr addrs, options
      end
    end

    def load_config options
      path = File.expand_path(options[:config_file] || "~/.pmux/config.yml")
      if File.file? path
        conf = YAML.load_file path
        if conf.kind_of? Hash
          for k, v in conf
            options[k.intern] ||= v
          end
        end
      end
    end

    def getaddr host
      sa = Socket.pack_sockaddr_in 0, host
      port, addr = Socket.unpack_sockaddr_in sa
      addr
    end

    def run_server options
      STDOUT.sync = true
      server = MR::Server.new
      handler = Pmux::Handler.new server, options
      pipe_transport = MR::PipeTransport.new STDIN, STDOUT, STDERR
      server.listen pipe_transport, handler
      unix_transport = MR::UNIXServerTransport.new options[:sock_path]
      server.listen unix_transport, handler
      server.run
    rescue SystemCallError
    ensure
      File.unlink options[:sock_path] rescue nil
    end

    alias :putline :puts

    def show_joblog options
      log_dir = options[:log_dir]
      if (job_id = options[:show_joblog]) == true
        joblogs = Dir.glob(log_dir + '/*.yml').map {|path|
          obj = YAML.load_file path
        }
        for h in joblogs.sort_by {|obj| obj[:invoked_at].to_f}
          t = (h[:invoked_at] || h[:start_time]).strftime("%m/%d %H:%M")
          line = format '%-10s %s "%s"', h[:id], t, h[:mapper]
          putline line
        end
      else
        path = File.join log_dir, "#{job_id}.yml"
        els = {}
        open(path) {|io|
          s = YAML.load_stream io
          header, tasks, footer = s[0], s[1], s[2]
          if tasks
            for task_id, task in tasks.sort_by {|k, v| k}
              line = format '%5d %s %g',
                task_id, task['node_addr'], task['welapse']
              putline line
              node_addr = task['node_addr']
              els[node_addr] ||= [0, 0]
              els[node_addr][0] += 1
              els[node_addr][1] += task['welapse']
            end
          end
        }
        putline
        for node_addr, v in els
          putline format '%s %g/%d = %g',
            node_addr, v[1], v[0], v[1] / v[0]
        end
      end
    end

    def show_status addrs, options
      addrs = ['localhost'] if addrs.empty?
      adapter = StorageAdapter.create options[:storage_name], addrs
      msession = MRSession.new addrs, options
      msession.on_error {|addr, err| $stderr.printf "%s: %s\n", addr, err.to_s}
      msession.connect

      mf = msession.multicast_call_async :get_properties
      templ = "%s: %s %s, num_cpu=%s, ruby %s\n"
      mf.on_success {|f|
        props = f.get
        print templ % [props['hostname'],
          props['program_name'], props['VERSION'],
          props['num_cpu'], props['RUBY_VERSION']]
      }
      mf.on_error {|f| printf "%s: ERROR: %s\n", f.addr, f.error}
      mf.join_all

      msession.on_error {}
      mf = msession.multicast_call_async :quit
      mf.join_all
    end

    def lookup addrs, options
      adapter = StorageAdapter.create options[:storage_name], addrs
      name = options[:lookup]
      locator_host = options[:locator_host] || addrs.first || 'localhost'
      locator_port = options[:locator_port]
      adapter.connect_to_storage locator_host, locator_port
      adapter.get_files [name]
      locations = adapter.lookup_file name
      $stderr.puts "name: #{name}"
      for host, path in locations
        $stderr.puts "location: #{host}:#{path}"
      end
    end

    def run_mr addrs, options, argv=ARGV
      invoked_at = Time.now
      if options[:storage_name] == 'local' and addrs.empty?
        addrs = ['localhost']
      end
      adapter = StorageAdapter.create options[:storage_name], addrs, options
      locator_host = options[:locator_host] || addrs.first || 'localhost'
      locator_port = options[:locator_port]

      puts "storage: #{adapter.class}" if options[:verbose]
      begin
        adapter.connect_to_storage locator_host, locator_port
        files = adapter.get_files argv, options[:expand_glob]
      rescue
        STDERR.puts "Storage Error: #{$!}"
        abort
      end
      raise RuntimeError, "no hostname specified" if adapter.addrs.empty?

      msession = MRSession.new adapter.addrs, options
      msession.on_error {|addr, err|
        $stderr.printf "%s: %s\n", addr, err.inspect if err
      }
      msession.connect

      if options[:reducer]
        options[:num_r] ||= 1
      end
      dispatcher = TaskDispatcher.new options, adapter, msession
      job = Job.new options, files
      job[:invoked_at] = invoked_at
      job.mk_reducer_addrs adapter.addrs
      dispatcher.run job
      abort if job.failed
    end

    def optparse opts
      op = OptionParser.new
      op.on('--debug') {$debug = true; STDOUT.sync = true}
      op.on('--server') {opts[:server] = true}
      op.on('--argv=FILES') {}
      op.on('--brick=HOST:/DIR', '-b') {|arg| (opts[:bricks] ||= []).push arg}
      op.on('--config-file=FILE', '-F') {|arg| opts[:config_file] = arg}
      op.on('--disable-plugins') {opts[:disable_plugins] = true}
      op.on('--expand-glob') {opts[:expand_glob] = true}
      op.on('--ff=FF', Integer) {|arg| opts[:ff] = arg}
      op.on('--hosts=HOST,HOST,...') {|arg| opts[:hosts] = arg}
      op.on('--ipaddr=ADDR') {|arg| opts[:ipaddr] = arg}
      op.on('--locator-host=HOST') {|arg| opts[:locator_host] = arg}
      op.on('--locator-port=PORT', Integer) {|arg|
        opts[:locator_port] = arg}
      op.on('--lookup=FILE') {|arg| opts[:lookup] = arg}
      op.on('--mapper=CMD') {|arg| opts[:mapper] = arg}
      op.on('--reducer=CMD') {|arg| opts[:reducer] = arg}
      op.on('--num-r=NUM', Integer) {|arg| opts[:num_r] = arg}
      op.on('--root-dir=DIR') {|arg| opts[:root_dir] = arg}
      op.on('--ship-file=FILE', '--file=FILE') {|arg|
        (opts[:ship_files] ||= []).push arg}
      op.on('--show-joblog [job_id]') {|arg| opts[:show_joblog] = arg || true}
      op.on('--status') {opts[:status] = true}
      op.on('--storage=STORAGE_NAME') {|arg|
        opts[:storage_name] = arg}
      op.on('--verbose') {opts[:verbose] = true}
      op.on('--version') {
        puts "#{op.program_name} #{VERSION}"
        exit
      }
      class <<op
        attr_accessor :options
      end
      op.options = opts
      op
    end
  end
end
