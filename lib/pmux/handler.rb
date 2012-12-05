require 'fileutils'

module Pmux
  class Handler
    attr_reader :options
    attr_accessor :server

    def initialize server=nil, options={}
      @server = server
      @options = options
      @ipaddr = options[:ipaddr]

      @wtq = []
      @wq = []
      @ases = {}
      @msession = nil
      @seqid = 0
    end

    def init_job job_id
      path = "#{options[:tmp_dir]}/#{job_id}"
      Dir.mkdir path
      options[:job_dir] = path
      Log.init "#{path}/worker.log", :log_level=>'debug'

      num_cpu = get_num_cpu
      #fork_worker num_cpu, options

      {
        'job_dir' => path,
        'num_cpu' => num_cpu,
      }
    end

    def get_num_cpu
      cpuinfo_path = '/proc/cpuinfo'
      if File.exist? cpuinfo_path
        lines = File.readlines(cpuinfo_path).grep(/^processor/)
        lines.size
      else
        2
      end
    end

    # execute a task and return the result
    def exec_streaming_task task
      start_time = Time.now
      as = MR::AsyncResult.new
      if (task_keys = task['task_keys'])
        error_ids = []
        fusion_id = task['task_id']
        fiber = Fiber.new {
          for task_id, file in task_keys
            ntask = task.merge 'fusion_id'=>fusion_id,
              'task_id'=>task_id, 'path'=>file
            result = do_one_task ntask, fiber
          end
          result.update :task_id=>fusion_id, :task_keys=>task_keys,
            :welapse=>(Time.now - start_time)
          as.result result
        }
      else
        fiber = Fiber.new {
          result = do_one_task(task, fiber)
          result[:welapse] = Time.now - start_time
          as.result result
        }
      end
      fiber.resume
      as
    end

    def do_one_task task, fiber=nil
      job_id, task_id, node_addr =
        task.values_at 'job_id', 'task_id', 'node_addr'
      num_r = task['num_r'].to_i
      result = {:job_id=>job_id, :task_id=>task_id, :node_addr=>node_addr}
      exception = nil
      error_level = nil
      if @options and (otmp_dir = @options[:tmp_dir])
        tmp_dir = "#{otmp_dir}/#{job_id}"
      else
        tmp_dir = "/var/tmp/#{job_id}"
      end
      if task['pindex']
        # reduce task
        result[:reduce] = true
        reducer = StreamingReducer.new task, tmp_dir, @server.loop
        reducer.on_success {
          result[:output_path] = reducer.output_path
          fiber.resume result if fiber
        }
        reducer.on_error {|e|
          result.update :error=>e.class.to_s,
            :error_message=>e.message, :backtrace=>e.backtrace[0]
          fiber.resume result if fiber
        }
        reducer.do_streaming_reduce_task
      else
        # map task
        result[:map] = true
        mapper = StreamingMapper.new task, tmp_dir, @server.loop
        #result[:ifbase] = mapper.do_map_task
        mapper.on_success {
          result[:ifbase] = mapper.ifbase
          if num_r.zero?
            if (rsize = mapper.result_size)
              result[:result_body] = mapper.result_body if rsize < 1024
            else
              result[:result_body] = ''
            end
          end
          fiber.resume result if fiber
        }
        mapper.on_error {|e|
          result.update :error=>e.class.to_s,
            :error_message=>e.message, :backtrace=>e.backtrace[0]
          fiber.resume result if fiber
        }
        mapper.do_streaming_map_task
      end
      Fiber.yield
    end
    private :do_one_task

    def notify_reduce params
      job_id, task_id, pindex, node_addr, ifbase =
        params.values_at 'job_id', 'task_id', 'pindex', 'node_addr', 'ifbase'
      ifpath = "#{ifbase}-#{pindex}"
      Log.debug "H: notify_reduce #{job_id}-#{task_id} #{ifbase}"

      if @ipaddr == node_addr
        # local
        local = "#{options[:job_dir]}/#{File.basename(ifpath).sub(/^m/, 't')}"
        File.rename ifpath, local
        {:job_id=>job_id, :task_id=>task_id, :ifbase=>ifbase}
      else
        # remote
        @msession ||=
          MultiSession.new([], {:user=>@options[:user]}, @server.loop)
        @msession.connect_to_addr node_addr
        local = "#{options[:job_dir]}/#{File.basename(ifpath).sub(/^m/, 't')}"
        future = @msession.scp_download node_addr, ifpath, local
        future.attach_callback {|f|
          if (as = @ases.delete "r#{job_id}-#{task_id}")
            as.result :job_id=>job_id, :task_id=>task_id, :ifbase=>ifbase
          end
        }
        @ases["r#{job_id}-#{task_id}"] = MR::AsyncResult.new
      end
    #rescue Exception
    end


    def init_scan addrs
      log_path = "#{options[:log_dir]}/diffuser.log"
      Log.init log_path, :log_level=>'debug'
      @adapter = StorageAdapter.create 'pmuxfs', addrs
      @fs_dir = options[:fs_dir]
      @adapter.set_fs_dir @fs_dir
      @fs_dir
    end

    def scan_once
      files = @adapter.find
    end

    def close_download_channel node_addr
      @msession.close_channel node_addr if @msession
      @msession.class.to_s
    end


    def get_status
      [
        ['ruby_version', RUBY_VERSION, :string],
        ['hoge', 1, :gauge],
      ]
    end

    def get_properties
      {
        'hostname' => Socket.gethostname,
        'program_name' => options[:program_name],
        'root_dir' => options[:root_dir],
        'tmp_dir' => options[:tmp_dir],
        'VERSION' => VERSION,
        'RUBY_VERSION' => RUBY_VERSION,
        'num_cpu' => get_num_cpu,
      }
    end

    def hello
      'hello'
    end

    def ls dirs, args
      res = []
      for dir in dirs
        for arg in args
          Dir.chdir(dir) {
            res += Dir.glob(arg).select {|path|
              File.readable? path}.map {|path|
              [path, File.join(dir, path)]
            }
          }
        end
      end
      res
    end

    def quit
      @server.loop.stop
      cleaner = Cleaner.new "#{options[:tmp_dir]}/[0-9]*"
      cleaner.run
      nil
    end

=begin
    private
    def fork_worker num_worker, options
      for n in 1..num_worker
        pid = fork {
          @server.loop.stop
          begin
            worker = Worker.new options
          rescue Timeout::Error
            Log.info "worker #{$$}: initialization timeout"
            worker = nil
          end
          if worker
            worker.run
            Log.puts 'worker exit'
          end
          exit
        }
      end
    end

    def enq_task task
      Log.debug "H: enq_task #{task['job_id']}-#{task['task_id']}"
      @wtq.push task
      process_task_queue
      if $test and @fiber
        @fiber.resume
      end
      @ases["#{task['job_id']}-#{task['task_id']}"] = MR::AsyncResult.new
    end

    def get_task
      Log.debug "H: get_task"
      if @wtq.empty?
        as = MR::AsyncResult.new
        @wq.push as
        as
      else
        @wtq.shift
      end
    end

    def put_result result
      Log.debug "H: put_result #{result['task_id']}"
      if (as = @ases.delete "#{result['job_id']}-#{result['task_id']}")
        Log.debug "H: return as.result"
        as.result result
      end
      nil
    end

    def process_task_queue
      while !@wtq.empty? and (as = @wq.shift)
        as.result @wtq.shift
      end
    end
=end
  end
end
