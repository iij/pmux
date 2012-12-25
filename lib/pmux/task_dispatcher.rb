module Pmux
  class TaskDispatcher
    attr_reader :options, :adapter
    attr_reader :msession, :scheduler, :gatherer
    attr_reader :mf_shuffle
    attr_reader :jl

    def initialize options, adapter, msession, gatherer=nil
      @options = options
      @adapter = adapter
      @msession = msession
      @gatherer = gatherer || Gatherer.new(STDOUTWriter.new)
      @scheduler = TaskScheduler.new adapter

      @verbose = options[:verbose]
      @on_error = proc {|r|
        $stderr.write "%s: %s, %s\n" %
          [r['error'], r['error_message'], r['backtrace']]
      }
    end

    def run job
      if job.num_r.nonzero?
        @mf_shuffle = MR::MultiFuture.new
      end
      @jl = Joblogger.new options[:log_dir], job
      scheduler.push_job job
      scheduler.attach_flush_callback {|node_addr, task|
        t = task.dup
        [:node_addrs, :alloc_time].each {|k| t.delete k}
        Log.info "send task #{t[:task_id]} to #{node_addr}"
        printf "send task %s to %s\n", t[:task_id], node_addr if @verbose
        future = msession.call_async node_addr, :exec_streaming_task, t
        future.attach_callback {|f| receive_result job, f.get}
        # err callback ?
      }

      # init job
      wdir = "#{options[:tmp_dir]}/w#{job.id}"
      Dir.mkdir wdir
      jl.dump_header
      jl.sep
      Log.init "#{options[:log_dir] or wdir}/dispatcher.log"

      puts 'send "init_job"' if @verbose
      mf_init = msession.multicast_call_async :init_job, job.id
      mf_init.on_success {|future|
        addr = future.addr
        res = future.get
        job_dir = adapter[addr][:job_dir] = res['job_dir'] # remote job_dir
        adapter[addr][:num_workers] = res['num_cpu'] || 2
        printf "%s: remote job_dir = %s\n", addr, res['job_dir'] if @verbose
        # scp ship_files to remote job_dir
        if (ship_files = options[:ship_files])
          mf_scp = msession.scp_upload_files addr, ship_files, job_dir
          mf_scp.on_all {
            scheduler.shipped[addr] = true
            scheduler.process_queue
          }
        else
          puts 'start scheduler' if @verbose
          scheduler.shipped[addr] = true
          scheduler.process_queue
        end
      }
      mf_init.on_error {job.taskhash.clear}

      # wait for all map tasks to finish
      until job.completed?
        msession.loop.run_once
      end
      if job.num_r.zero?
        gatherer.join_all
      else
        mf_shuffle.join_all
        # reduce phase
        job.mk_reduce_tasks
        scheduler.inject_tasks job.tasks
        scheduler.process_queue
        # wait for all reduce tasks to finish
        until job.completed?
          msession.loop.run_once
        end
        gatherer.join_all
      end

      Log.info "END"
      job[:job_finished_at] = Time.now
      jl.dump_footer
      jl.close

      mf_quit = msession.multicast_call_async :quit
      mf_quit.join_all

      cleaner = Cleaner.new "#{options[:tmp_dir]}/w*",
        "#{options[:log_dir]}/*.yml"
      cleaner.run
    end

    def on_error &block
      @on_error = block
    end

    def receive_result job, result
      task_id, node_addr, ifbase =
        result.values_at 'task_id', 'node_addr', 'ifbase'
      Log.info "receive result #{task_id} from #{node_addr}"
      puts "receive result #{task_id} from #{node_addr}" if @verbose
      if result['error']
        if @on_error
          @on_error.call result
        else
        end
      end

      if (ifbase = result['ifbase'])
        if job.num_r.zero?
          # no reducers; get the intermediate file from the mapper node
          ifpath = ifbase + '-0'
          local = "#{@options[:tmp_dir]}/w#{job.id}/#{File.basename(ifpath).sub /^[mr]/, 'w'}"
          if (body = result['result_body'])
            if body.size > 0
              open(local, 'w') {|f| f.write body}
              gatherer.writer.write local
            end
          else
            puts "gather #{node_addr}:#{ifpath} -> #{local}" if @verbose
            Log.info "gather #{node_addr}:#{ifpath} -> #{local}"
            gatherer.gather msession, node_addr, ifpath, local
          end
        else
          # send 'notify_reduce' message to the reducer node
          job.reducers.each_with_index {|reducer_addr, pindex|
            puts "send notify_reduce #{task_id} to #{reducer_addr}" if @verbose
            future = msession.call_async reducer_addr, :notify_reduce,
              :job_id=>job.id, :task_id=>task_id, :pindex=>pindex,
              :node_addr=>node_addr, :ifbase=>ifbase
            mf_shuffle.add future
          }
        end
      elsif (output_path = result['output_path'])
        # reduced result
        local = "#{@options[:tmp_dir]}/w#{job.id}/#{File.basename(output_path).sub /^[mr]/, 'w'}"
        gatherer.gather msession, node_addr, output_path, local
      end

      task = job.get_task_by_id task_id
      alloc_time = task[:alloc_time]
      allocated_at = alloc_time - job[:job_started_at]
      elapse = Time.now - alloc_time if alloc_time
      task[:welapse] = result['welapse']
      jl.add task_id, :node_addr=>node_addr, :ifbase=>ifbase,
        :welapse=>result['welapse'], :elapse=>elapse,
        :allocated_at=>allocated_at

      # delete task
      scheduler.delete_task_from_job job, task, node_addr
      if (task_keys = result['task_keys'])
        for tid in task_keys.keys
          job.delete_task_by_id tid
        end
      end

      scheduler.process_queue

      perc = 100 * (job.tasks.size - job.taskhash.size) / job.tasks.size
      task_type = result['map'] ? 'map' : 'reduce'
      Log.info "done #{task_type} task #{result['task_id']} (#{perc}%)"
    end
  end
end
