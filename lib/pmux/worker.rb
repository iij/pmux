require 'timeout'

module Pmux
  class Worker
    def initialize options, client=nil
      @options = options
      unless (@client = client)
        timeout(3) {
          @client = init_client(options[:sock_path])
          @client.timeout = 3600
        }
      end
    end

    def init_client sock_path
      transport = MR::UNIXTransport.new
      MR::Client.new transport, sock_path
    end

    def run
      Log.debug "W#{$$}: run"
      while true
        task = @client.call :get_task
        result = exec_task task
        @client.call :put_result, result
      end
      Log.debug "W#{$$}: end"
    rescue MR::TimeoutError
      Log.debug "W#{$$}: request timed out"
    rescue MR::TransportError
      Log.debug "W#{$$}: transport closed"
    end

    def exec_task task
      start_time = Time.now
      if task['task_keys']
        error_ids = []
        fusion_id = task['task_id']
        for task_id, file in task['task_keys']
          ntask = task.merge 'fusion_id'=>fusion_id,
            'task_id'=>task_id, 'path'=>file
          result = do_one_task ntask
          result.update :task_id=>fusion_id, :task_keys=>task['task_keys']
        end
      else
        result = do_one_task task
      end
      result[:welapse] = Time.now - start_time
      result
    end

    def do_one_task task
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
      begin
        if task['pindex']
          # reduce task
          result[:reduce] = true
          reducer = StreamingReducer.new task, tmp_dir
          result[:output_path] = reducer.do_reduce_task
        else
          # map task
          result[:map] = true
          mapper = StreamingMapper.new task, tmp_dir
          result[:ifbase] = mapper.do_map_task
          if num_r.zero?
            if (rsize = mapper.result_size)
              result[:result_body] = mapper.result_body if rsize < 1024
            else
              result[:result_body] = ''
            end
          end
        end
      rescue StandardError => e
        #q $!, $@[0] if $test
        result.update :error=>e.class.to_s,
          :error_message=>e.message, :backtrace=>e.backtrace[0]
      end
      result
    end
  end
end
