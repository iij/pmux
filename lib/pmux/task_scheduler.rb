
module Pmux
  class TaskScheduler
    attr_reader :shipped

    def initialize adapter=nil
      @adapter = adapter
      @node_table = adapter
      @job_table = {}
      @task_queue = TaskQueue.new
      @allocated_tasks = {}
      @shipped = {}
    end

    def push_job job
      @job_table[job.id] = job
      @task_queue.inject_tasks job.tasks
    end

    def inject_tasks tasks
      @task_queue.inject_tasks tasks
    end

    def delete_task_from_job job, task, node_addr=nil
      node_addr ||= task[:node_addr]
      remove_allocated_task node_addr, job.id, task[:task_id]
      job.delete_task_by_id task[:task_id]
    end

    def process_queue
      job_map = {}
      success_count = 0
      fail_count = 0
      fail_lim = @node_table.size * 2
      tmp_queue = []

      task_queue_size = @task_queue.size
      loop_count = 0
      while task = @task_queue.shift
        if task[:pindex] and task[:node_addr]
          allocated_p = allocate_reduce_task_to_node job_map, task
        else
          unless task[:node_addrs]
            task[:node_addrs] = @adapter.lookup_file task[:file]
          end
          allocated_p = allocate_map_task_to_node job_map, task
        end
        if allocated_p
          # success
          fail_count = 0
        else
          # fail
          tmp_queue.push task
          fail_count += 1
        end

        loop_count += 1

        break if loop_count >= task_queue_size
      end
      @task_queue.replace tmp_queue + @task_queue

      flush_job_map job_map
    end

    #
    def attach_flush_callback &block
      @flush_callback = block
    end

    #
    def flush_job_map job_map
      for job_id, nt_map in job_map
        for node_addr, fslots in nt_map
          for fslot in fslots
            if fslot.size > 1
              task_keys =
                Hash[*(fslot.map {|t| [t[:task_id], t[:path]]}).flatten]
              task = fslot.first.merge :task_keys=>task_keys
              @flush_callback.call node_addr, task if @flush_callback
            else
              @flush_callback.call node_addr, fslot.first if @flush_callback
            end
          end
        end
      end
    end

    def allocate_task_to_node job_map, task, node_addr
      if (ff = task[:ff]) and !task[:pindex]
        # task fusion
        job_id = task[:job_id]
        if (nt_map = job_map[job_id]) and (fslots = nt_map[node_addr])
          for fslot in fslots
            if fslot.size < ff
              fslot.push task
              return true
            end
          end
        end
      end

      #if @node_table.allocate_task node_addr, task
      if allocate_task_to_slot node_addr, task
        # success
        job_id = task[:job_id]
        nt_map = (job_map[job_id] ||= {})

        #task = task.dup #???
        task[:alloc_time] = Time.now
        (nt_map[node_addr] ||= []).push [task]

        return true
      end
      return false
    end

    #
    def allocate_task_to_slot node_addr, task
      if (node = @node_table[node_addr]) and @shipped[node_addr]
        slot = (@allocated_tasks[node_addr] ||= [])
        num_workers = node[:num_workers] || 2
        if slot.size >= num_workers
          return false
        else
          return false if slot.include? task
          task[:node_addr] = node_addr
          slot.push task
          return true
        end
      end
    end

    #
    def remove_allocated_task node_addr, job_id, task_id
      if (slot = @allocated_tasks[node_addr])
        slot.delete_if {|t| t[:job_id] == job_id and t[:task_id] == task_id}
      end
    end

    #
    def allocate_map_task_to_node job_map, task
      node_addrs = task[:node_addrs].dup
      for node_addr, path in node_addrs
        next unless @node_table[node_addr]
        next unless path
        task[:path] = path
        if allocate_task_to_node job_map, task, node_addr
          task[:node_addrs].delete [node_addr, path]
          return true
        end
      end
      return false
    end

    #
    def allocate_reduce_task_to_node job_map, task
      return false unless (node_addr = task[:node_addr])
      if allocate_task_to_node job_map, task, node_addr
        return true
      else
        return false
      end
    end
  end
end
