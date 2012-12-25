require 'forwardable'

module Pmux
  class Job
    extend Forwardable
    def_delegators :@h, :[], :[]=, :delete
    attr_reader :tasks, :num_t, :num_r
    attr_reader :taskhash
    attr_reader :reducers

    def initialize params, files
      @params = params
      @files = files

      @task_id = 0
      @num_r = @params[:num_r] || 0
      @params[:job_name] ||= %Q{"#{@params[:mapper]}"}

      @taskhash = {}
      @done_taskhash = {}
      @tasks = mk_tasks files
      @num_t = @tasks.size + @num_r
      @h = {:job_started_at=>Time.now,
        :map_tasks=>@tasks.size, :reduce_tasks=>@num_r,
      }
    end

    def mk_tasks files
      job_id = self.id
      files.map {|file|
        @task_id += 1
        @taskhash[@task_id] = {:job_id=>job_id, :task_id=>@task_id,
          :file=>file,
          :mapper=>@params[:mapper], #:reducer=>@params[:reducer],
          :num_r=>@num_r, :ff=>@params[:ff],
          :separator=>@params[:separator],
          :hist=>[],
        }
      }
    end

    def mk_reducer_addrs addrs, num_r=nil
      num_r ||= @num_r
      step = addrs.size.to_f / num_r
      @reducers = (0..num_r-1).map {|ind| addrs[step*ind]}
      @reducers
    end

    def mk_reduce_tasks
      pindex = 0
      job_id = self.id
      @tasks = reducers.map {|reducer_node_addr|
        @task_id += 1
        #task = make_reduce_task pindex, reducer_node_addr
        task = {:pindex=>pindex, :job_id=>self.id, :task_id=>@task_id,
          :node_addr=>reducer_node_addr,
          :reducer=>@params[:reducer],
          :hist=>[],
        }
        @taskhash[@task_id] = task
        pindex += 1
        task
      }
    end

    def id
      self.object_id.abs.to_s
    end

    def get_task_by_id task_id
      @taskhash[task_id]
    end

    def delete_task_by_id task_id
      if (task = @taskhash[task_id])
        task[:status] = :DONE
        @done_taskhash[task_id] = task
        @taskhash.delete task_id
      end
    end

    def completed?
      @taskhash.empty?
    end

    def to_jlheader
      h = {:id=>id, :files_first=>@files.first, :tasksize=>@tasks.size,
        :params=>@params,
        :invoked_at=>@h[:invoked_at], :job_started_at=>@h[:job_started_at],
        :map_tasks=>@h[:map_tasks], :reduce_tasks=>@h[:reduce_tasks],
        :storage_name=>@params[:storage_name],
        :mapper=>@params[:mapper], :reducer=>@params[:reducer],
        :num_r=>@params[:num_r],
      }
    end

    def to_jlfooter
      h = {:job_finished_at=>@h[:job_finished_at]}
    end
  end
end
