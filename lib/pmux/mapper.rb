module Pmux
  class Mapper
    attr_accessor :tmp_dir
    attr_accessor :num_r
    attr_reader :ifbase
    attr_reader :exitstatus

    def initialize task, tmp_dir, loop=nil
      @task = task
      @tmp_dir = tmp_dir
      @exitstatus = nil
      job_id, task_id, fusion_id =
        task.values_at 'job_id', 'task_id', 'fusion_id'
      @path = task['path']
      @num_r = task['num_r'] || 0
      @loop = loop
      @on_receive = nil
      @on_success = nil
      @on_error = nil
      if false
      else
        @ifbase = "#{tmp_dir}/m#{fusion_id or task_id}"
      end
    end

    def do_map_task; end
    def result_size; end
    def result_body; end
  end

  class StreamingMapper < Mapper
    include FixCmdLine
    CHUNK_SIZE = 8192

    def on_success &block
      @on_success = block
    end
    def on_error &block
      @on_error = block
    end

    def do_map_task
      mapper_cmd = @task['mapper'] || 'cat'
      err_path = "#{tmp_dir}/.err.#{$$}"
      err_msg = nil
      if @num_r <= 1
        cmd_line = fix_cmd_line mapper_cmd,
          @path, "#{@ifbase}-0", err_path, tmp_dir
        Log.debug "system: #{cmd_line}"
        system cmd_line
      else # @num_r >= 2
        partitioner = TextPartitioner.new @ifbase, @num_r,
          :separator=>@task['separator']
        cmd_line = fix_cmd_line mapper_cmd, @path, nil, err_path, tmp_dir
        IO.popen(cmd_line, 'r') {|io|
          until io.eof?
            data = io.read CHUNK_SIZE
            partitioner.emit data
          end
        }
        partitioner.close
      end
      @exitstatus = $?.exitstatus
      if File.size? err_path
        err_msg = File.read(err_path).chomp!
        raise RuntimeError, err_msg
      end
      if @exitstatus > 1
        raise RuntimeError, "failed to execute mapper: #{cmd_line}"
      end
      @ifbase
    end

    def do_streaming_map_task
      mapper_cmd = @task['mapper'] || 'cat'
      err_path = "#{tmp_dir}/.err.#{object_id}"
      err_msg = nil
      pipeio = nil
      if @num_r <= 1
        cmd_line = fix_cmd_line mapper_cmd,
          @path, nil, err_path, tmp_dir
        Log.debug "pipe: #{cmd_line}"
        Dir.chdir(tmp_dir) {pipeio = PipeIO.new cmd_line}
        out = open("#{@ifbase}-0", 'a')
        pipeio.on_receive {|data| out.write data}
      else # @num_r >= 2
        partitioner = TextPartitioner.new @ifbase, @num_r,
          :separator=>@task['separator']
        cmd_line = fix_cmd_line mapper_cmd, @path, nil, err_path, tmp_dir
        Dir.chdir(tmp_dir) {pipeio = PipeIO.new cmd_line}
        pipeio.on_receive {|data| partitioner.emit data}
      end
      on_success = @on_success
      on_error = @on_error
      pipeio.on_close {
        if out
          out.close rescue nil
        end
        if partitioner
          partitioner.close
        end
        #@exitstatus = $?.exitstatus
        if File.size? err_path
          err_msg = File.read(err_path).chomp!
          e = RuntimeError.new err_msg
          e.set_backtrace ['mapper']
          on_error.call e if on_error
        else
          on_success.call if on_success
        end
      }
      @loop.attach pipeio
    end

    def result_size
      @num_r <= 1 ? File.size?("#{@ifbase}-0") : nil
    end

    def result_body
      @num_r <= 1 ? File.read("#{@ifbase}-0") : nil
    end
  end

  class TextPartitioner
    def initialize ifbase, num_r, options={}
      @ifbase = ifbase
      @num_r = num_r
      @ifiles = (0..(num_r-1)).map {|n| open("#{ifbase}-#{n}", 'w')}
      @rbuf = ''
      if (sep = options[:separator])
        @separator_re = Regexp.new sep
      else
        @separator_re = /\t/
      end
    end

    def emit data
      @rbuf << data
      while true
        break unless @rbuf =~ /\n/
        line, s = @rbuf.split /^/, 2
        key, = line.split @separator_re, 2
        @ifiles[key.hash % @num_r].write line
        @rbuf.replace(s || '')
      end
    end

    def close
      @ifiles.each {|io| io.close}
    end
  end
end
