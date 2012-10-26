module Pmux
  class Reducer
    attr_reader :exitstatus, :output_path
    attr_accessor :tmp_dir

    def initialize task, tmp_dir, loop=nil
      @task = task
      @tmp_dir = tmp_dir
      @loop = loop
      @paths = Dir.glob("#{tmp_dir}/t*-#{task['pindex']}")
      @output_path = nil
      @on_receive = nil
      @on_success = nil
      @on_error = nil
      raise RuntimeError, 'no input files' if @paths.empty?
    end

    def do_reduce_task; end
  end

  class StreamingReducer < Reducer
    include FixCmdLine

    def on_receive &block
      @on_receive = block
    end
    def on_success &block
      @on_success = block
    end
    def on_error &block
      @on_error = block
    end

    def do_reduce_task
      reducer_cmd = @task['reducer'] || 'cat'
      @output_path = "#{@tmp_dir}/r#{@task['pindex']}"
      err_path = "#{@tmp_dir}/.err.#{$$}"
      err_msg = nil
      cmd_line = fix_cmd_line reducer_cmd,
        @paths.join(' '), @output_path, err_path, tmp_dir
      Log.debug "system: #{cmd_line}"
      system cmd_line
      @exitstatus = $?.exitstatus
      if File.size? err_path
        err_msg = File.read(err_path).chomp!
        raise RuntimeError, err_msg
      end
      if @exitstatus > 1
        raise RuntimeError, "failed to execute reducer: #{cmd_line}"
      end
      @output_path
    end

    def do_streaming_reduce_task
      reducer_cmd = @task['reducer'] || 'cat'
      @output_path = "#{@tmp_dir}/r#{@task['pindex']}"
      err_path = "#{@tmp_dir}/.rerr.#{$$}"
      err_msg = nil
      cmd_line = fix_cmd_line reducer_cmd,
        @paths.join(' '), nil, err_path, tmp_dir
      Log.debug "popen: #{cmd_line}"
      pipeio = PipeIO.new cmd_line
      if @on_receive
        pipeio.on_receive &@on_receive
      else
        out = open(@output_path, 'a')
        pipeio.on_receive {|data|
          out.write data
        }
      end
      on_success = @on_success
      on_error = @on_error
      pipeio.on_close {
        if out
          out.close rescue nil
        end
        if File.size? err_path
          err_msg = File.read(err_path).chomp!
          #raise RuntimeError, err_msg
          e = RuntimeError.new err_msg
          e.set_backtrace ['reducer']
          on_error.call e if on_error
        else
          on_success.call self if on_success
        end
      }
      @loop.attach pipeio
    end
  end
end
