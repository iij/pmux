require 'yaml'

module Pmux
  class Joblogger
    attr_reader :path

    def initialize dir, job
      @dir = dir
      @job = job
      if dir and File.directory? dir
        @path = "#{dir}/#{job.id}.yml"
        @f = open(@path, 'w')
      end
    end

    def dump_header
      dump @job.to_jlheader
    end

    def dump_footer
      dump @job.to_jlfooter
    end

    def dump obj
      @f.print YAML.dump(obj) if @f
    end

    def sep
      return unless @f
      @f.puts '---'
      @f.flush
    end

    def add key, obj
      return unless @f
      @f.puts "#{key}:"
      for k, v in obj
        @f.puts "  #{k}: #{v.inspect}"
      end
    end

    def close
      @f.close if @f
    end
  end
end
