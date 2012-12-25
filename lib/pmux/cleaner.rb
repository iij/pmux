require 'fileutils'

module Pmux
  class Cleaner
    def initialize *glob_pats
      @glob_pats = glob_pats
    end

    def run lim_time=nil
      fork {fork {@glob_pats.each {|glob_pat| clean glob_pat, lim_time}}}
      Process.wait
    end

    def clean glob_pat=nil, lim_time=nil
      glob_pat ||= @glob_pat
      lim_time ||= Time.now - 3600*24*7
      paths = Dir.glob glob_pat
      for path in paths
        if File.exist? path
          mtime = File.mtime path
          if mtime < lim_time
            FileUtils.rm_rf path
          end
        end
      end
    end
  end
end
