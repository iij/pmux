module Pmux
  module FixCmdLine
    def fix_cmd_line cmd_line, in_path=nil, out_path=nil, err_path=nil, tmp_dir=nil
      res = []
      cmds = cmd_line.split /\s*\|\s*/
      n = 0
      for cmd in cmds
        c, = cmd.split
        if tmp_dir and File.executable?(cc = "#{tmp_dir}/#{c}")
          #cmd = cc
          cmd = "#{tmp_dir}/#{cmd}"
        end
        if n == 0
          cmd << " #{in_path}" if in_path
          err_path ||= '/dev/null'
          cmd << " 2>#{err_path}"
        end
        res.push cmd
        n += 1
      end
      cmd << " >>#{out_path}" if out_path
      res.join '|'
    end
  end
end
