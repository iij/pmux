require File.expand_path(File.dirname(__FILE__) + '/unittest_helper')

require 'fileutils'

include Pmux::FixCmdLine

class TestFixCmdLine < Test::Unit::TestCase
  def test_fix_cmd_line
    mapper = 'cat'
    res = fix_cmd_line 'grep PAT', 'input'
    ae "grep PAT input 2>/dev/null", res
    res = fix_cmd_line 'grep PAT|uniq', 'input'
    ae "grep PAT input 2>/dev/null|uniq", res
    res = fix_cmd_line 'grep PAT|uniq', 'input', 'output'
    ae "grep PAT input 2>/dev/null|uniq >>output", res
  end
end
