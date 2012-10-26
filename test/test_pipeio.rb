require File.expand_path(File.dirname(__FILE__) + '/unittest_helper')
require 'mock_pipeio'

class TestPipeio < Test::Unit::TestCase
  def test_pipeio
    loop = Coolio::MockLoop.new
    loop.world = MockWorld.new

    cmd = "cat #{$test_dir}/txt/0.log"
    pipeio = Pmux::PipeIO.new cmd
    loop.attach pipeio
    str = nil
    pipeio.on_receive {|data|
      str = data
    }
    closed = false
    pipeio.on_close {closed = true}
    loop.world.run_once
    assert_match /START: \w+ /, str
    while !closed
      loop.world.run_once
    end
  end
end
