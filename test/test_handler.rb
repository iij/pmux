require File.expand_path(File.dirname(__FILE__) + '/unittest_helper')

class TestHandler < Test::Unit::TestCase
  def setup
    @handler = Pmux::Handler.new
  end

  def test_get_num_cpu
    assert(@handler.get_num_cpu > 0)
  end

  def _test_get_properties
    p @handler.get_properties
  end
end
