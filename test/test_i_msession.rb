require File.expand_path(File.dirname(__FILE__) + '/unittest_helper')

require 'mock_world'
require 'mock_mros'

class TestIMultiSession < Test::Unit::TestCase
  def setup_loop
    loop = Coolio::MockLoop.new
    loop.world = MockWorld.new
    loop
  end

  def test_mr_multicast_call_async
    loop = setup_loop
    msession = Pmux::MRSession.new ['192.168.0.1', '192.168.0.2'], {}, loop
    msession.connect

    mf = msession.multicast_call_async :get_properties
    mf.join_all
    mf.all.each {|f|
      assert_match /^192\.168\.0\.\d+$/, f.addr
      res = f.get
      assert_kind_of Hash, res
      ae RUBY_VERSION, res['RUBY_VERSION']
    }
  end
end
