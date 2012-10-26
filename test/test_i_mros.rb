require File.expand_path(File.dirname(__FILE__) + '/unittest_helper')

require 'mock_world'
require 'mock_mros'

class TestIMros < Test::Unit::TestCase
  def test_server
    world = MockWorld.new

    options = {}
    server = MR::Server.new
    handler = Pmux::Handler.new server, options
    transport = MR::MockPipeTransport.new
    server.listen transport, handler
    server.run

    # get_status handler test
    bin = [0, 0, 'get_status', []].to_msgpack
    transport.receive_data bin
    msg = MessagePack.unpack transport.data
    # [1, 0, nil, [['key', value, :type], [], ...]

    ae 1, msg[0]
    ae 0, msg[1]
    assert_nil msg[2]
    assert_kind_of Array, msg[3]
  end
end
