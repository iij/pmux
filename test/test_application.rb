require File.expand_path(File.dirname(__FILE__) + '/unittest_helper')

class TestMain < Test::Unit::TestCase
  def test_optparse
    main = Pmux::Application.new
    options = {}
    optparser = main.optparse options
    assert_kind_of OptionParser, optparser
    optparser.parse!(['--root-dir=/tmp', '--status'])
    ae options[:root_dir], '/tmp'
    assert options[:status]
  end
end
