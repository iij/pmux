require File.expand_path(File.dirname(__FILE__) + '/unittest_helper')

class TestMain < Test::Unit::TestCase
  def test_show_joblog
    main = Pmux::Application.new
    class <<main
      attr_reader :lines
      def putline line=''; (@lines||=[]).push line; end
    end
    log_dir = File.expand_path(File.dirname(__FILE__) + '/log')

    main.show_joblog :log_dir=>log_dir, :show_joblog=>true
    ae 1, main.lines.size
    main.lines.clear
    main.show_joblog :log_dir=>log_dir, :show_joblog=>'1234567890'
    ae 10, main.lines.size
  end

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
