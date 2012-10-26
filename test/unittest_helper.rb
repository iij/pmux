require 'rubygems'
require 'test/unit'
require 'pp'

$test_dir = File.expand_path File.dirname(__FILE__)
$:.unshift $test_dir
$:.unshift($test_dir + '/../lib')
require 'pmux'
$test = true
$debug = true

begin
  require 'rubygems'
  require 'test/unit/ui/console/testrunner'
  require 'term/ansicolor'
  Term::ANSIColor::coloring = STDERR.tty?
  module Test
    module Unit
      module UI
	module Console
	  class TestRunner
	    include Term::ANSIColor
	    def output_single(something, level=NORMAL)
	      if output?(level)
		@io.write(send(something == '.' ? 'on_green' : 'on_red', something))
	      end
	      @io.flush
	    end
	    def output(something, level=NORMAL)
	      if output?(level)
		if something.respond_to?('passed?')
		  @io.puts(send(something.passed? ? 'on_green' : 'on_red', something.to_s))
		else
		  @io.puts(something)
		end
	      end
	      @io.flush
	    end
	  end
	end
      end
    end
  end
rescue LoadError
end

module Test
  module Unit
    module Assertions
      alias ae assert_equal
    end
  end
end

unless ARGV.grep(/--show-log/).empty?
  $show_log = true
end
