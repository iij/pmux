require 'test/unit'

if RUBY_VERSION < '1.9.0'
  force_standalone = (RUBY_VERSION >= '1.8.3')
  exit Test::Unit::AutoRunner.run(force_standalone, '.')
else
  Test::Unit.setup_argv {|files|
    ['.']
  }
end
