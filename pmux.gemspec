# -*- encoding: utf-8 -*-
$:.push File.expand_path("../lib", __FILE__)
require "pmux/version"

Gem::Specification.new do |s|
  s.name        = "pmux"
  s.version     = Pmux::VERSION
  s.authors     = ["maebashi"]
  s.homepage    = ""
  s.summary     = %q{lightweight file-based MapReduce system}
  s.description = %q{lightweight file-based MapReduce system}

  s.rubyforge_project = "pmux"

  s.files         = `git ls-files`.split("\n").select {|e| /^tmp/!~e}
  s.test_files    = `git ls-files -- {test,spec,features}/*`.split("\n")
  s.executables   = `git ls-files -- bin/*`.split("\n").map{ |f| File.basename(f) }
  s.require_paths = ["lib"]

  # specify any dependencies here; for example:

  s.add_runtime_dependency "msgpack-rpc"
  s.add_runtime_dependency "net-scp"
end
