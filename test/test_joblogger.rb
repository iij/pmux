require File.expand_path(File.dirname(__FILE__) + '/unittest_helper')

class TestJoblogger < Test::Unit::TestCase
  def test_j
    jl = Pmux::Joblogger.new nil, nil
    assert_nil jl.path

    job = Pmux::Job.new({:job_name=>'unknown'}, [])
    jl = Pmux::Joblogger.new '/var/tmp', job
    jl.dump job.to_jlheader
    assert_match %r{/var/tmp/\d+\.yml}, jl.path
    jl.close
    assert File.size?(jl.path)
    File.unlink jl.path
  end
end
