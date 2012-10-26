require File.expand_path(File.dirname(__FILE__) + '/unittest_helper')

class TestJob < Test::Unit::TestCase
  def test_job_id
    job = Pmux::Job.new({}, [])
    assert_match /^\d+$/, job.id
  end

  def test_mk_tasks
    files = ['/tmp/f1', '/tmp/f2', '/tmp/f3']
    params = {}
    job = Pmux::Job.new params, files
    ae files.size, job.tasks.size
  end

  def test_mk_reducer_addrs
    job = Pmux::Job.new({}, [])

    # 1 node
    addrs = ['192.168.0.1']
    res = job.mk_reducer_addrs addrs, 1
    ae ['192.168.0.1'], res
    res = job.mk_reducer_addrs addrs, 3
    ae ['192.168.0.1', '192.168.0.1', '192.168.0.1'], res

    # 4 nodes
    addrs = ['192.168.0.1', '192.168.0.2', '192.168.0.3', '192.168.0.4']
    res = job.mk_reducer_addrs addrs, 1
    ae ['192.168.0.1'], res
    res = job.mk_reducer_addrs addrs, 2
    ae ['192.168.0.1', '192.168.0.3'], res
    res = job.mk_reducer_addrs addrs, 3
    ae ['192.168.0.1', '192.168.0.2', '192.168.0.3'], res
  end
end
