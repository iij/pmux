require File.expand_path(File.dirname(__FILE__) + '/unittest_helper')

class TestTaskScheduler < Test::Unit::TestCase
  def test_map_task
    addrs = ['192.168.0.1', '192.168.0.2', '192.168.0.3',]
    adapter = Pmux::StorageAdapter.create 'local', addrs
    files = ['/tmp/f1', '/tmp/f2', '/tmp/f3', '/tmp/f4',]
    job = Pmux::Job.new({}, files)
    sch = Pmux::TaskScheduler.new adapter
    sent_tasks = {}
    sch.attach_flush_callback {|node_addr, task|
      (sent_tasks[node_addr] ||= []).push task[:task_id]
    }
    addrs.each {|addr| sch.shipped[addr] = true}
    sch.push_job job
    sch.process_queue
    ae [1, 4], sent_tasks['192.168.0.1']
    ae [2], sent_tasks['192.168.0.2']
    ae [3], sent_tasks['192.168.0.3']
  end

  def test_task_fusion
    addrs = ['192.168.0.1', '192.168.0.2', '192.168.0.3',]
    adapter = Pmux::StorageAdapter.create 'local', addrs
    files = (0..8).map {|n| "txt/#{n}.log"}
    job = Pmux::Job.new({:ff=>3}, files)
    sch = Pmux::TaskScheduler.new adapter
    sent_tasks = {}
    sch.attach_flush_callback {|node_addr, task|
      (sent_tasks[node_addr] ||= []).push task[:task_id]
    }
    addrs.each {|addr| sch.shipped[addr] = true}
    sch.push_job job
    sch.process_queue
    ae [1], sent_tasks['192.168.0.1']
    ae [2], sent_tasks['192.168.0.2']
    ae [3], sent_tasks['192.168.0.3']
  end
end
