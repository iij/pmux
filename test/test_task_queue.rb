require File.expand_path(File.dirname(__FILE__) + '/unittest_helper')

class TestTaskQueue < Test::Unit::TestCase
  def test_task_queue
    tq = Pmux::TaskQueue.new

    # push
    v = {:id=>0}
    tq.push(v)
    tq.push({:id=>1})
    tq.push({:id=>2, :job_id=>99})
    ae 3, tq.size

    ae 2, tq.at(2)[:id]
    assert_nil tq.at(3)

    # inject_tasks, shift, delete_job
    tq.inject_tasks [{:id=>3}, {:id=>4}, {:id=>5}, {:id=>6}, {:id=>7}]
    #tq.each {|v| p v}
    ae 8, tq.size
    a = tq.shift
    ae 0, a[:id]
    ae 7, tq.size
    a = tq.shift
    ae 1, a[:id] # ae 3, a[:id]
    ae 6, tq.size
    tq.delete_job 99
    ae 5, tq.size

    # clear
    tq.clear
    ae 0, tq.size

    # inject, shift
    tasks = (0..9).map {|i|{:id=>i, :job_id=>99}}
    tq.inject_tasks tasks
    tmpq = Pmux::TaskQueue.new
    while item = tq.shift
      if item[:id] % 2 == 0
        item.delete :_f #XXX
        tmpq.push item
      end
      break if item[:id] == 6
    end

    # delete
    tq.delete_job 99
    ae 0, tq.size

    # clear, inject
    tmpq.clear
    tmpq.inject_tasks [{:id=>0}, {:id=>1}]
    ae 2, tmpq.size
  end

  # inject_tasks ĎņŹň
  def test_inject_tasks
    tq = Pmux::TaskQueue.new
    ae 0, tq.size

    tasks = []
    tq.inject_tasks tasks
    ae 0, tq.size

    tasks = [{:id=>0}, {:id=>1}]
    tq.inject_tasks tasks
    ae 2, tq.size
    ae 0, tq.at(0)[:id]
    ae 1, tq.at(1)[:id]

    tasks = [{:id=>2, :mapper_class=>true}, {:id=>3}, {:id=>4}]
    tq.inject_tasks tasks
    ae 5, tq.size
    # TaskQueue#inject_tasks
    ae 0, tq.at(0)[:id]
    ae 1, tq.at(1)[:id]
    ae 2, tq.at(2)[:id]
    ae 3, tq.at(3)[:id]
    ae 4, tq.at(4)[:id]

    tasks = (5..6).map {|i|{:id=>i}}
    tq.inject_tasks tasks
    ae 7, tq.size
    tasks = (7..14).map {|i|{:id=>i}}
    tq.inject_tasks tasks
  end
end
