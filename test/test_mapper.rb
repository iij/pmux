require File.expand_path(File.dirname(__FILE__) + '/unittest_helper')

require 'fileutils'

class TestMapper < Test::Unit::TestCase
  def setup
    @tmp_dir = $test_dir + '/.t'
    FileUtils.mkdir_p @tmp_dir
  end

  def test_r0_r1
    task = {'task_id'=>1, 'mapper'=>'cat', 'path'=>"#{$test_dir}/txt/0.log"}
    size = File.size task['path']

    mapper = Pmux::StreamingMapper.new task, @tmp_dir
    ifbase = mapper.do_map_task
    ae size, File.size(ifbase+'-0')
    ae 0, mapper.exitstatus

    task['task_id'] = 2
    task['num_r'] = 1
    mapper = Pmux::StreamingMapper.new task, @tmp_dir
    ifbase = mapper.do_map_task
    ae size, File.size(ifbase+'-0')
    ae 0, mapper.exitstatus
  end

  def test_r2
    task = {'task_id'=>1, 'mapper'=>'cat', 'path'=>"#{$test_dir}/txt/0.log"}
    size = File.size task['path']

    task['num_r'] = 2
    mapper = Pmux::StreamingMapper.new task, @tmp_dir
    ifbase = mapper.do_map_task
    ae size, File.size(ifbase+'-0') + File.size(ifbase+'-1')
    ae 0, mapper.exitstatus
  end

  def test_err
    task = {'task_id'=>1, 'mapper'=>'cat', 'path'=>"notexist"}
    mapper = Pmux::StreamingMapper.new task, @tmp_dir
    assert_raise(RuntimeError) {
      ifbase = mapper.do_map_task
    }
    assert(mapper.exitstatus > 0)

    task['mapper'] = 'notexist'
    mapper = Pmux::StreamingMapper.new task, @tmp_dir
    assert_raise(RuntimeError) {
      ifbase = mapper.do_map_task
    }

    task['mapper'] = 'ruby -e "undefinedmethod()"'
    mapper = Pmux::StreamingMapper.new task, @tmp_dir
    assert_raise(RuntimeError) {
      ifbase = mapper.do_map_task
    }
    assert(mapper.exitstatus > 0)
  end
end
