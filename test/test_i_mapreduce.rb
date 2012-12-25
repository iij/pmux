require File.expand_path(File.dirname(__FILE__) + '/unittest_helper')

require 'stringio'

require 'mock_world'
require 'mock_mros'
require 'mock_pipeio'

Log.open('-') if __FILE__ == $0 and $show_log

class MockWorker < Pmux::Worker
  def run
    while true
      future = @client.call_async :get_task
      future.attach_callback {|f|
        task = f.get
        result = exec_task task
        res = @client.call :put_result, result
      }
      Fiber.yield
    end
  end
end

class MockHandler < Pmux::Handler
  def fork_worker num_cpu, options
    world = @server.loop.world
    stransport = @server.listeners.detect {|c| c.kind_of? MR::MockTransport}
    ctransport = world.new_connection MR::MockTransport
    ctransport.mock_receiver = stransport
    stransport.mock_receiver = ctransport
    client = MR::Client.new ctransport, 'dummy', @server.loop
    worker = MockWorker.new options, client
    @fiber = Fiber.new {
      worker.run
    }
    @fiber.resume
  end
end

class MockWriter < Pmux::Writer
  attr_reader :lines

  def initialize
    @lines = []
  end

  def write path
    @lines += File.readlines path
  end
end

class Pmux::Cleaner
  def run lim_time=nil
    lim_time = Time.now - 3600
    @glob_pats.each {|glob_pat| clean glob_pat, lim_time}
  end
end

class TestIMapreduce < Test::Unit::TestCase
  # gflocatorを生成し、そのサーバへのconnection(socket)を返す
  def setup_gflocator world, host, port
    #handler = GFL::Handler.new
    #server = MRIO::MockServer.start world, host, port, handler
    #c = MockTCPSocket.new host, port
  end

  def setup_world root_dir
    MockWorld.cleanup root_dir
    world = MockWorld.new
    Coolio::Loop.default.world = world
    Net::SSH::MockChannel.handler_class = MockHandler
    world
  end

  def setup_task_dispatcher addrs, options, files, writer=nil
    setup_world options[:root_dir]
    FileUtils.mkdir_p options[:tmp_dir]
    sn = options[:storage_name] || 'local'
    adapter = Pmux::StorageAdapter.create sn, addrs
    msession = Pmux::MRSession.new addrs, options
    msession.connect
    gatherer = Pmux::Gatherer.new(writer || MockWriter.new)
    dispatcher = Pmux::TaskDispatcher.new options, adapter, msession, gatherer
  end

  def test_show_status
  end

  # mr test; num_r=0, 4 files
  def test_run_mr0
    root_dir = "#{$test_dir}/.t"

    # 2 nodes, 4 files
    addrs = ['192.168.0.1', '192.168.0.2']
    options = {:root_dir=>root_dir, :tmp_dir=>(root_dir + '/localhost'),
      :mapper=>'cat'}
    files = Dir.glob($test_dir + '/txt/[0-3].log')
    writer = MockWriter.new
    dispatcher = setup_task_dispatcher addrs, options, files, writer
    job = Pmux::Job.new options, files
    dispatcher.run job
    ae 420, writer.lines.size
  end

  # mr test; num_r=1, 4 files
  def test_run_mr1
    root_dir = "#{$test_dir}/.t"
    world = setup_world root_dir

    # 2 nodes, 4 files, 2 reducers
    addrs = ['192.168.0.1', '192.168.0.2']
    options = {:root_dir=>root_dir, :tmp_dir=>(root_dir + '/localhost'),
      :mapper=>'cat', :num_r=>1}
    files = Dir.glob($test_dir + '/txt/[0-3].log')
    writer = MockWriter.new
    dispatcher = setup_task_dispatcher addrs, options, files, writer
    job = Pmux::Job.new options, files
    job.mk_reducer_addrs addrs
    dispatcher.run job
    ae 420, writer.lines.size
  end

  # mr test; num_r=2, 4 files
  def test_run_mr2
    root_dir = "#{$test_dir}/.t"
    world = setup_world root_dir

    # 2 nodes, 4 files, 2 reducers
    addrs = ['192.168.0.1', '192.168.0.2']
    options = {:root_dir=>root_dir, :tmp_dir=>(root_dir + '/localhost'),
      :mapper=>'cat', :num_r=>2, :separator=>' '}
    files = Dir.glob($test_dir + '/txt/[0-3].log')
    writer = MockWriter.new
    dispatcher = setup_task_dispatcher addrs, options, files, writer
    job = Pmux::Job.new options, files
    job.mk_reducer_addrs addrs
    dispatcher.run job
    ae 420, writer.lines.size

    # error check
    files.push '/notexist'
    writer = MockWriter.new
    dispatcher = setup_task_dispatcher addrs, options, files, writer
    er = nil
    dispatcher.on_error {|r| er = r}
    job = Pmux::Job.new options, files
    job.mk_reducer_addrs addrs
    dispatcher.run job
    assert_match %r{/notexist: No such file or directory}, er['error_message']
    ae 420, writer.lines.size
  end

  # mr test; num_r=0, 4 files, ff=2
  def test_run_mr_f
    root_dir = "#{$test_dir}/.t"

    # 2 nodes, 9 files, ff=2
    addrs = ['192.168.0.1', '192.168.0.2']
    options = {:root_dir=>root_dir, :tmp_dir=>(root_dir + '/localhost'),
      :mapper=>'cat', :ff=>2}
    files = Dir.glob($test_dir + '/txt/*.log')
    writer = MockWriter.new
    dispatcher = setup_task_dispatcher addrs, options, files, writer
    job = Pmux::Job.new options, files
    dispatcher.run job
    ae 945, writer.lines.size
  end
end
