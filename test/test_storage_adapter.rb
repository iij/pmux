require File.expand_path(File.dirname(__FILE__) + '/unittest_helper')

class TestStorageAdapter < Test::Unit::TestCase
  def setup
  end

  def test_create
    addrs = ['192.168.0.1', '192.168.0.2']

    adapter = Pmux::StorageAdapter.create 'local', addrs
    assert_kind_of Pmux::LocalAdapter, adapter
    ae addrs, adapter.addrs
    assert_kind_of Hash, adapter['192.168.0.1']
    assert adapter['192.168.0.1'].empty?

    adapter = Pmux::StorageAdapter.create 'glusterfs', []
    assert_kind_of Pmux::GlusterFSAdapter, adapter
  end

  def test_local
    addrs = ['192.168.0.1', '192.168.0.2']
    adapter = Pmux::StorageAdapter.create 'local', addrs

    args = ['/tmp/0.log', '/tmp/1.log', '/tmp/2.log', '/tmp/3.log']
    files = adapter.get_files args
    ae args.size, files.size

    args = ["#{$test_dir}/txt/*.log"]
    files = adapter.get_files args
    ae args.size, files.size

    files = ["#{$test_dir}/txt/*.log"]
    files = adapter.get_files args, true
    ae 9, files.size
  end

  def _test_pmuxfs
    addrs = ['192.168.0.1', '192.168.0.2']
    adapter = Pmux::StorageAdapter.create 'pmuxfs', addrs

    # pmuxfs find test
    adapter.scan_unit = 5
    adapter.set_fs_dir "#{$test_dir}/txt"
    res = adapter.find
    ae 5, res.size
    until (res = adapter.find).empty?; end
  end

  def _test_pmux_node_table
    addrs = ['192.168.0.1', '192.168.0.2', '192.168.0.3']#, '192.168.0.4']
    nt = Pmux::PmuxNodeTable.new
    addrs.each {|addr| nt.push_node addr}
    nt.reposition

    counts = Hash.new 0
    for i in 1..1000
      s = i.to_s
      h = Digest::SHA1.digest(s)
      node_addr, = nt.lookup h
      counts[node_addr] += 1
    end
  end
end
