module Pmux
  class Gatherer
    attr_accessor :writer
    attr_reader :mf

    def initialize writer=nil
      @writer = writer
      @mf = MR::MultiFuture.new
      @mf.on_success {|f| writer.write f.get}
    end

    def gather msession, node_addr, remote, local, options={}
      future = msession.scp_download node_addr, remote, local,
        :set_result=>local
      @mf.add future
    end

    def join_all
      @mf.join_all
      @writer.finish
    end
  end
end
