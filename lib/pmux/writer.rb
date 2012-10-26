module Pmux
  class Writer
    CHUNK_SIZE = 8192

    def write; end
    def finish; end
  end

  class STDOUTWriter < Writer
    def write path
      open(path) {|f|
        until f.eof?
          data = f.read(CHUNK_SIZE)
          STDOUT.write data
        end
      }
    end
  end
end
