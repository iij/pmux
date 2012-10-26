module Pmux
  class PluginClass
    def initialize
    end

    def load_plugins
      dir = File.join File.dirname(__FILE__), 'plugin'
      load_plugin_dir dir
    end

    def load_plugin_dir dir
      dir = File.expand_path dir
      return unless File.directory? dir
      Dir.entries(dir).sort.each {|fname|
        if fname =~ /\.rb$/
          require File.join(dir, fname)
        end
      }
    end
  end

  Plugin = PluginClass.new
end
