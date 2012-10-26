# ダミーのXAttr
class XAttr
  # pathに応じて拡張属性を偽装して返す
  def self.get path, name
    dist = 'DISTRIBUTE:dist-test-dht'
    x = path.hash % 4
    path = path.gsub!(%r{^/}, '')
    res = "(<%s> <POSIX:host%d.example.com:/prefix%s>)" % [dist, x, path]
  end
end
