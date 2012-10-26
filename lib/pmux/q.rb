def q(*args)
  STDERR.puts args.map {|item| item.inspect}.join(', ')
end
