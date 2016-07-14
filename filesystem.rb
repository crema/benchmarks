require 'fileutils'
require 'benchmark'
require 'filesize'
require_relative 'lib/native_file'
require_relative 'lib/native_malloc'


class FilesystemBenchmark
  def initialize(dest, count, read)
    @dest = dest
    @count = count
    @read = read
  end

  def benchmark
    clear_cache
    file_benchmark
    dir_benchmark
    mix_benchmark
  end

  private

  attr_reader :dest, :count, :read

  def with_dest_dir
    FileUtils.makedirs(dest)
    yield
    FileUtils.rm_rf(File.join(dest,'*'))
  end

  def buffer
    @buffer ||= NativeMalloc.malloc(1024)
  end

  def write_file(path, size)
    file = NativeFile.fopen(path,'wb')
    size.times do
      NativeFile.fwrite(buffer,1,1024,file)
    end
    NativeFile.fclose(file)
  end

  def read_file(path)
    file = NativeFile.fopen(path,'rb')
    loop do
      read_size = NativeFile.fread(buffer,1,1024,file)
      break if read_size < 1024
    end
    NativeFile.fclose(file)
  end

  def clear_cache
    `sudo 2>/dev/null  bash -c "sync; echo 3 > /proc/sys/vm/drop_caches"`
  end

  def traversal(path)
    queue = Queue.new
    queue << path
    while !queue.empty?
      dir = queue.pop
      Dir.entries(dir).each do |entry|
        next if ['.','..'].include?(entry)
        entry = File.join(dir, entry)
        if File.directory?(entry)
          queue << entry
        else
          yield entry if block_given?
        end
      end
    end
  end

  def file_benchmark
    puts ''
    puts 'file rw'
    results = Benchmark.bm(32) do |x|
      total = 1024 * count
      size = total
      count = total / size

      while size >= 10
        clear_cache
        with_dest_dir do
          x.report("w #{Filesize.new(size * 1024).pretty} * #{count}") do
            for i in (0..count) do
              write_file(File.join(dest,"tmp.#{size * 1024}.#{i}"), size)
            end
          end
          clear_cache
          x.report("r #{Filesize.new(size * 1024).pretty} * #{count}") do
            for i in (0..count) do
              read_file(File.join(dest,"tmp.#{size * 1024}.#{i}"))
            end
          end
        end
        size = size / 10
        count = total / size
      end
    end
  end

  def dir_benchmark
    puts ''
    puts 'dir'
    Benchmark.bm(32) do |x|
      with_dest_dir do
        clear_cache
        x.report("create #{count} dirs") do
          (1..count).to_a.shuffle.each do |i|
            FileUtils.makedirs(File.join(dest,'dirs',format('%010d', i).scan(/.{2}/).join('/')))
          end
        end
        clear_cache
        x.report('traversal dirs') do
          traversal(File.join(dest,'dirs'))
        end
      end
    end
  end

  def mix_benchmark
    puts ''
    puts 'mix'
    Benchmark.bm(32) do |x|
      dirs = (1..count).to_a
      read.times {dirs += (1..count).to_a}
      with_dest_dir do
        clear_cache
        x.report("read(#{read}) write(1) 100K * #{count}") do
          dirs.each do |i|
            dir = File.join(dest,'dirs',format('%010d', i).scan(/.{2}/).join('/'))
            if Dir.exist?(dir)
              FileUtils.makedirs(dir)
              write_file(File.join(dir,'tmp'), 100)
            else
              read_file(File.join(dir,'tmp'))
            end
          end
        end
      end
    end
  end
end


args = ARGV.map {|arg| arg.split('=')}.to_h
dest = args['dest'] || './tmp/'
count = args['count'].to_i
read = args['read'].to_i
count = 1024 if count <= 0
read = 10 if read <= 0

FilesystemBenchmark.new(dest, count, read).benchmark