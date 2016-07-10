require 'fileutils'
require 'logger'
require 'benchmark'
require 'filesize'

class FilesystemBenchmark
  def initialize(dest)
    @dest = dest
  end

  def benchmark
    clear_cache
    file_benchmark
    dir_benchmark
    mix_benchmark
  end

  private

  attr_reader :dest

  def with_dest_dir
    FileUtils.makedirs(dest)
    yield
    FileUtils.rm_rf(File.join(dest,'*'))
  end

  def write_file(path, size)
    `dd if=/dev/zero of=#{path} bs=#{1024} count=#{size} 2>/dev/null`
  end

  def read_file(path)
    `dd if=#{path} of=/dev/null 2>/dev/null`
  end

  def clear_cache
    `sudo bash -c "sync; echo 3 > /proc/sys/vm/drop_caches"`
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
    with_dest_dir do
      puts ''
      puts 'file rw'
      results = Benchmark.bm(32) do |x|
        clear_cache
        x.report('w 1g') do
          write_file(File.join(dest,'tmp.1G'), 1024 * 1024)
        end
        clear_cache
        x.report('w 1M * 1024') do
          for i in (0..1024) do
            write_file(File.join(dest,"tmp.1M.#{i}"), 1024)
          end
        end
        clear_cache
        x.report('w 100K * 10240') do
          for i in (0..10240) do
            write_file(File.join(dest,"tmp.100K.#{i}"), 100)
          end
        end
        clear_cache
        x.report('r 1g') do
          read_file(File.join(dest,'tmp.1G'))
        end
        clear_cache
        x.report('r 1M * 1024') do
          for i in (0..1024) do
            read_file(File.join(dest,"tmp.1M.#{i}"))
          end
        end
        x.report('r 100k * 10240') do
          for i in (0..10240) do
            read_file(File.join(dest,"tmp.100k.#{i}"))
          end
        end
      end
    end
  end

  def dir_benchmark
    with_dest_dir do
      puts ''
      puts 'dir'
      n = 1024 * 100
      Benchmark.bm(32) do |x|
        clear_cache
        x.report("create #{n} dirs") do
          (1..n).to_a.shuffle.each do |i|
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
    with_dest_dir do
      puts ''
      puts 'mix'
      file_count = 1024 * 10
      read_count = 5
      Benchmark.bm(32) do |x|
        dirs = (1..file_count).to_a
        read_count.times {dirs += (1..file_count).to_a}
        clear_cache
        x.report("read(#{read_count}) write(1) 100K * #{file_count}") do
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

FilesystemBenchmark.new(dest).benchmark