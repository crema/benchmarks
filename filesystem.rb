require 'fileutils'
require 'benchmark'
require 'filesize'
require 'parallel'
require 'concurrent'
require_relative 'lib/native_file'
require_relative 'lib/native_malloc'


class FilesystemBenchmark
  def initialize(args)
    @dest = args['dest'] || './tmp/'

    @file = args['file'].to_i
    @dir = args['dir'].to_i
    @mix = args['mix'].to_i
    @read = args['read'].to_i
    @thread = args['thread'].to_i

    @mix_size = args.fetch('mix_size', 100).to_i

    @thread = 1 if @thread <= 0
  end

  def benchmark
    clear_cache
    file_benchmark if file > 0
    dir_benchmark if dir > 0
    mix_benchmark if mix > 0
  end

  private

  attr_reader :dest, :file, :dir, :mix, :read, :thread, :mix_size

  def with_dest_dir
    FileUtils.rm_rf(dest)
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
    results = Benchmark.bm(40) do |x|
      total = 1024 * file
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
    Benchmark.bm(40) do |x|
      with_dest_dir do
        clear_cache
        x.report("create #{dir} dirs") do
          (1..dir).to_a.shuffle.each do |i|
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


  def head(filename)
    result = false
    elapsed = Benchmark.realtime do
      result = Dir.exist?(File.dirname(filename))
    end
    [result, elapsed]
  end

  def get(filename)
    elapsed = Benchmark.realtime do
      read_file(filename)
    end
    [true, elapsed]
  end

  def put(filename)
    elapsed = Benchmark.realtime do
      dir = File.dirname(filename)
      FileUtils.makedirs(dir) unless Dir.exist?(dir)
      write_file(filename, mix_size)
    end
    [true, elapsed]
  end

  def mix_benchmark
    puts ''
    puts 'mix'
    results = []
    Benchmark.bm(40) do |x|
      with_dest_dir do
        clear_cache       
        x.report("read(#{read}) write(1) 100K * #{mix} thread=#{thread}") do
          dirs = (1..mix).to_a
          read.times {dirs += (1..mix).to_a}
          dirs.shuffle!

          results = Parallel.map(dirs, in_processes: thread) do |dir|
            dir = File.join(dest,'dirs',format('%010d', dir).scan(/.{2}/).join('/'))
            key = File.join(dir,'tmp')

            result = []
            head_result, head_elapsed = head(key)

            result << [:head, head_elapsed]
            if head_result
              get_result, get_elapsed = get(key)
              result << [:get, get_elapsed]
            else
              put_result, put_elapsed = put(key)
              result << [:put, put_elapsed]
            end
            result
          end
        end
      end
    end

    results.flatten!(1)

    head_count = get_count = put_count = 0
    head_time = get_time = put_time = 0

    results.each do |result|
      case result.first
        when :head
          head_count += 1
          head_time += result.last
        when :get
          get_count += 1
          get_time += result.last
        when :put
          put_count += 1
          put_time += result.last
      end
    end

    puts("head count: #{head_count}, time: #{head_time}, average #{head_count / head_time}")
    puts("get count: #{get_count}, time: #{get_time}, average #{get_count / get_time}")
    puts("put count: #{put_count}, time: #{put_time}, average #{put_count / put_time}")
  end
end


args = ARGV.map {|arg| arg.split('=')}.to_h

FilesystemBenchmark.new(args).benchmark