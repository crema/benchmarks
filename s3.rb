require 'fileutils'
require 'benchmark'
require 'filesize'
require 'parallel'
require 'concurrent'
require 'aws-sdk'

class S3Benchmark
  def initialize(args)

    region = args.fetch('region','ap-northeast-2')
    id = args['id']
    secret = args['secret']

    @size = args.fetch('size', 100).to_i
    @thread = args.fetch('thread', 1).to_i
    @count = args.fetch('count', 1000).to_i
    @read = args.fetch('read', 10).to_i
    @bucket = args.fetch('bucket', 'cremas3test')
    @tmp = args.fetch('tmp','/tmp')

    @s3 = Aws::S3::Client.new(region: region, credentials: Aws::Credentials.new(id, secret))
  end

  def benchmark
    create_tmpfile
    s3_benchmark
  end

  private

  attr_reader :thread, :count, :read, :size, :tmp, :s3, :bucket

  def try_realtime
    result = true
    elapsed = Benchmark.realtime do
      begin
        yield if block_given?
      rescue
        result = false
      end
    end
    [result, elapsed]
  end

  def head(key)
    try_realtime do
      s3.head_object(bucket: bucket, key: key)
    end
  end

  def get(key)
    try_realtime do
      s3.get_object(bucket: bucket, key: key)
    end
  end

  def put(key)
    try_realtime do
      File.open(File.join(tmp, 's3tmp'), 'rb') do |file|
        s3.put_object(bucket: bucket, key: key, body: file)
      end
    end
  end

  def with_bucket
    begin
      unless (resp = s3.list_objects(bucket: bucket)).contents.empty?
        resp.contents.each do |content|
          s3.delete_object(bucket:bucket, key: content.key)
        end
      end
    rescue Aws::S3::Errors::NoSuchBucket => e
    end

    yield if block_given?

    begin
      unless (resp = s3.list_objects(bucket: bucket)).contents.empty?
        resp.contents.each do |content|
          s3.delete_object(bucket:bucket, key: content.key)
        end
      end
    rescue Aws::S3::Errors::NoSuchBucket => e
    end
  end

  def create_tmpfile
    tmpfile = File.join(tmp, 's3tmp')
    `dd if=/dev/urandom of=#{tmpfile} bs=1024 count=#{size} 2>/dev/null`
    tmpfile
  end

  def s3_benchmark
    results = []
    with_bucket do

      Benchmark.bm(40) do |x|
        x.report("read(#{read}) write(1) #{size}K * #{count} thread=#{thread}") do
          keys = (1..count).to_a
          read.times {keys += (1..count).to_a}
          keys.shuffle!

          results = Parallel.map(keys, in_processes: thread) do |key|
            dir = format('%010d', key).scan(/.{2}/).join('/')
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
end

args = ARGV.map {|arg| arg.split('=')}.to_h

S3Benchmark.new(args).benchmark