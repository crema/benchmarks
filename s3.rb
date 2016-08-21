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


    @put_count = Concurrent::AtomicFixnum.new
    @put_time = Concurrent::AtomicFixnum.new
    @get_count = Concurrent::AtomicFixnum.new
    @get_time = Concurrent::AtomicFixnum.new
    @head_count = Concurrent::AtomicFixnum.new
    @head_time = Concurrent::AtomicFixnum.new
  end

  def benchmark
    create_tmpfile
    create_bucket
    s3_benchmark
  end

  private

  attr_reader :thread, :count, :read, :size, :tmp, :s3, :bucket,
              :get_count, :get_time, :put_count, :put_time, :head_count, :head_time

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
    result, elapsed = try_realtime do
      s3.head_object(bucket: bucket, key: key)
    end

    head_count.increment
    head_time.increment((elapsed * 1000000).round)
    result
  end

  def get(key)
    result, elapsed =  try_realtime do
      s3.get_object(bucket: bucket, key: key)
    end

    get_count.increment
    get_time.increment((elapsed * 1000000).round)
  end

  def put(key)
    result, elapsed =  try_realtime do
      File.open(File.join(tmp, 's3tmp'), 'rb') do |file|
        s3.put_object(bucket: bucket, key: key, body: file)
      end
    end
    put_count.increment
    put_time.increment((elapsed * 1000000).round)
  end

  def create_bucket
    begin
      resp = s3.list_objects(bucket: bucket)
      resp.contents.each do |content|
        s3.delete_object(bucket:bucket, key: content.key)
      end
      s3.delete_bucket(bucket: bucket)
    ensure
      s3.create_bucket(acl: "private", bucket: bucket,
                       create_bucket_configuration: { location_constraint: "ap-northeast-2"})
    end
  end

  def create_tmpfile
    tmpfile = File.join(tmp, 's3tmp')
    `dd if=/dev/urandom of=#{tmpfile} bs=1024 count=#{size} 2>/dev/null`
    tmpfile
  end

  def s3_benchmark
    Benchmark.bm(40) do |x|
      x.report("read(#{read}) write(1) #{size}K * #{count} thread=#{thread}") do
        keys = (1..count).to_a
        read.times {keys += (1..count).to_a}
        keys.shuffle!

        Parallel.each(keys, in_threads: thread) do |key|
          dir = format('%010d', key).scan(/.{2}/).join('/')
          key = File.join(dir,'tmp')
          if head(key)
            get(key)
          else
            put(key)
          end
        end
      end
    end

    puts("head count: #{head_count.value}, time: #{head_time.value / 1000000.0}, average #{1000000.0 * head_count.value / head_time.value }")
    puts("get count: #{get_count.value}, time: #{get_time.value / 1000000.0}, average #{1000000.0 * get_count.value / get_time.value}")
    puts("put count: #{put_count.value}, time: #{put_time.value / 1000000.0}, average #{1000000.0 * put_count.value / put_time.value}")
  end
end

args = ARGV.map {|arg| arg.split('=')}.to_h

S3Benchmark.new(args).benchmark