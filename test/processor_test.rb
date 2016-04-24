require 'test/unit'
require 'mocha/test_unit'
require 'processor'

class ProcessorTest < Test::Unit::TestCase

  setup do
    @redis = Redis.new(db: 15)
    @processor = Processor.new(@redis)
  end

  teardown do
    @redis.flushdb
  end

  test "should cleanup database" do
    assert_equal(false, @redis.exists("test:dirty"))
    @redis.set("test:dirty", "1")
  end

  test "should get dispatcher" do
    assert_not_nil(@processor.dispatcher)
  end

  test "should append dispatching events to queue on dispatcher creation" do
    @redis.lpush("events:dispatching", "1")

    @processor.dispatcher
    assert_equal(false, @redis.exists("events:dispatching"))
    assert_equal("1", @redis.rpop("events:queue"))
  end

  test "should not overwrite events in queue on dispatcher creation" do
    @redis.lpush("events:dispatching", "1")
    @redis.lpush("events:dispatching", "2")
    @redis.lpush("events:queue", "3")

    @processor.dispatcher
    assert_equal(3, @redis.llen("events:queue"))
  end

  test "should push dispatching events before queue on dispatcher creation" do
    @redis.lpush("events:dispatching", "1")
    @redis.lpush("events:dispatching", "2")
    @redis.lpush("events:queue", "3")

    @processor.dispatcher
    (1..3).each { |i| assert_equal(i.to_s, @redis.rpop("events:queue")) }
  end

  test "should push dispatching events atomically on dispatcher creation" do
    redis = mock
    redis.expects(:eval)

    p = Processor.new(redis)
    p.dispatcher
  end

  test "should not create dispatcher twice" do
    redis = mock
    redis.expects(:eval).once

    p = Processor.new(redis)
    assert_equal(p.dispatcher, p.dispatcher)
  end

end
