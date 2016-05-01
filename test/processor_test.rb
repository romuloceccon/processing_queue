require 'test/unit'
require 'mocha/test_unit'
require 'json'
require 'processor'

class ProcessorTest < Test::Unit::TestCase
  # ----- test framework -----

  class Error < StandardError
  end

  class Wrapper
    def initialize(obj)
      @obj = obj
      @before_hooks = {}
      @after_hooks = {}
    end

    def method_missing(symbol, *args, &block)
      call_hook(@before_hooks, symbol, *args)
      result = @obj.send(symbol, *args, &block)
      call_hook(@after_hooks, symbol, *args)
      result
    end

    def before(symbol, &block)
      @before_hooks[symbol] = block
    end

    def after(symbol, &block)
      @after_hooks[symbol] = block
    end

    def call_hook(hook_list, symbol, *args)
      hook_list[symbol].call(*args) if hook_list.key?(symbol)
    end
  end

  setup do
    @redis = Redis.new(db: 15)
    @processor = Processor.new(@redis)
  end

  teardown do
    @redis.flushdb
  end

  # ----- test framework tests -----

  test "should cleanup database" do
    assert_equal(false, @redis.exists("test:dirty"))
    @redis.set("test:dirty", "1")
  end

  test "should wrap redis" do
    processor = Processor.new(Wrapper.new(@redis))

    @redis.lpush("events:queue", { 'id' => 1 }.to_json)
    processor.dispatcher.dispatch_all { [1, 1] }

    assert_equal(["1"], @redis.smembers("operators:known"))
  end

  test "should hook redis methods" do
    wrapper = Wrapper.new(@redis)
    dispatcher = Processor.new(wrapper).dispatcher

    before, after = nil, nil

    wrapper.before(:rename) { |*args| before = @redis.exists("events:dispatching") }
    wrapper.after(:rename) { |*args| after = @redis.exists("events:dispatching") }

    @redis.lpush("events:queue", { 'id' => 1 }.to_json)

    dispatcher.dispatch_all { [1, 1] }

    assert_equal(false, before)
    assert_equal(true, after)
  end

  # ----- dispatcher unit tests -----

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

  test "should dispatch single event" do
    dispatcher = @processor.dispatcher

    @redis.lpush("events:queue", { 'id' => 1 }.to_json)

    dispatcher.dispatch_all do |event|
      assert_equal({ 'id' => 1 }, event)
      [10, 500]
    end

    assert_equal(["10"], @redis.smembers("operators:known"))

    assert_equal(1, @redis.llen("operators:queue"))
    assert_equal("10", @redis.rpop("operators:queue"))

    assert_equal(1, @redis.llen("operators:10:events"))
    assert_equal({ 'installation_id' => 500, 'data' => { 'id' => 1 } },
      JSON.parse(@redis.rpop("operators:10:events")))
  end

  test "should dispatch two events from different operators" do
    dispatcher = @processor.dispatcher

    @redis.lpush("events:queue", { 'id' => 2 }.to_json)
    @redis.lpush("events:queue", { 'id' => 3 }.to_json)

    dispatcher.dispatch_all do |event|
      [event['id'] * 10, event['id'] * 100]
    end

    assert_equal(["20", "30"], @redis.smembers("operators:known"))

    assert_equal(2, @redis.llen("operators:queue"))
    assert_equal("20", @redis.rpop("operators:queue"))
    assert_equal("30", @redis.rpop("operators:queue"))

    (2..3).each do |i|
      k = "operators:#{i * 10}:events"
      assert_equal(1, @redis.llen(k))
      assert_equal({ 'installation_id' => i * 100, 'data' => { 'id' => i } },
        JSON.parse(@redis.rpop(k)))
    end
  end

  test "should dispatch two events from same operator" do
    dispatcher = @processor.dispatcher

    @redis.lpush("events:queue", { 'id' => 2 }.to_json)
    @redis.lpush("events:queue", { 'id' => 3 }.to_json)

    dispatcher.dispatch_all do |event|
      [10, event['id'] * 100]
    end

    assert_equal(["10"], @redis.smembers("operators:known"))

    assert_equal(1, @redis.llen("operators:queue"))
    assert_equal("10", @redis.rpop("operators:queue"))

    k = "operators:10:events"
    assert_equal(2, @redis.llen(k))
    (2..3).each do |i|
      assert_equal({ 'installation_id' => i * 100, 'data' => { 'id' => i } },
        JSON.parse(@redis.rpop(k)))
    end
  end

  test "should clear event queue on dispatch" do
    dispatcher = @processor.dispatcher

    (2..4).each { |i| @redis.lpush("events:queue", { 'id' => i }.to_json) }

    dispatcher.dispatch_all do |event|
      [event['id'] * 10, event['id'] * 100]
    end

    assert_equal(0, @redis.llen("events:queue"))
    assert_equal(0, @redis.llen("events:dispatching"))
  end

  test "should dispatch events in fifo order" do
    dispatcher = @processor.dispatcher

    @redis.lpush("events:queue", { 'id' => 2 }.to_json)
    @redis.lpush("events:queue", { 'id' => 3 }.to_json)

    @dispatched = []

    dispatcher.dispatch_all do |event|
      @dispatched << event['id']
      [event['id'] * 10, event['id'] * 100]
    end

    assert_equal([2, 3], @dispatched)
  end

  test "should not dispatch any event if an error occurs" do
    dispatcher = @processor.dispatcher

    (2..4).each { |i| @redis.lpush("events:queue", { 'id' => i }.to_json) }

    assert_raises(Error) do
      dispatcher.dispatch_all do |event|
        if event['id'] == 2 then
          [20, 200]
        else
          raise Error, 'abort'
        end
      end
    end

    assert_equal(false, @redis.exists("operators:known"))
    assert_equal(0, @redis.llen("events:queue"))
    assert_equal(3, @redis.llen("events:dispatching"))
  end

  test "should dispatch events atomically" do
    wrapper = Wrapper.new(@redis)
    dispatcher = Processor.new(wrapper).dispatcher

    @redis.lpush("events:queue", { 'id' => 1 }.to_json)

    wrapper.before(:del) do |*args|
      raise Error, 'abort' if args.first == 'events:dispatching'
    end

    assert_raises(Error) { dispatcher.dispatch_all { [1, 1] } }

    assert_equal(0, @redis.llen("operators:queue"))
    assert_equal(false, @redis.exists("operators:known"))
  end

  test "should reenqueue known processing operators at end of queue" do
    dispatcher = @processor.dispatcher

    @redis.sadd("operators:known", "20")
    @redis.lpush("operators:processing", "20")

    @redis.lpush("events:queue", { "id" => 1 }.to_json)
    dispatcher.dispatch_all { [10, 100] }

    assert_equal(["10", "20"], @redis.smembers("operators:known"))

    assert_equal(["20", "10"], @redis.lrange("operators:queue", 0, -1))
    assert_equal([], @redis.lrange("operators:processing", 0, -1))
  end

  test "should not reenqueue already enqueued processing operator" do
    dispatcher = @processor.dispatcher

    @redis.sadd("operators:known", "20")
    @redis.lpush("operators:processing", "20")

    @redis.lpush("events:queue", { "id" => 2 }.to_json)
    dispatcher.dispatch_all { [20, 200] }

    assert_equal(["20"], @redis.smembers("operators:known"))

    assert_equal(["20"], @redis.lrange("operators:queue", 0, -1))
    assert_equal([], @redis.lrange("operators:processing", 0, -1))
  end

  test "should reenqueue known processing operators even without events" do
    dispatcher = @processor.dispatcher

    @redis.sadd("operators:known", "20")
    @redis.lpush("operators:processing", "20")

    dispatcher.dispatch_all { [0, 0] }

    assert_equal(1, @redis.llen("operators:queue"))
    assert_equal("20", @redis.rpop("operators:queue"))

    assert_equal(0, @redis.llen("operators:processing"))
  end

  test "should not reenqueue unknown processing operators" do
    dispatcher = @processor.dispatcher

    @redis.lpush("operators:processing", "20")

    @redis.lpush("events:queue", { "id" => 1 }.to_json)
    dispatcher.dispatch_all { [10, 100] }

    assert_equal(1, @redis.llen("operators:queue"))
    assert_equal("10", @redis.rpop("operators:queue"))

    assert_equal(0, @redis.llen("operators:processing"))
  end

  test "should not delete processing operator added during event dispatching" do
    wrapper = Wrapper.new(@redis)
    dispatcher = Processor.new(wrapper).dispatcher

    @redis.sadd("operators:known", "10")
    @redis.sadd("operators:known", "20")

    redis_aux = Redis.new(db: 15)
    wrapper.before(:sadd) do |*args|
      redis_aux.lpush("operators:processing", "20")
    end

    @redis.lpush("operators:processing", "10")
    @redis.lpush("events:queue", { "id" => 3 }.to_json)
    dispatcher.dispatch_all { [30, 300] }

    assert_equal(1, @redis.llen("operators:processing"))
    assert_equal("20", @redis.rpop("operators:processing"))
  end

  test "should ignore locked processing operator" do
    dispatcher = @processor.dispatcher

    @redis.sadd("operators:known", "20")
    @redis.lpush("operators:processing", "20")
    @redis.set("operators:20:lock", "abc")

    dispatcher.dispatch_all { [0, 0] }

    assert_equal([], @redis.lrange("operators:queue", 0, -1))
    assert_equal(["20"], @redis.lrange("operators:processing", 0, -1))
  end

  test "should reenqueue processing operators while ignoring locked ones" do
    dispatcher = @processor.dispatcher

    (1..4).each do |i|
      op = (i * 10).to_s
      @redis.sadd("operators:known", op)
      @redis.lpush("operators:processing", op)
    end
    @redis.set("operators:20:lock", "abc")
    @redis.set("operators:40:lock", "def")

    dispatcher.dispatch_all { [0, 0] }
    assert_equal(["30", "10"], @redis.lrange("operators:queue", 0, -1))
    assert_equal(["40", "20"], @redis.lrange("operators:processing", 0, -1))
  end

  # ----- worker unit tests -----

  test "should get operator from queue" do
    worker = @processor.worker

    @redis.lpush("operators:queue", "1")
    assert_equal("1", worker.wait_for_operator)

    assert_equal([], @redis.lrange("operators:queue", 0, -1))
    assert_equal(["1"], @redis.lrange("operators:processing", 0, -1))
  end

  test "should process multiple events" do
    worker = @processor.worker

    @redis.sadd("operators:known", "1")
    @redis.lpush("operators:1:events", { 'val' => 1 }.to_json)
    @redis.lpush("operators:1:events", { 'val' => 2 }.to_json)

    cnt = 0
    worker.process("1") do |event|
      cnt += 1
      assert_equal({ 'val' => cnt }, event)
    end

    assert_equal(2, cnt)
    assert_equal([], @redis.smembers("operators:known"))
    assert_equal(0, @redis.llen("operators:1:events"))
    assert_equal(false, @redis.exists("operators:1:lock"))
  end

  test "should lock operator before processing" do
    worker = @processor.worker

    @redis.sadd("operators:known", "1")
    @redis.lpush("operators:1:events", { 'val' => 1 }.to_json)
    assert_raises(Error) { worker.process("1") { raise Error, "abort" } }

    assert_equal(1, @redis.llen("operators:1:events"))
    assert_match(/^\w{8}-\w{4}-\w{4}-\w{4}-\w{12}$/,
      @redis.get("operators:1:lock"))
  end

  test "should lock operator with timeout" do
    worker = @processor.worker

    @redis.lpush("operators:1:events", { 'val' => 1 }.to_json)
    assert_raises(Error) { worker.process("1") { raise Error, "abort" } }

    assert_in_delta(300_000, @redis.pttl("operators:1:lock"), 100)
  end

  test "should not process events if operator is locked" do
    worker = @processor.worker

    @redis.lpush("operators:1:events", { 'val' => 1 }.to_json)
    @redis.set("operators:1:lock", "1234")

    cnt = 0
    worker.process("1") { cnt += 1 }

    assert_equal(0, cnt)
    assert_equal(1, @redis.llen("operators:1:events"))
  end

  test "should not unlock lock from another worker" do
    wrapper = Wrapper.new(@redis)
    worker = Processor.new(wrapper).worker

    @redis.sadd("operators:known", "1")
    @redis.lpush("operators:1:events", { 'val' => 1 }.to_json)

    wrapper.before(:rpop) { @redis.set("operators:1:lock", "1234") }
    worker.process("1") {}

    assert_equal("1234", @redis.get("operators:1:lock"))
  end

  test "should not remove operator from known set if queue is not empty" do
    wrapper = Wrapper.new(@redis)
    worker = Processor.new(wrapper).worker

    @redis.sadd("operators:known", "1")
    @redis.lpush("operators:1:events", { 'val' => 1 }.to_json)

    wrapper.after(:lindex) do
      if @redis.llen("operators:1:events") == 0
        @redis.lpush("operators:1:events", { 'val' => 2 }.to_json)
      end
    end
    worker.process("1") {}

    assert_equal(["1"], @redis.smembers("operators:known"))
    assert_equal(1, @redis.llen("operators:1:events"))
    assert_equal(false, @redis.exists("operators:1:lock"))
  end
end
