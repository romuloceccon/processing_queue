require 'test/unit'
require 'mocha/test_unit'
require 'json'
require 'processing_queue'

class ProcessingQueueTest < Test::Unit::TestCase
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

    def clear
      @before_hooks.clear
      @after_hooks.clear
    end

    def call_hook(hook_list, symbol, *args)
      hook_list[symbol].call(*args) if hook_list.key?(symbol)
    end
  end

  setup do
    @redis = Redis.new(db: 15)
    @processor = ProcessingQueue.new(@redis)
  end

  teardown do
    @redis.flushdb
  end

  SLOTS_PER_MIN = 60 / ProcessingQueue::PERF_COUNTER_RESOLUTION

  def slot_number(t)
    (t / ProcessingQueue::PERF_COUNTER_RESOLUTION).to_i
  end

  def epoch_to_redis_time(t)
    (t * 1e6).divmod(1e6).map { |x| x.to_i.to_s }
  end

  # ----- test framework tests -----

  test "should cleanup database" do
    assert_equal(false, @redis.exists("test:dirty"))
    @redis.set("test:dirty", "1")
  end

  test "should wrap redis" do
    processor = ProcessingQueue.new(Wrapper.new(@redis))

    @redis.lpush("events:queue", { 'id' => 1 }.to_json)
    processor.dispatcher.dispatch_all { [1, 1] }

    assert_equal(["1"], @redis.smembers("queues:known"))
  end

  test "should hook redis methods" do
    wrapper = Wrapper.new(@redis)
    dispatcher = ProcessingQueue.new(wrapper).dispatcher

    before, after = [], []

    wrapper.before(:set) { |*args| before << @redis.exists("events:lock") }
    wrapper.after(:set) { |*args| after << @redis.exists("events:lock") }

    @redis.lpush("events:queue", { 'id' => 1 }.to_json)

    dispatcher.dispatch_all { [1, 1] }

    assert_equal([false, false], before)
    assert_equal([true, true], after)
  end

  # ----- dispatcher unit tests -----

  test "should get dispatcher" do
    assert_not_nil(@processor.dispatcher)
  end

  test "should not reset event counters" do
    @redis.set("events:counters:dispatched", "123")
    @redis.set("events:counters:processed", "456")

    @processor.dispatcher

    assert_equal("123", @redis.get("events:counters:dispatched"))
    assert_equal("456", @redis.get("events:counters:processed"))
  end

  test "should not create dispatcher twice" do
    redis = mock
    redis.expects(:script).once.returns("678")

    p = ProcessingQueue.new(redis)
    assert_same(p.dispatcher, p.dispatcher)
  end

  test "should dispatch single event" do
    dispatcher = @processor.dispatcher

    @redis.lpush("events:queue", { 'id' => 1 }.to_json)

    result = dispatcher.dispatch_all do |event|
      assert_equal({ 'id' => 1 }, event)
      [10, 500]
    end
    assert_equal(true, result)
    assert_nil(@redis.get("events:lock"))

    assert_equal(["10"], @redis.smembers("queues:known"))

    assert_equal(1, @redis.llen("queues:waiting"))
    assert_equal("10", @redis.rpop("queues:waiting"))

    assert_equal(1, @redis.llen("queues:10:events"))
    assert_equal({ 'object' => 500, 'data' => { 'id' => 1 } },
      JSON.parse(@redis.rpop("queues:10:events")))
  end

  test "should increment dispatched event counter" do
    dispatcher = @processor.dispatcher

    @redis.lpush("events:queue", { 'id' => 1 }.to_json)
    dispatcher.dispatch_all { |event| [10, 500] }

    assert_equal(1, @redis.get("events:counters:dispatched").to_i)
  end

  test "should dispatch event without object" do
    dispatcher = @processor.dispatcher

    @redis.lpush("events:queue", { 'id' => 1 }.to_json)

    dispatcher.dispatch_all do |event|
      assert_equal({ 'id' => 1 }, event)
      ['DISCARD', nil]
    end

    assert_equal("DISCARD", @redis.rpop("queues:waiting"))
    assert_equal({ 'data' => { 'id' => 1 } },
      JSON.parse(@redis.rpop("queues:DISCARD:events")))
  end

  test "should dispatch two events from different operators" do
    dispatcher = @processor.dispatcher

    @redis.lpush("events:queue", { 'id' => 2 }.to_json)
    @redis.lpush("events:queue", { 'id' => 3 }.to_json)

    dispatcher.dispatch_all do |event|
      [event['id'] * 10, event['id'] * 100]
    end

    assert_equal(["20", "30"], @redis.smembers("queues:known"))

    assert_equal(2, @redis.llen("queues:waiting"))
    assert_equal("20", @redis.rpop("queues:waiting"))
    assert_equal("30", @redis.rpop("queues:waiting"))

    (2..3).each do |i|
      k = "queues:#{i * 10}:events"
      assert_equal(1, @redis.llen(k))
      assert_equal({ 'object' => i * 100, 'data' => { 'id' => i } },
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

    assert_equal(["10"], @redis.smembers("queues:known"))

    assert_equal(1, @redis.llen("queues:waiting"))
    assert_equal("10", @redis.rpop("queues:waiting"))

    k = "queues:10:events"
    assert_equal(2, @redis.llen(k))
    (2..3).each do |i|
      assert_equal({ 'object' => i * 100, 'data' => { 'id' => i } },
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

    assert_equal(3, @redis.llen("events:queue"))
    assert_equal(false, @redis.exists("queues:known"))
    assert_equal(false, @redis.exists("queues:20:events"))
  end

  test "should dispatch events atomically" do
    wrapper = Wrapper.new(@redis)
    dispatcher = ProcessingQueue.new(wrapper).dispatcher

    @redis.lpush("events:queue", { 'id' => 1 }.to_json)

    wrapper.after(:incr) do |*args|
      raise Error, 'abort'
    end

    assert_raises(Error) { dispatcher.dispatch_all { [1, 1] } }

    assert_equal(1, @redis.llen("events:queue"))
    assert_equal(0, @redis.llen("queues:waiting"))
    assert_equal(false, @redis.exists("queues:known"))
  end

  test "should preserve events added during dispatch" do
    wrapper = Wrapper.new(@redis)
    dispatcher = ProcessingQueue.new(wrapper).dispatcher

    @redis.lpush("events:queue", { 'id' => 1 }.to_json)

    redis_aux = Redis.new(db: 15)
    wrapper.after(:incr) do |*args|
      redis_aux.lpush("events:queue", { 'id' => 2 }.to_json)
      # avoid start of second iteration
      wrapper.before(:set) { throw(:abort) }
    end

    catch(:abort) { dispatcher.dispatch_all { [1, 1] } }

    assert_equal(1, @redis.llen("events:queue"))
    assert_equal({ 'id' => 2 }, JSON.parse(@redis.lindex("events:queue", 0)))
  end

  test "should limit size of dispatched events batch" do
    wrapper = Wrapper.new(@redis)
    dispatcher = ProcessingQueue.new(wrapper).dispatcher

    (1..101).each { |i| @redis.lpush("events:queue", { 'id' => i }.to_json) }

    queue_states = []
    wrapper.before(:lrange) do |*args|
      if args.first == "events:queue"
        queue_states << @redis.lrange("events:queue", 0, -1)
      end
    end

    dispatcher.dispatch_all { |ev| [1, ev['id']] }

    k = "queues:1:events"
    assert_equal(101, @redis.llen(k))
    assert_equal(1, JSON.parse(@redis.lindex(k, -1))['object'])
    assert_equal(101, JSON.parse(@redis.lindex(k, 0))['object'])

    assert_equal(3, queue_states.size)

    assert_equal(101, queue_states[0].size)
    assert_equal({ 'id' => 1 }, JSON.parse(queue_states[0].last))

    assert_equal(1, queue_states[1].size)
    assert_equal({ 'id' => 101 }, JSON.parse(queue_states[1].last))

    assert_equal(0, queue_states[2].size)
  end

  test "should reenqueue known processing operators at end of queue" do
    dispatcher = @processor.dispatcher

    @redis.sadd("queues:known", "20")
    @redis.lpush("queues:processing", "20")

    @redis.lpush("events:queue", { "id" => 1 }.to_json)
    dispatcher.dispatch_all { [10, 100] }

    assert_equal(["10", "20"], @redis.smembers("queues:known"))

    assert_equal(["20", "10"], @redis.lrange("queues:waiting", 0, -1))
    assert_equal([], @redis.lrange("queues:processing", 0, -1))
  end

  test "should reenqueue known processing operators even without events" do
    dispatcher = @processor.dispatcher

    @redis.sadd("queues:known", "20")
    @redis.lpush("queues:processing", "20")

    dispatcher.dispatch_all { [0, 0] }

    assert_equal(1, @redis.llen("queues:waiting"))
    assert_equal("20", @redis.rpop("queues:waiting"))

    assert_equal(0, @redis.llen("queues:processing"))
  end

  test "should not reenqueue known processing operators before final batch" do
    wrapper = Wrapper.new(@redis)
    dispatcher = ProcessingQueue.new(wrapper).dispatcher

    (1..101).each { |i| @redis.lpush("events:queue", { 'id' => i }.to_json) }

    @redis.sadd("queues:known", "10")
    @redis.lpush("queues:processing", "10")

    known_operators_states = []
    wrapper.before(:lrange) do |*args|
      if args.first == "events:queue"
        known_operators_states << @redis.lrange("queues:waiting", 0, -1)
      end
    end

    dispatcher.dispatch_all { [0, 0] }

    assert_equal(["10", "0", "0"], @redis.lrange("queues:waiting", 0, -1))

    assert_equal(3, known_operators_states.size)
    assert_equal([], known_operators_states[0])
    assert_equal(["0"], known_operators_states[1])
    assert_equal(["0", "0"], known_operators_states[2])
  end

  test "should not reenqueue unknown processing operators" do
    dispatcher = @processor.dispatcher

    @redis.lpush("queues:processing", "20")

    @redis.lpush("events:queue", { "id" => 1 }.to_json)
    dispatcher.dispatch_all { [10, 100] }

    assert_equal(1, @redis.llen("queues:waiting"))
    assert_equal("10", @redis.rpop("queues:waiting"))

    assert_equal(0, @redis.llen("queues:processing"))
  end

  test "should not delete processing operator added during event dispatching" do
    wrapper = Wrapper.new(@redis)
    dispatcher = ProcessingQueue.new(wrapper).dispatcher

    @redis.sadd("queues:known", "10")
    @redis.sadd("queues:known", "20")

    redis_aux = Redis.new(db: 15)
    wrapper.before(:exists) do |*args|
      if args.first == "queues:10:lock"
        redis_aux.lpush("queues:processing", "20")
      end
    end

    @redis.lpush("queues:processing", "10")
    @redis.lpush("events:queue", { "id" => 3 }.to_json)
    dispatcher.dispatch_all { [30, 300] }

    assert_equal(1, @redis.llen("queues:processing"))
    assert_equal("20", @redis.rpop("queues:processing"))
  end

  test "should ignore locked processing operator" do
    dispatcher = @processor.dispatcher

    @redis.sadd("queues:known", "20")
    @redis.lpush("queues:processing", "20")
    @redis.set("queues:20:lock", "abc")

    dispatcher.dispatch_all { [0, 0] }

    assert_equal([], @redis.lrange("queues:waiting", 0, -1))
    assert_equal(["20"], @redis.lrange("queues:processing", 0, -1))
  end

  test "should reenqueue processing operators while ignoring locked ones" do
    dispatcher = @processor.dispatcher

    (1..4).each do |i|
      op = (i * 10).to_s
      @redis.sadd("queues:known", op)
      @redis.lpush("queues:processing", op)
    end
    @redis.set("queues:20:lock", "abc")
    @redis.set("queues:40:lock", "def")

    dispatcher.dispatch_all { [0, 0] }
    assert_equal(["30", "10"], @redis.lrange("queues:waiting", 0, -1))
    assert_equal(["40", "20"], @redis.lrange("queues:processing", 0, -1))
  end

  test "should not enqueue suspended operator" do
    dispatcher = @processor.dispatcher

    @redis.sadd("queues:suspended", "30")

    @redis.lpush("events:queue", { "id" => 1 }.to_json)
    dispatcher.dispatch_all { [30, 300] }

    assert_equal(1, @redis.llen("queues:30:events"))
    assert_equal([], @redis.lrange("queues:waiting", 0, -1))
  end

  test "should not reenqueue abandoned and suspended operator" do
    dispatcher = @processor.dispatcher

    @redis.sadd("queues:suspended", "30")
    @redis.lpush("queues:processing", "30")

    @redis.lpush("events:queue", { "id" => 1 }.to_json)
    dispatcher.dispatch_all { [30, 300] }

    assert_equal(1, @redis.llen("queues:30:events"))
    assert_equal([], @redis.lrange("queues:waiting", 0, -1))
  end

  test "should not enqueue operator suspended during dispatching" do
    wrapper = Wrapper.new(@redis)
    dispatcher = ProcessingQueue.new(wrapper).dispatcher

    @redis.sadd("queues:suspended", "30")

    @redis.lpush("events:queue", { "id" => 1 }.to_json)
    @redis.lpush("events:queue", { "id" => 2 }.to_json)

    redis_aux = Redis.new(db: 15)
    wrapper.before(:sadd) do |*args|
      redis_aux.sadd("queues:suspended", "60")
      wrapper.clear
    end

    dispatcher.dispatch_all { |ev| [ev['id'] * 30, ev['id'] * 300] }

    assert_equal(1, @redis.llen("queues:30:events"))
    assert_equal(1, @redis.llen("queues:60:events"))
    assert_equal([], @redis.lrange("queues:waiting", 0, -1))
  end

  test "should post out-of-band event" do
    dispatcher = @processor.dispatcher
    dispatcher.post({ "id" => 1 }, 30, 300)

    assert_equal([{ "data" => { "id" => 1 }, "object" => 300 }.to_json],
      @redis.lrange("queues:30:events", 0, -1))
  end

  test "should not dispatch if queue is locked" do
    dispatcher = @processor.dispatcher

    @redis.lpush("events:queue", { 'id' => 1 }.to_json)
    @redis.set("events:lock", "123")

    result = dispatcher.dispatch_all do |event|
      assert_equal({ 'id' => 1 }, event)
      [10, 100]
    end
    assert_equal(false, result)

    assert_equal([{ "id" => 1 }.to_json], @redis.lrange("events:queue", 0, -1))
    assert_equal([], @redis.lrange("queues:10:events", 0, -1))
    assert_equal("123", @redis.get("events:lock"))
  end

  test "should set dispatcher lock" do
    dispatcher = @processor.dispatcher

    @redis.lpush("events:queue", { 'id' => 1 }.to_json)
    assert_throws(:interrupt) do
      dispatcher.dispatch_all { throw :interrupt }
    end

    assert_match(/^\w{8}-\w{4}-\w{4}-\w{4}-\w{12}\/#{Process.pid}$/,
      @redis.get("events:lock"))
    assert_in_delta(300_000, @redis.pttl("events:lock"), 100)
  end

  # ----- worker unit tests -----

  test "should get operator from queue" do
    worker = @processor.worker

    @redis.lpush("queues:waiting", "1")
    assert_equal("1", worker.wait_for_queue)

    assert_equal([], @redis.lrange("queues:waiting", 0, -1))
    assert_equal(["1"], @redis.lrange("queues:processing", 0, -1))
  end

  test "should process multiple events" do
    worker = @processor.worker

    @redis.sadd("queues:known", "1")
    @redis.lpush("queues:1:events", { 'val' => 1 }.to_json)
    @redis.lpush("queues:1:events", { 'val' => 2 }.to_json)

    cnt = 0
    process_result = worker.process("1") do |events|
      events.each do |event|
        cnt += 1
        assert_equal({ 'val' => cnt }, event)
      end
    end

    assert_equal(true, process_result)
    assert_equal(2, cnt)
    assert_equal([], @redis.smembers("queues:known"))
    assert_equal(0, @redis.llen("queues:1:events"))
    assert_equal(false, @redis.exists("queues:1:lock"))
  end

  test "should not change queue while processing events" do
    worker = @processor.worker

    @redis.sadd("queues:known", "1")
    @redis.lpush("queues:1:events", { 'val' => 1 }.to_json)
    @redis.lpush("queues:1:events", { 'val' => 2 }.to_json)

    worker.process("1") do |events|
      assert_equal(2, @redis.llen("queues:1:events"))
      events.each {}
      assert_equal(2, @redis.llen("queues:1:events"))
    end
    assert_equal(0, @redis.llen("queues:1:events"))
  end

  test "should increment processed event counter" do
    worker = @processor.worker

    @redis.sadd("queues:known", "1")
    @redis.lpush("queues:1:events", { 'val' => 1 }.to_json)
    @redis.lpush("queues:1:events", { 'val' => 2 }.to_json)

    worker.process("1") { |events| events.each {} }

    assert_equal("2", @redis.get("events:counters:processed"))
  end

  test "should yield events multiple times and increment processed event counter only once" do
    worker = @processor.worker

    @redis.sadd("queues:known", "1")
    @redis.lpush("queues:1:events", { 'val' => 1 }.to_json)
    @redis.lpush("queues:1:events", { 'val' => 2 }.to_json)

    processed = []

    worker.process("1") do |events|
      events.each { |ev| processed << ev }
      events.each { |ev| processed << ev }
    end

    assert_equal(4, processed.size)
    [0, 2].each { |i| assert_equal({ 'val' => 1 }, processed[i]) }
    [1, 3].each { |i| assert_equal({ 'val' => 2 }, processed[i]) }

    assert_equal("2", @redis.get("events:counters:processed"))
  end

  test "should remove only events iterated through the last time during multiple yields" do
    worker = @processor.worker

    @redis.sadd("queues:known", "1")
    @redis.lpush("queues:1:events", { 'val' => 1 }.to_json)
    @redis.lpush("queues:1:events", { 'val' => 2 }.to_json)

    processed = []

    worker.process("1") do |events|
      events.each { |ev| processed << ev }
      events.each { |ev| processed << ev; break if ev['val'] == 1 }
    end

    assert_equal(3, processed.size)
    assert_equal("1", @redis.get("events:counters:processed"))

    processed = []
    worker.process("1") do |events|
      events.each { |ev| processed << ev }
    end

    assert_equal(1, processed.size)
    assert_equal({ 'val' => 2 }, processed[0])
    assert_equal("2", @redis.get("events:counters:processed"))
  end

  test "should increment statistical counters after processing" do
    res = ProcessingQueue::PERF_COUNTER_RESOLUTION

    # Worker is going to call get_time 4 times (once in the beginning, once for
    # each event, once in the end), totalling 70% of one slot time.
    times = [0.0, 0.1, 0.3, 0.7].map { |x| res * (100 + x) }
    ProcessingQueue.expects(:get_time).times(4).returns(*times)

    # Server time is exactly 400 slots (500 - 100) ahead of client
    t = epoch_to_redis_time(res * (500 + 0.0))
    @redis.expects(:call).with('TIME').returns(t)

    worker = @processor.worker

    @redis.sadd("queues:known", "1")
    @redis.lpush("queues:1:events", { 'val' => 1 }.to_json)
    @redis.lpush("queues:1:events", { 'val' => 2 }.to_json)

    worker.process("1") { |events| events.each {} }

    # Should have counted 2 events and 70% of a slot time
    assert_in_delta(0.7 * res, @redis.get("events:counters:500:time"), 1e-6)
    assert_equal("2", @redis.get("events:counters:500:count"))

    # Worker is going to call get_time 3 times, totalling 20% of one slot time.
    times = [0.5, 0.7, 0.7].map { |x| res * (100 + x) }
    ProcessingQueue.expects(:get_time).times(3).returns(*times)

    t = epoch_to_redis_time(res * (500 + 0.5))
    @redis.expects(:call).with('TIME').returns(t)

    worker = @processor.worker

    @redis.sadd("queues:known", "1")
    @redis.lpush("queues:1:events", { 'val' => 1 }.to_json)

    worker.process("1") { |events| events.each {} }

    # Should have counted a total of 3 events and 90% of a slot time
    assert_in_delta(0.9 * res, @redis.get("events:counters:500:time"), 1e-6)
    assert_equal("3", @redis.get("events:counters:500:count"))
  end

  test "should increment statistical counters with fractional redis epoch" do
    res = ProcessingQueue::PERF_COUNTER_RESOLUTION

    t = epoch_to_redis_time(res * 500 - 2e-6)
    @redis.expects(:call).with('TIME').returns(t)

    # first event ends at the last milisecond of the first slot; second event
    # ends at the first milisecond of the second slot
    times = [1_000.0, 1_000.000_001, 1_000.000_002, 1_000.000_002]
    ProcessingQueue.expects(:get_time).times(4).returns(*times)

    worker = @processor.worker

    @redis.sadd("queues:known", "1")
    @redis.lpush("queues:1:events", { 'val' => 1 }.to_json)
    @redis.lpush("queues:1:events", { 'val' => 2 }.to_json)

    worker.process("1") { |events| events.each {} }

    assert_equal("1", @redis.get("events:counters:499:count"))
    assert_equal("1", @redis.get("events:counters:500:count"))
  end

  test "should lock operator before processing" do
    worker = @processor.worker

    @redis.sadd("queues:known", "1")
    @redis.lpush("queues:1:events", { 'val' => 1 }.to_json)
    assert_raises(Error) do
      worker.process("1") { |events| events.each { raise Error, "abort" } }
    end

    assert_equal(1, @redis.llen("queues:1:events"))
    assert_match(/^\w{8}-\w{4}-\w{4}-\w{4}-\w{12}\/#{Process.pid}$/,
      @redis.get("queues:1:lock"))
  end

  test "should lock operator with timeout" do
    worker = @processor.worker

    @redis.lpush("queues:1:events", { 'val' => 1 }.to_json)
    assert_raises(Error) do
      worker.process("1") { |events| events.each { raise Error, "abort" } }
    end

    assert_in_delta(300_000, @redis.pttl("queues:1:lock"), 100)
  end

  test "should not process events if operator is locked" do
    worker = @processor.worker

    @redis.lpush("queues:1:events", { 'val' => 1 }.to_json)
    @redis.set("queues:1:lock", "1234")

    cnt = 0
    process_result = worker.process("1") { |events| events.each { cnt += 1 } }

    assert_equal(false, process_result)
    assert_equal(0, cnt)
    assert_equal(1, @redis.llen("queues:1:events"))
  end

  test "should not unlock lock from another worker" do
    wrapper = Wrapper.new(@redis)
    worker = ProcessingQueue.new(wrapper).worker

    @redis.sadd("queues:known", "1")
    @redis.lpush("queues:1:events", { 'val' => 1 }.to_json)

    wrapper.before(:rpop) { @redis.set("queues:1:lock", "1234") }
    worker.process("1") { |events| events.each {} }

    assert_equal("1234", @redis.get("queues:1:lock"))
  end

  test "should not remove operator from known set if queue is not empty" do
    wrapper = Wrapper.new(@redis)
    worker = ProcessingQueue.new(wrapper).worker

    @redis.sadd("queues:known", "1")
    @redis.lpush("queues:1:events", { 'val' => 1 }.to_json)

    wrapper.before(:multi) do
      @redis.lpush("queues:1:events", { 'val' => 2 }.to_json)
    end
    worker.process("1") { |events| events.each {} }

    assert_equal(["1"], @redis.smembers("queues:known"))
    assert_equal(1, @redis.llen("queues:1:events"))
    assert_equal(false, @redis.exists("queues:1:lock"))
  end

  test "should stop processing if interrupted" do
    worker = @processor.worker

    @redis.sadd("queues:known", "1")
    @redis.lpush("queues:1:events", { 'val' => 1 }.to_json)
    @redis.lpush("queues:1:events", { 'val' => 2 }.to_json)

    worker.process("1") do |events|
      events.each { worker.instance_variable_set(:@interrupted, 1) }
    end

    assert_equal(["1"], @redis.smembers("queues:known"))
    assert_equal(1, @redis.llen("queues:1:events"))
    assert_equal(false, @redis.exists("queues:1:lock"))
  end

  test "should continue processing if new events arrive after batch starts" do
    worker = @processor.worker

    @redis.sadd("queues:known", "1")
    @redis.lpush("queues:1:events", { 'val' => 1 }.to_json)
    @redis.lpush("queues:1:events", { 'val' => 2 }.to_json)

    cnt = 0
    worker.process("1") do |events|
      events.each do |event|
        cnt += 1
        if cnt == 2
          @redis.lpush("queues:1:events", { 'val' => 3 }.to_json)
          @redis.lpush("queues:1:events", { 'val' => 4 }.to_json)
        end
      end
    end

    assert_equal(4, cnt)
    assert_equal([], @redis.smembers("queues:known"))
    assert_equal(0, @redis.llen("queues:1:events"))
  end

  test "should limit worker batch size" do
    worker = @processor.worker

    @redis.sadd("queues:known", "1")
    (1..150).each do |i|
      @redis.lpush("queues:1:events", { 'val' => 1 }.to_json)
    end

    cnt = 0
    worker.process("1") { |events| events.each { cnt += 1 } }

    assert_equal(100, cnt)
    assert_equal(["1"], @redis.smembers("queues:known"))
    assert_equal(50, @redis.llen("queues:1:events"))
  end

  test "should allow custom worker batch size" do
    worker = @processor.worker(:max_batch_size => 2)

    (1..3).each { |i| @redis.lpush("queues:1:events", { 'val' => 1 }.to_json) }

    cnt = 0
    worker.process("1") { |events| events.each { cnt += 1 } }

    assert_equal(2, cnt)
    assert_equal(1, @redis.llen("queues:1:events"))
  end

  test "should delete current operator's other entries from waiting queues list" do
    worker = @processor.worker

    @redis.sadd("queues:known", "1")
    @redis.lpush("queues:waiting", "1")
    @redis.lpush("queues:1:events", { 'val' => 1 }.to_json)

    worker.process("1") { |events| events.each {} }

    assert_equal([], @redis.lrange("queues:waiting", 0, -1))
  end

  test "should not delete other operators' entries from waiting queues list" do
    worker = @processor.worker

    @redis.sadd("queues:known", "1")
    @redis.lpush("queues:waiting", "2")
    @redis.lpush("queues:1:events", { 'val' => 1 }.to_json)

    worker.process("1") { |events| events.each {} }

    assert_equal(["2"], @redis.lrange("queues:waiting", 0, -1))
  end

  test "should not delete locked operators's entries from waiting queues list" do
    worker = @processor.worker

    @redis.sadd("queues:known", "1")
    @redis.lpush("queues:waiting", "1")
    @redis.set("queues:1:lock", "1234")
    @redis.lpush("queues:1:events", { 'val' => 1 }.to_json)

    worker.process("1") { |events| events.each {} }

    assert_equal(["1"], @redis.lrange("queues:waiting", 0, -1))
  end

  test "should reenqueue operator immediatelly if events exist after batch" do
    worker = @processor.worker

    @redis.sadd("queues:known", "1")
    (1..101).each do |i|
      @redis.lpush("queues:1:events", { 'val' => 1 }.to_json)
    end

    worker.process("1") { |events| events.each {} }

    assert_equal(1, @redis.llen("queues:1:events"))
    assert_equal(["1"], @redis.lrange("queues:waiting", 0, -1))
  end

  test "should not reenqueue operator if queue is found to be suspended after batch" do
    worker = @processor.worker

    @redis.sadd("queues:known", "1")
    @redis.sadd("queues:suspended", "1")
    (1..101).each do |i|
      @redis.lpush("queues:1:events", { 'val' => 1 }.to_json)
    end

    worker.process("1") { |events| events.each {} }

    assert_equal(1, @redis.llen("queues:1:events"))
    assert_equal([], @redis.lrange("queues:waiting", 0, -1))
  end

  test "should be able to break from processing" do
    worker = @processor.worker

    @redis.sadd("queues:known", "1")
    (1..3).each do |i|
      @redis.lpush("queues:1:events", { 'val' => 1 }.to_json)
    end

    worker.process("1") { |events| events.each { break } }

    assert_equal(["1"], @redis.smembers("queues:known"))
    assert_equal(2, @redis.llen("queues:1:events"))
  end

  # ----- general processing queue tests -----

  test "should add queue to suspended list" do
    @redis.lpush("queues:waiting", "1")
    @redis.lpush("queues:waiting", "2")
    @redis.lpush("queues:waiting", "1")

    @processor.suspend_queue("1")

    assert_equal(["1"], @redis.smembers("queues:suspended"))
    assert_equal(["2"], @redis.lrange("queues:waiting", 0, -1))
  end

  test "should resume queue without events" do
    @redis.sadd("queues:suspended", "1")

    @processor.resume_queue("1")

    assert_equal([], @redis.smembers("queues:suspended"))
    assert_equal([], @redis.smembers("queues:known"))
    assert_equal([], @redis.lrange("queues:waiting", 0, -1))
  end

  test "should resume queue with events" do
    @redis.sadd("queues:suspended", "1")
    @redis.lpush("queues:1:events", "a")

    @processor.resume_queue("1")

    assert_equal([], @redis.smembers("queues:suspended"))
    assert_equal(["1"], @redis.smembers("queues:known"))
    assert_equal(["1"], @redis.lrange("queues:waiting", 0, -1))
  end

  test "should not resume unsuspended queue" do
    @redis.lpush("queues:1:events", "a")
    @redis.lpush("queues:waiting", "1")

    @processor.resume_queue("1")

    assert_equal(["1"], @redis.lrange("queues:waiting", 0, -1))
  end

  # ----- statistics unit tests -----

  test "should count received events" do
    @redis.set('events:counters:dispatched', 4)
    ('a'..'c').each { |c| @redis.lpush('events:queue', c) }
    @statistics = ProcessingQueue::Statistics.new(@redis)

    assert_equal(7, @statistics.received_count)
  end

  test "should count processed events" do
    @redis.set('events:counters:processed', 13)
    @statistics = ProcessingQueue::Statistics.new(@redis)

    assert_equal(13, @statistics.processed_count)
  end

  test "should calculate queue length" do
    ('a'..'c').each { |c| @redis.lpush('events:queue', c) }
    @statistics = ProcessingQueue::Statistics.new(@redis)

    assert_equal(3, @statistics.queue_length)
  end

  test "should get known operator status" do
    @redis.sadd("queues:known", '1')
    @statistics = ProcessingQueue::Statistics.new(@redis)

    assert_equal(1, @statistics.queues.count)
    operator = @statistics.queues.first
    assert_nil(operator.queue_pos)
    assert_false(operator.taken?)
  end

  test "should get queued operator status" do
    @redis.sadd("queues:known", '1')
    @redis.lpush("queues:waiting", '1')
    @statistics = ProcessingQueue::Statistics.new(@redis)

    assert_equal(1, @statistics.queues.count)
    operator = @statistics.queues.first
    assert_false(operator.locked?)
    assert_equal(0, operator.queue_pos)
    assert_false(operator.taken?)
  end

  test "should get position of multiple queued operators" do
    ['3', '2', '1'].each do |x|
      @redis.sadd("queues:known", x)
      @redis.lpush("queues:waiting", x)
    end
    # should consider only first entry if operator was queued twice
    @redis.lpush("queues:waiting", '3')

    @statistics = ProcessingQueue::Statistics.new(@redis)
    assert_equal(3, @statistics.queues.count)

    [[0, '3'], [1, '2'], [2, '1']].each_with_index do |(pos, name), i|
      assert_equal(name, @statistics.queues[i].name)
      assert_equal(pos, @statistics.queues[i].queue_pos)
    end
  end

  test "should get suspended operator status" do
    @redis.sadd("queues:suspended", '1')
    @statistics = ProcessingQueue::Statistics.new(@redis)

    assert_equal(1, @statistics.queues.count)
    operator = @statistics.queues.first
    assert_true(operator.suspended?)
  end

  test "should get processing operator status" do
    @redis.sadd("queues:known", '1')
    @redis.lpush("queues:processing", '1')
    @statistics = ProcessingQueue::Statistics.new(@redis)

    assert_equal(1, @statistics.queues.count)
    operator = @statistics.queues.first
    assert_false(operator.locked?)
    assert_nil(operator.queue_pos)
    assert_true(operator.taken?)
  end

  test "should calculate operator queue length" do
    @redis.sadd("queues:known", '1')
    ('a'..'c').each { |c| @redis.lpush("queues:1:events", c) }
    @statistics = ProcessingQueue::Statistics.new(@redis)

    assert_equal(1, @statistics.queues.count)
    operator = @statistics.queues.first
    assert_equal(3, operator.count)
  end

  test "should calculate waiting event count" do
    @redis.sadd("queues:known", '1')
    ('a'..'c').each { |c| @redis.lpush("queues:1:events", c) }
    @redis.sadd("queues:known", '2')
    ('d'..'e').each { |c| @redis.lpush("queues:2:events", c) }
    @statistics = ProcessingQueue::Statistics.new(@redis)

    assert_equal(5, @statistics.waiting_count)
  end

  test "should show operator lock status" do
    @redis.sadd("queues:known", '1')
    @redis.set("queues:1:lock", '1234567890/1500')
    @redis.pexpire("queues:1:lock", 60_000)
    @statistics = ProcessingQueue::Statistics.new(@redis)

    assert_equal(1, @statistics.queues.count)
    operator = @statistics.queues.first
    assert_true(operator.locked?)
    assert_equal('1500', operator.locked_by)
    assert_in_delta(60_000, operator.ttl, 50)
  end

  test "should sort operators by name" do
    @redis.sadd("queues:known", '+11')
    @redis.sadd("queues:known", '10')
    @redis.sadd("queues:known", 'a')
    @redis.sadd("queues:known", '{c}')
    @statistics = ProcessingQueue::Statistics.new(@redis)

    assert_equal(4, @statistics.queues.count)
    assert_equal(['+11', '10', 'a', '{c}'], @statistics.queues.map(&:name))
  end

  test "should sort operators by queue position" do
    @redis.sadd("queues:known", '+11')
    @redis.sadd("queues:known", '10')
    @redis.sadd("queues:known", 'a')
    @redis.lpush("queues:waiting", 'a')
    @redis.lpush("queues:waiting", '10')
    @statistics = ProcessingQueue::Statistics.new(@redis)

    assert_equal(3, @statistics.queues.count)
    assert_equal(['a', '10', '+11'], @statistics.queues.map(&:name))
  end

  test "should sort operators by lock status" do
    @redis.sadd("queues:known", '+11')
    @redis.sadd("queues:known", '10')
    @redis.sadd("queues:known", 'a')
    @redis.lpush("queues:waiting", 'a')
    @redis.set("queues:10:lock", '123/123')
    @statistics = ProcessingQueue::Statistics.new(@redis)

    assert_equal(3, @statistics.queues.count)
    assert_equal(['10', 'a', '+11'], @statistics.queues.map(&:name))
  end

  test "should sort operators by locker pid" do
    @redis.sadd("queues:known", '+11')
    @redis.sadd("queues:known", '10')
    @redis.sadd("queues:known", 'a')
    @redis.set("queues:10:lock", 'abcd/1001')
    @redis.set("queues:a:lock", 'abcd/101')
    @redis.lpush("queues:waiting", '10')
    @statistics = ProcessingQueue::Statistics.new(@redis)

    assert_equal(3, @statistics.queues.count)
    assert_equal(['a', '10', '+11'], @statistics.queues.map(&:name))
  end

  test "should sort operators by suspend state" do
    @redis.sadd("queues:known", '+11')
    @redis.sadd("queues:known", '10')
    @redis.sadd("queues:known", 'a')
    @redis.set("queues:a:lock", '123/123')
    @redis.sadd("queues:suspended", '10')
    @statistics = ProcessingQueue::Statistics.new(@redis)

    assert_equal(3, @statistics.queues.count)
    assert_equal(['10', 'a', '+11'], @statistics.queues.map(&:name))
  end

  test "should count events in last minute" do
    prepare_event_counter_test(1, 9, 2.22)
    @statistics = ProcessingQueue::Statistics.new(@redis)

    counter = @statistics.counters[0]
    assert_equal(9 * SLOTS_PER_MIN, counter.count)
    assert_equal(9 * SLOTS_PER_MIN, counter.count_per_min)
    assert_in_delta(2.22 * SLOTS_PER_MIN / 60, counter.time_busy, 0.1)
  end

  test "should count events in last 5 minutes" do
    prepare_event_counter_test(5, 10, 3.33)
    @statistics = ProcessingQueue::Statistics.new(@redis)

    counter = @statistics.counters[1]
    assert_equal(10 * SLOTS_PER_MIN * 5, counter.count)
    assert_equal(10 * SLOTS_PER_MIN, counter.count_per_min)
    assert_in_delta(3.33 * SLOTS_PER_MIN / 60, counter.time_busy, 0.1)
  end

  test "should count events in last 15 minutes" do
    prepare_event_counter_test(15, 11, 4.44)
    @statistics = ProcessingQueue::Statistics.new(@redis)

    counter = @statistics.counters[2]
    assert_equal(11 * SLOTS_PER_MIN * 15, counter.count)
    assert_equal(11 * SLOTS_PER_MIN, counter.count_per_min)
    assert_in_delta(4.44 * SLOTS_PER_MIN / 60, counter.time_busy, 0.1)
  end

  private
    def prepare_event_counter_test(mins, count_per_slot, time_per_slot)
      t = 100_001.0
      slot = slot_number(t)
      (1..(SLOTS_PER_MIN * mins)).each do |i|
        @redis.set("events:counters:#{slot - i}:count", count_per_slot)
        @redis.set("events:counters:#{slot - i}:time", time_per_slot)
      end
      # We want to calculate the last {SLOTS_PER_MIN * mins} *whole* slots. So,
      # it should not include (we create a counter with 10 times the normal
      # event count/time so it's easy to notice any calculation error):
      # - the current slot
      @redis.set("events:counters:#{slot}:count", count_per_slot * 10)
      @redis.set("events:counters:#{slot}:time", time_per_slot * 10)
      # - any slot before the last {SLOTS_PER_MIN * mins} whole slots.
      @redis.set("events:counters:#{slot - (SLOTS_PER_MIN * mins) - 1}:count",
        count_per_slot * 10)
      @redis.set("events:counters:#{slot - (SLOTS_PER_MIN * mins) - 1}:time",
        time_per_slot * 10)

      @redis.expects(:call).with('TIME').returns(epoch_to_redis_time(t))
    end
end
