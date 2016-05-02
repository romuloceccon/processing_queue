require 'curses'
require 'redis'
require 'processor'

class Stats
  attr_reader :events_received, :events_processed, :events_queued,
    :events_waiting, :operators, :counter_times, :counter_counts

  class Operator
    attr_reader :name, :count, :locked_by, :ttl

    def initialize(redis, operator_id, op_queued, op_processing)
      @name = operator_id
      @count = redis.llen("operators:#{operator_id}:events")
      lock_name = "operators:#{operator_id}:lock"
      if lock = redis.get(lock_name)
        @locked = true
        @locked_by = lock.split('/').last
        @ttl = redis.pttl(lock_name)
      end
      @queued = op_queued.include?(operator_id)
      @taken = op_processing.include?(operator_id)
    end

    def locked?
      return @locked
    end

    def queued?
      return @queued
    end

    def taken?
      return @taken
    end

    def <=>(other)
      return locked? ? -1 : 1 if locked? ^ other.locked?
      return other.count <=> count if count != other.count

      return name.to_i <=> other.name.to_i
    end
  end

  def initialize
    @redis = Redis.new
    update
  end

  def update
    @events_received = @redis.get('events:counters:received').to_i
    @events_processed = @redis.get('events:counters:processed').to_i
    @events_queued = @redis.llen('events:queue')

    op_known, op_queued, op_processing = @redis.multi do
      @redis.smembers('operators:known')
      @redis.lrange('operators:queue', 0, -1)
      @redis.lrange('operators:processing', 0, -1)
    end

    all = (op_known + op_queued + op_processing).uniq

    @operators = all.map do |operator_id|
      Operator.new(@redis, operator_id, op_queued, op_processing)
    end
    @operators.sort!

    @events_waiting = @operators.inject(0) { |r, obj| r + obj.count }

    res = Processor::PERF_COUNTER_RESOLUTION

    t = Process.clock_gettime(Process::CLOCK_MONOTONIC)
    slot = (t / res).to_i
    values = @redis.multi do
      slot.downto(slot - 900 / res + 1).each do |s|
        @redis.get("events:counters:#{s}:time")
        @redis.get("events:counters:#{s}:count")
      end
    end

    @counter_times = [0.0, 0.0, 0.0]
    @counter_counts = [0, 0, 0]
    # 1 min, 5 min, 15 min
    [60, 300, 900].each_with_index do |t, i|
      cnt = t / res
      0.upto(cnt - 1).each do |p|
        @counter_times[i] += values[p * 2].to_f
        @counter_counts[i] += values[p * 2 + 1].to_i
      end
      @counter_times[i] /= t
      @counter_counts[i] /= (t / 60)
    end
  end
end

class Header
  def initialize(message, top)
    @top = top
    @cols = Curses.cols
    @message = message
    @window = Curses::Window.new(3, @cols, @top, 0)

    update_window
    @window.refresh
  end

  def update
    if @cols != Curses.cols
      @cols = Curses.cols
      @window.resize(3, @cols)

      update_window
      @window.refresh
    end
  end

  def close
    @window.close
  end

  private
    def update_window
      @window.clear

      x = [(@cols - @message.size) / 2, 0].max
      @window.setpos(0, x)
      @window.addstr(@message[0..[@cols - 1, @message.size - 1].min])

      @window.setpos(1, 0)
      @window.addstr("=" * @cols)
    end
end

class Events
  def initialize(stats, top)
    @top = top
    @stats = stats

    @window = Curses::Window.new(1, 80, @top, 0)
  end

  def update
    @window.clear

    @window.setpos(0, 0)
    @window.addstr('Events: ')

    @window.attron(Curses::A_BOLD)
    @window.addstr('%6d' % [@stats.events_received])
    @window.attroff(Curses::A_BOLD)
    @window.addstr(' received, ')

    @window.attron(Curses::A_BOLD)
    @window.addstr('%6d' % [@stats.events_queued])
    @window.attroff(Curses::A_BOLD)
    @window.addstr(' queued, ')

    @window.attron(Curses::A_BOLD)
    @window.addstr('%6d' % [@stats.events_waiting])
    @window.attroff(Curses::A_BOLD)
    @window.addstr(' waiting, ')

    @window.attron(Curses::A_BOLD)
    @window.addstr('%6d' % [@stats.events_processed])
    @window.attroff(Curses::A_BOLD)
    @window.addstr(' processed')

    @window.refresh
  end

  def close
    @window.close
  end
end

class Performance
  def initialize(stats, top)
    @top = top
    @stats = stats

    @window = Curses::Window.new(2, 80, @top, 0)
  end

  def update
    @window.clear

    @window.setpos(0, 0)
    @window.addstr('Worker load:         ')

    @window.attron(Curses::A_BOLD)
    (0..2).each do |i|
      @window.addstr('%10.2f' % [@stats.counter_times[i]])
    end
    @window.attroff(Curses::A_BOLD)

    @window.setpos(1, 0)
    @window.addstr('Throughput (ev/min): ')

    @window.attron(Curses::A_BOLD)
    (0..2).each do |i|
      @window.addstr('%10d' % [@stats.counter_counts[i]])
    end
    @window.attroff(Curses::A_BOLD)

    @window.refresh
  end

  def close
    @window.close
  end
end

class Operators
  def initialize(stats, top)
    @top = top
    @stats = stats
    @cols, @lines = Curses.cols, Curses.lines

    @window = Curses::Window.new(@lines - @top, @cols, @top, 0)
    @window.timeout = 1000
  end

  def update
    if @lines != Curses.lines || @cols != Curses.cols
      @cols, @lines = Curses.cols, Curses.lines
      @window.resize(@lines - @top, @cols)
    end

    @window.clear
    @window.setpos(0, 0)

    @window.attron(Curses::A_BOLD)
    @window.attron(Curses::A_REVERSE)
    header = 'NAME        SIZE   WORKER      TTL  QUEUED?  TAKEN?'
    @window.addstr(header)
    @window.addstr(' ' * (@cols - header.size)) if header.size < @cols
    @window.attroff(Curses::A_BOLD)
    @window.attroff(Curses::A_REVERSE)

    @stats.operators.each_with_index do |operator, i|
      y = i + 1
      break if y >= @window.maxy

      @window.setpos(y, 0)
      @window.addstr(operator.name)

      @window.setpos(y, 9)
      @window.addstr("%7d" % [operator.count])

      if operator.locked?
        @window.setpos(y, 18)
        @window.addstr("%7s" % [operator.locked_by])

        @window.setpos(y, 27)
        @window.addstr("%7d" % [operator.ttl])
      end

      if operator.queued?
        @window.setpos(y, 36 + 3)
        @window.addstr("X")
      end

      if operator.taken?
        @window.setpos(y, 45 + 3)
        @window.addstr("X")
      end
    end

    @window.refresh
  end

  def wait
    @window.getch == 'q'
  end

  def close
    @window.close
  end
end

Curses.init_screen
begin
  Curses.crmode
  Curses.noecho
  Curses.curs_set(0)

  stats = Stats.new

  header = Header.new('VMpay queue monitor', 0)
  events = Events.new(stats, 3)
  performance = Performance.new(stats, 4)
  operators = Operators.new(stats, 7)

  begin
    loop do
      stats.update

      header.update
      events.update
      performance.update
      operators.update

      break if operators.wait
    end
  ensure
    header.close
    events.close
    performance.close
    operators.close
  end
ensure
  Curses.close_screen
end
