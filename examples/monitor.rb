require 'curses'
require 'redis'
require 'processor'

class Stats
  attr_reader :events_received, :events_processed, :events_queued,
    :events_waiting, :operators

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
  operators = Operators.new(stats, 5)

  begin
    loop do
      stats.update

      header.update
      events.update
      operators.update

      break if operators.wait
    end
  ensure
    header.close
    events.close
    operators.close
  end
ensure
  Curses.close_screen
end
