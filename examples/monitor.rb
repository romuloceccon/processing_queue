require 'curses'
require 'redis'
require 'processing_queue'

class Header
  def initialize(message, top)
    @top = top
    @cols = Curses.cols
    @message = message

    @window = Curses::Window.new(3, @cols, @top, 0)
  end

  def resize
    @cols = Curses.cols
    @window.resize(3, @cols)
  end

  def refresh
    @window.clear

    x = [(@cols - @message.size) / 2, 0].max
    @window.setpos(0, x)
    @window.addstr(@message[0..[@cols - 1, @message.size - 1].min])

    @window.setpos(1, 0)
    @window.addstr("=" * @cols)
    @window.noutrefresh
  end

  def close
    @window.close
  end
end

class Events
  def initialize(stats, top)
    @top = top
    @stats = stats

    @window = Curses::Window.new(1, 80, @top, 0)
  end

  def resize

  end

  def refresh
    @window.clear

    @window.setpos(0, 0)
    @window.addstr('Events: ')

    @window.attron(Curses::A_BOLD)
    @window.addstr('%6d' % [@stats.received_count])
    @window.attroff(Curses::A_BOLD)
    @window.addstr(' received, ')

    @window.attron(Curses::A_BOLD)
    @window.addstr('%6d' % [@stats.queue_length])
    @window.attroff(Curses::A_BOLD)
    @window.addstr(' queued, ')

    @window.attron(Curses::A_BOLD)
    @window.addstr('%6d' % [@stats.waiting_count])
    @window.attroff(Curses::A_BOLD)
    @window.addstr(' waiting, ')

    @window.attron(Curses::A_BOLD)
    @window.addstr('%6d' % [@stats.processed_count])
    @window.attroff(Curses::A_BOLD)
    @window.addstr(' processed')

    @window.noutrefresh
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

  def resize

  end

  def refresh
    @window.clear

    @window.setpos(0, 0)
    @window.addstr('Worker load:         ')

    @window.attron(Curses::A_BOLD)
    (0..2).each do |i|
      @window.addstr('%10.2f' % [@stats.counters[i].time_busy])
    end
    @window.attroff(Curses::A_BOLD)

    @window.setpos(1, 0)
    @window.addstr('Throughput (ev/min): ')

    @window.attron(Curses::A_BOLD)
    (0..2).each do |i|
      @window.addstr('%10d' % [@stats.counters[i].count_per_min])
    end
    @window.attroff(Curses::A_BOLD)

    @window.noutrefresh
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

  def resize
    @cols, @lines = Curses.cols, Curses.lines
    @window.resize(@lines - @top, @cols)
  end

  def refresh
    @window.clear
    @window.setpos(0, 0)

    @window.attron(Curses::A_BOLD)
    @window.attron(Curses::A_REVERSE)
    header = 'NAME        SIZE   WORKER      TTL  QUEUED?  TAKEN?'
    @window.addstr(header)
    @window.addstr(' ' * (@cols - header.size)) if header.size < @cols
    @window.attroff(Curses::A_BOLD)
    @window.attroff(Curses::A_REVERSE)

    @stats.queues.each_with_index do |queue, i|
      y = i + 1
      break if y >= @window.maxy

      @window.setpos(y, 0)
      @window.addstr(queue.name)

      @window.setpos(y, 9)
      @window.addstr("%7d" % [queue.count])

      if queue.locked?
        @window.setpos(y, 18)
        @window.addstr("%7s" % [queue.locked_by])

        @window.setpos(y, 27)
        @window.addstr("%7d" % [queue.ttl])
      end

      if queue.queued?
        @window.setpos(y, 36 + 3)
        @window.addstr("X")
      end

      if queue.taken?
        @window.setpos(y, 45 + 3)
        @window.addstr("X")
      end
    end

    @window.noutrefresh
  end

  def get_key
    @window.getch
  end

  def close
    @window.close
  end
end

class WindowList
  def initialize(windows, main)
    @windows = windows
    @main = main
  end

  def close
    @windows.each(&:close)
  end

  def get_key
    @main.get_key
  end

  def refresh
    @windows.each(&:refresh)
  end

  def resize
    @windows.each(&:resize)
  end
end

Curses.init_screen
begin
  Curses.crmode
  Curses.noecho
  Curses.curs_set(0)

  stats = ProcessingQueue::Statistics.new(Redis.new)

  header = Header.new('VMpay queue monitor', 0)
  events = Events.new(stats, 3)
  performance = Performance.new(stats, 4)
  operators = Operators.new(stats, 7)
  windows = WindowList.new([header, events, performance, operators], operators)

  begin
    loop do
      stats.update

      windows.refresh
      Curses.doupdate

      key = windows.get_key
      break if key == 'q'
      windows.resize if key == Curses::Key::RESIZE
    end
  ensure
    windows.close
  end
ensure
  Curses.close_screen
end
