require 'curses'
require 'redis'

redis = Redis.new
known_workers = []

Curses.init_screen
begin
  Curses.crmode
  Curses.noecho
  Curses.curs_set(0)

  Curses.setpos(0, 0)
  Curses.addstr('Queue monitor')
  Curses.setpos(1, 0)
  Curses.addstr('=============')
  Curses.refresh

  win = Curses::Window.new(Curses.lines - 3, Curses.cols, 3, 0)
  win.timeout = 1000
  loop do
    win.clear

    win.setpos(0, 0)
    win.addstr("len events:queue: %d" % [redis.llen("events:queue")])

    known = redis.smembers("operators:known")
    win.setpos(2, 0)
    win.addstr("operators:known: %s" % [known.join(', ')])
    win.setpos(3, 0)
    operators, processing = redis.multi do
      redis.lrange("operators:queue", 0, -1)
      redis.lrange("operators:processing", 0, -1)
    end
    msg = operators.join(', ')
    if msg.size + 17 > win.maxx
      msg = "... " + msg[(17 - win.maxx + 4)..-1]
    end
    win.addstr("operators:queue: %s" % [msg])

    (operators + processing).uniq.sort { |a, b| a.to_i <=> b.to_i }.each_with_index do |op, i|
      win.setpos(5 + i, 0)
      s = "operators:#{op}:events"
      w = "operators:#{op}:lock"
      ttl = redis.pttl(w)
      p = -1
      if lock = redis.get(w)
        unless p = known_workers.index(lock)
          p = known_workers.size
          known_workers << lock
        end
        win.addstr("len %s: %d (worker %d ttl %d)" % [s, redis.llen(s), p, ttl])
      else
        win.addstr("len %s: %d" % [s, redis.llen(s)])
      end
    end

    win.refresh

    break if win.getch == 'q'
  end
ensure
  Curses.close_screen
end
