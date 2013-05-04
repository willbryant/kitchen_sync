require 'fileutils'
require 'net/http'
require 'pp' # **

class KitchenSyncSpawner
  STARTUP_TIMEOUT = 10 # seconds
  
  attr_reader :program_binary, :capture_stderr_in
  
  def initialize(program_binary, program_args, options = {})
    @program_binary = program_binary
    @program_args = program_args
    @capture_stderr_in = options[:capture_stderr_in]
    raise "Can't see a program binary at #{program_binary}" unless File.executable?(program_binary)
  end
  
  def start_binary
    exec_args = [@program_binary] + @program_args
    
    if ENV['VALGRIND']
      exec_args.unshift "--leak-check=full" if ENV['VALGRIND'] == "full"
      exec_args.unshift "valgrind"
    end
    
    stdin_r, stdin_w = IO.pipe
    stdout_r, stdout_w = IO.pipe
    @child_pid = fork do
      begin
        stdin_w.close
        stdout_r.close
        STDIN.reopen(stdin_r)
        STDOUT.reopen(stdout_w)
        STDERR.reopen(@capture_stderr_in, "wb") if @capture_stderr_in
      rescue => e
        puts e
        exit 1
      end
      exec *exec_args
    end
    stdin_r.close
    stdout_w.close
    @program_stdin = stdin_w
    @program_stdout = stdout_r
  end
  
  def stop_binary
    return unless @child_pid
    @program_stdin.close unless @program_stdin.closed?
    @program_stdout.close
    Process.kill('TERM', @child_pid)
    Process.wait(@child_pid)
    @child_pid = nil
    @unpacker = nil
  end

  def stderr_contents
    File.read(@capture_stderr_in)
  end

  def expect_stderr(contents)
    @expected_stderr_contents = contents
    yield
  ensure
    @expected_stderr_contents = nil
  end

  def unpacker
    @unpacker ||= MessagePack::Unpacker.new(@program_stdout)
  end

  def send_command(*args)
    @program_stdin.write(args.to_msgpack)
    unpacker.read
  ensure
    stderr = stderr_contents
    fail "Unexpected stderr output: #{stderr_contents.chomp}" unless stderr_contents == (@expected_stderr_contents || "")
  end

  def quit
    send_command("quit")
  end
end
