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
      @capture_stderr_in = nil
    end

    if ENV['OS_X_MALLOC_CHECKS']
      ENV['MallocStackLogging'] = '1'
      ENV['MallocScribble'] = '1'
      ENV['MallocPreScribble'] = '1'
      ENV['MallocGuardEdges'] = '1'
      ENV['MallocCheckHeapStart'] = '1'
      ENV['MallocCheckHeapEach'] = '1'
      @capture_stderr_in = nil
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
    Process.kill('TERM', @child_pid) if @child_pid
    wait
    @unpacker = nil
  end

  def wait
    Process.wait(@child_pid) if @child_pid
    @child_pid = nil
  end

  def stderr_contents
    File.read(@capture_stderr_in).chomp if @capture_stderr_in
  end

  def expect_stderr(contents)
    @expected_stderr_contents = contents if @capture_stderr_in
    yield
  ensure
    if stderr_contents != @expected_stderr_contents
      fail "Unexpected stderr output: #{stderr_contents.inspect}; should be #{@expected_stderr_contents.inspect}"
    end
    @expected_stderr_contents = nil
  end

  def expected_stderr_contents
    @expected_stderr_contents || "" if @capture_stderr_in
  end

  def unpacker
    @unpacker ||= MessagePack::Unpacker.new(@program_stdout)
  end

  def send_command(*args)
    @program_stdin.write(args.to_msgpack)
    unpacker.read
  ensure
    if stderr_contents && stderr_contents != "" && !@expected_stderr_contents
      fail "Unexpected stderr output: #{stderr_contents.inspect}"
    end
  end

  def send_results(results)
    results.each {|result| @program_stdin.write(result.to_msgpack)}
  end

  def receive_commands(*args)
    loop do
      command = unpacker.read.collect {|argument| argument.is_a?(String) ? argument.force_encoding("ASCII-8BIT") : argument}
      results = yield command
      break if command == ["quit"]
      send_results(results)
    end

    # to get meaningful test results, we have to wait until the program has committed its work, and we can't assume that till it's terminated
    @program_stdin.close
    wait
  rescue EOFError
    # ignore; the test case will use expects(:quit) if it expects a less abrupt end to the conversation
  ensure
    if stderr_contents != expected_stderr_contents
      fail "Unexpected stderr output: #{stderr_contents.inspect} instead of #{expected_stderr_contents.inspect}"
    end
  end

  def quit
    send_command("quit")
  end
end
