require 'rubygems'
require 'test/unit'
require 'fileutils'
require 'ruby-debug'
require 'msgpack'

require File.expand_path(File.join(File.dirname(__FILE__), 'kitchen_sync_spawner'))

FileUtils.mkdir_p(File.join(File.dirname(__FILE__), 'tmp'))

ENDPOINT_DATABASES = %w(mysql postgresql)

module KitchenSync
  class TestCase < Test::Unit::TestCase
    undef_method :default_test if instance_methods.include? 'default_test' or
                                  instance_methods.include? :default_test

    def captured_stderr_filename
      @captured_stderr_filename ||= File.join(File.dirname(__FILE__), 'tmp', 'captured_stderr')
    end

    def binary_path
      @binary_path ||= File.join(File.dirname(__FILE__), '..', 'build', binary_name)
    end

    def spawner
      @spawner ||= KitchenSyncSpawner.new(binary_path, program_args, :capture_stderr_in => captured_stderr_filename)
    end

    def send_command(*args)
      spawner.send_command(*args)
    end

    def expect_stderr(contents)
      spawner.expect_stderr(contents) { yield }
    end
    
    def fixture_file_path(filename)
      File.join(File.dirname(__FILE__), 'fixtures', filename)
    end
  end

  class EndpointTestCase < TestCase
    attr_accessor :database

    def binary_name
      @binary_name ||= "ks_#{database}"
    end

    def program_args
      [
        from_or_to.to_s,
        ENV["#{database.upcase}_DATABASE_HOST"]     || ENV["ENDPOINT_DATABASE_HOST"]     || "",
        ENV["#{database.upcase}_DATABASE_PORT"]     || ENV["ENDPOINT_DATABASE_PORT"]     || "",
        ENV["#{database.upcase}_DATABASE_NAME"]     || ENV["ENDPOINT_DATABASE_NAME"]     || "ks_test",
        ENV["#{database.upcase}_DATABASE_USERNAME"] || ENV["ENDPOINT_DATABASE_USERNAME"] || "",
        ENV["#{database.upcase}_DATABASE_PASSWORD"] || ENV["ENDPOINT_DATABASE_PASSWORD"] || "",
        ENV["#{database.upcase}_DATABASE_READONLY"] || ENV["ENDPOINT_DATABASE_READONLY"] || ""
      ]
    end

    def self.test_each(description, &block)
      ENDPOINT_DATABASES.each do |database_name|
        define_method("test #{description} for #{database_name}".gsub(/\W+/,'_').to_sym) do
          self.database = database_name
          spawner.start_binary
          begin
            instance_eval(&block)
          ensure
            spawner.stop_binary
          end
        end
      end
    end
  end
end
