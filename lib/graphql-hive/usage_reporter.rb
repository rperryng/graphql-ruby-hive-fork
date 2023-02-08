# frozen_string_literal: true

require 'digest'
require 'graphql-hive/analyzer'
require 'graphql-hive/printer'

module GraphQL
  class Hive < GraphQL::Tracing::PlatformTracing
    # Report usage to Hive API without impacting application performances
    class UsageReporter
      @@instance = nil

      @queue = nil
      @thread = nil
      @operations_buffer = nil
      @client = nil

      def self.instance
        @@instance
      end

      def initialize(options, client)
        @@instance = self

        @options = options
        @client = client

        @options_mutex = Mutex.new
        @queue = Queue.new

        start_thread
      end

      def add_operation(operation)
        @queue.push(operation)
        log("operation added, buffer size: #{@queue.size}")
      end

      def on_exit
        @queue.close
        @thread.join
      end

      def on_start
        start_thread
      end

      private

      def start_thread
        if @thread&.alive?
          @options[:logger].warn("[#{tag}] Tried to start operations flushing thread but it was already alive")
          return
        end

        if @options_mutex.nil?
          @options_mutex = Mutex.new
        end

        if @queue.closed?
          log('Re-created graphql hive queue')
          @queue = Queue.new
        end

        # Thread.abort_on_exception = true

        log('Starting operations thread')
        @thread = Thread.new do
          log('Operation flushing thread started, thread id:')
          buffer = []
          while (operation = @queue.pop(false))
            log("add operation to buffer: #{operation}")
            buffer << operation
            @options_mutex.synchronize do
              if buffer.size >= @options[:buffer_size]
                log('buffer is full, sending!')
                process_operations(buffer)
                buffer = []
                log('buffer sent and reset')
              end
            end
          end

          log('Queue closed, exiting thread')

          unless buffer.size.zero?
            log('shuting down with buffer, sending!')
            process_operations(buffer)
          end
        rescue Exception => e
          @options[:logger].error("[#{tag}]: Operations flushing thread terminating", e)
          raise e
        end
      end

      def process_operations(operations)
        report = {
          size: 0,
          map: {},
          operations: []
        }

        operations.each do |operation|
          log("adding operation to report")
          add_operation_to_report(report, operation)
        end

        log("sending report: #{report}")

        @client.send('/usage', report, :usage)
      rescue StandardError => e
        @options[:logger].error("[#{tag}]Failed to send report: #{report}", e)
        raise e
      end

      def add_operation_to_report(report, operation)
        log("entered add_operation_to_report #{operation}")
        timestamp, queries, results, duration = operation
        log("extracted queries #{queries}")
        log("extracted results #{results}")

        errors = errors_from_results(results)
        log("collected errors from query result #{errors}")

        operation_name = queries.map(&:operations).map(&:keys).flatten.compact.join(', ')
        operation = ''
        fields = Set.new
        log("extracted operation name #{operation_name}")

        log("iterating queries (#{queries.size})")
        queries.each do |query|
          log("iterating query #{query}")

          analyzer = GraphQL::Hive::Analyzer.new(query)
          visitor = GraphQL::Analysis::AST::Visitor.new(
            query: query,
            analyzers: [analyzer]
          )
          log("analyizer and visitor created")

          visitor.visit
          log("visiting")

          fields.merge(analyzer.result)
          log("merged #{fields}")

          operation += "\n" unless operation.empty?
          operation += GraphQL::Hive::Printer.new.print(visitor.result)
          log("operation appended #{operation}")
        end

        log("calculating hash for #{operation_map_key}")
        md5 = Digest::MD5.new
        md5.update operation
        operation_map_key = md5.hexdigest
        log("added hash #{operation_map_key}")

        operation_record = {
          operationMapKey: operation_map_key,
          timestamp: timestamp.to_i,
          execution: {
            ok: errors[:errorsTotal].zero?,
            duration: duration,
            errorsTotal: errors[:errorsTotal],
            errors: errors[:errors]
          }
        }
        log("operation_record created #{operation_record}")

        if results[0]
          log("adding metadata")
          context = results[0].query.context
          operation_record[:metadata] = { client: @options[:client_info].call(context) } if @options[:client_info]
          log("metadata added #{operation_record}")
        end

        report[:map][operation_map_key] = {
          fields: fields.to_a,
          operationName: operation_name,
          operation: operation
        }
        report[:operations] << operation_record
        report[:size] += 1
        log("report updated #{report}")
      end

      def errors_from_results(results)
        acc = { errorsTotal: 0, errors: [] }
        results.each do |result|
          errors = result.to_h.fetch('errors', [])
          errors.each do |error|
            acc[:errorsTotal] += 1
            acc[:errors] << { message: error['message'], path: error['path'].join('.') }
          end
        end
        acc
      end

      def log(msg, level: :info)
        if level == :info
          @options[:logger].info("#{tag}: #{msg}")
        else
          @options[:logger].error("#{tag}: #{msg}")
        end
      end

      def tag
        "[graphql-hive (T:#{Thread.current.object_id})]"
      end
    end
  end
end
