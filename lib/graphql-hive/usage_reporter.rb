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
        @options[:logger].info("operation added, buffer size: #{@queue.size}")
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
          @options[:logger].warn('Tried to start operations flushing thread but it was already alive')
          return
        end

        if @options_mutex.nil?
          @options_mutex = Mutex.new
        end

        if @queue.closed?
          @options[:logger].info('Re-created graphql hive queue')
          @queue = Queue.new
        end

        # otherwise the thread will just silently die when exceptions occur
        Thread.abort_on_exception = true

        @options[:logger].info('Starting operations thread')
        @thread = Thread.new do
          @options[:logger].info('Operation flushing thread started, thread id:', Thread.current.object_id)
          buffer = []
          while (operation = @queue.pop(false))
            @options[:logger].info("[#{Thread.current.object_id}]: add operation to buffer: #{operation}")
            buffer << operation
            @options_mutex.synchronize do
              if buffer.size >= @options[:buffer_size]
                @options[:logger].info('buffer is full, sending!')
                process_operations(buffer)
                buffer = []
                @options[:logger].info('buffer sent and reset')
              end
            end
          rescue Exception => e
            @options[:logger].error("operation flushing thread encountered error, dying", e)
            raise e
          end

          @options[:logger].info('Queue closed, exiting thread')

          unless buffer.size.zero?
            @options[:logger].info('shuting down with buffer, sending!')
            process_operations(buffer)
          end
        end
      end

      def process_operations(operations)
        report = {
          size: 0,
          map: {},
          operations: []
        }

        operations.each do |operation|
          @options[:logger].info("[#{Thread.current.object_id}]: [graphql-hive] adding operation to report")
          add_operation_to_report(report, operation)
        end

        @options[:logger].info("[#{Thread.current.object_id}]: [graphql-hive] sending report: #{report}")

        @client.send('/usage', report, :usage)
      rescue StandardError => e
        @options[:logger].error("Failed to send report: #{report}", e)
        raise e
      end

      def add_operation_to_report(report, operation)
        @options[:logger].info("[#{Thread.current.object_id}]: [graphql-hive] entered add_operation_to_report", operation)
        timestamp, queries, results, duration = operation
        @options[:logger].info("[#{Thread.current.object_id}]: [graphql-hive] extracted queries", queries)
        @options[:logger].info("[#{Thread.current.object_id}]: [graphql-hive] extracted results", results)

        errors = errors_from_results(results)
        @options[:logger].info("[#{Thread.current.object_id}]: [graphql-hive] collected errors from query result", errors)

        operation_name = queries.map(&:operations).map(&:keys).flatten.compact.join(', ')
        operation = ''
        fields = Set.new
        @options[:logger].info("[#{Thread.current.object_id}]: [graphql-hive] extracted operation name", operation_name)

        @options[:logger].info("[#{Thread.current.object_id}]: [graphql-hive] iterating queries (#{queries.size})")
        queries.each do |query|
          @options[:logger].info("[#{Thread.current.object_id}]: [graphql-hive] iterating query", query)

          analyzer = GraphQL::Hive::Analyzer.new(query)
          visitor = GraphQL::Analysis::AST::Visitor.new(
            query: query,
            analyzers: [analyzer]
          )
          @options[:logger].info("[#{Thread.current.object_id}]: [graphql-hive] analyizer and visitor created")

          visitor.visit
          @options[:logger].info("[#{Thread.current.object_id}]: [graphql-hive] visiting")

          fields.merge(analyzer.result)
          @options[:logger].info("[#{Thread.current.object_id}]: [graphql-hive] merged", fields)

          operation += "\n" unless operation.empty?
          operation += GraphQL::Hive::Printer.new.print(visitor.result)
          @options[:logger].info("[#{Thread.current.object_id}]: [graphql-hive] operation appended", operation)
        end

        md5 = Digest::MD5.new
        md5.update operation
        operation_map_key = md5.hexdigest
        @options[:logger].info("[#{Thread.current.object_id}]: [graphql-hive] added hash", operation_map_key)

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
        @options[:logger].info("[#{Thread.current.object_id}]: [graphql-hive] operation_record created", operation_record)

        if results[0]
          @options[:logger].info("[#{Thread.current.object_id}]: [graphql-hive] adding metadata")
          context = results[0].query.context
          operation_record[:metadata] = { client: @options[:client_info].call(context) } if @options[:client_info]
          @options[:logger].info("[#{Thread.current.object_id}]: [graphql-hive] metadata added", operation_record[:metadata])
        end

        report[:map][operation_map_key] = {
          fields: fields.to_a,
          operationName: operation_name,
          operation: operation
        }
        report[:operations] << operation_record
        report[:size] += 1
        @options[:logger].info("[#{Thread.current.object_id}]: [graphql-hive] report updated", report)
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
    end
  end
end
