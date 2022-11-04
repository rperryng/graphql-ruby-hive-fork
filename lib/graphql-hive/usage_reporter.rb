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
        @thread = Thread.new do
          @options[:logger].info("[hive] spinning up thread to monitor usage report buffer from queue #{@queue}")

          buffer = []
          while (operation = @queue.pop(false))
            @options[:logger].info("add operation to buffer: #{operation}")
            @options_mutex.synchronize do
              if buffer.size >= @options[:buffer_size]
                @options[:logger].info('buffer is full, sending!')
                process_operations(buffer)
                buffer = []
              end
            end
          end
          unless buffer.size.zero?
            @options[:logger].info('shuting down with buffer, sending!')
            process_operations(buffer)
          end
        end
      end

      def add_operation(operation)
        @options[:logger].info("usage_reporter.add_operation adding report to queue (#{@queue})")
        @queue.push(operation)
        @options[:logger].info("usage_reporter.add_operation done - (queue #{@queue} has #{@queue.length} reports buffered)")
      end

      def on_exit
        @options[:logger].info("usage_reporter.add_operation")
        @queue.close
        @thread.join
      end

      private

      def process_operations(operations)
        report = {
          size: 0,
          map: {},
          operations: []
        }

        operations.each do |operation|
          add_operation_to_report(report, operation)
        end

        @options[:logger].info("sending report: #{report}")

        @client.send('/usage', report, :usage)
      end

      def add_operation_to_report(report, operation)
        timestamp, queries, results, duration = operation

        errors = errors_from_results(results)

        operation_name = queries.map(&:operations).map(&:keys).flatten.compact.join(', ')
        operation = ''
        fields = Set.new

        queries.each do |query|
          analyzer = GraphQL::Hive::Analyzer.new(query)
          visitor = GraphQL::Analysis::AST::Visitor.new(
            query: query,
            analyzers: [analyzer]
          )

          visitor.visit

          fields.merge(analyzer.result)

          operation += "\n" unless operation.empty?
          operation += GraphQL::Hive::Printer.new.print(visitor.result)
        end

        md5 = Digest::MD5.new
        md5.update operation
        operation_map_key = md5.hexdigest

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

        if results[0]
          context = results[0].query.context
          operation_record[:metadata] = { client: @options[:client_info].call(context) } if @options[:client_info]
        end

        report[:map][operation_map_key] = {
          fields: fields.to_a,
          operationName: operation_name,
          operation: operation
        }
        report[:operations] << operation_record
        report[:size] += 1
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
