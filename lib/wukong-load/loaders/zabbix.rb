module Wukong
  module Load

    # Loads data into a Zabbix server's trapper port.
    #
    # Uses the same protocol as the `zabbix_sender` utility that comes
    # with Zabbix.
    #
    # Records must define a `_host` field which names the Zabbix host
    # to which data from the record should be written.
    #
    # Records may also define a `_timestamp` field which sets the
    # timestamp for the record's data when written.
    #
    # All other data fields in the record will be turned into keys and
    # sent to Zabbix trapper as item measurements for the given host.
    # 
    # The following links provide details on the protocol used by Zabbix
    # to receive events:
    #
    # * https://www.zabbix.com/forum/showthread.php?t=20047&highlight=sender
    # * https://gist.github.com/1170577
    # * http://spin.atomicobject.com/2012/10/30/collecting-metrics-from-ruby-processes-using-zabbix-trappers/?utm_source=rubyflow&utm_medium=ao&utm_campaign=collecting-metrics-zabix
    class ZabbixLoader < Loader

      field :host,            String,  :default => 'localhost',  :doc => "Zabbix server host"
      field :port,            Integer, :default => 10051,        :doc => "Trapper port on Zabbix server"
      field :host_field,      String,  :default => '_host',      :doc => "Name of field in each record naming the Zabbix host"
      field :timestamp_field, String,  :default => '_timestamp', :doc => "Name of field in each record providing the timestamp at which the record was measured"

      description <<-EOF.gsub(/^ {8}/,'')
        Loads newline-separated, JSON-formatted records over STDIN
        into Zabbix using the zabbix_sender protocol.

          $ cat data.json | wu-load zabbix

        By default, wu-load attempts to write each record to a local
        Zabbix server.

        Input records must provide a '_host' field which names the
        Zabbix host the record's data is for.  Records may optionally
        provide a '_timestamp' field.  All other fields will be
        interpreted as measurements of Zabbix items for the given
        host.  The following record:

          { "_host": "first_zabbix_host",  "foo": { "bar": 3, "baz": { "quux": 4.3 } } }
          { "_host": "second_zabbix_host", "foo": { "bar": 12, } }

        Would have the same effect when loaded by wu-load as the
        following data when loaded by zabbix_sender:

          first_zabbix_host	foo.bar	3
          first_zabbix_host	foo.bar.baz.quux	4.3
          second_zabbix_host	foo.bar	12
       EOF

      # Socket used to send events to the Zabbix server.
      attr_accessor :socket

      def setup
        require 'socket'
      end

      def load record
        self.socket = TCPSocket.new(host, port)
        send_request(record)
        handle_response
        self.socket.close
      end
      
            
        # Insert events to a Zabbix server.
        #
        # The `topic` will be used as the name of the Zabbix host to
        # associate event data to.
        #
        # As per the documentation for the [Zabbix sender
        # protocol](https://www.zabbix.com/wiki/doc/tech/proto/zabbixsenderprotocol),
        # a new TCP connection will be created for each event.
        #
        # @param [String] topic
        # @param [Hash] cargo
        # Array<Hash>] text
        def insert topic, cargo={}
          self.socket = TCPSocket.new(host, port)
          send_request(topic, cargo)
          handle_response
          self.socket.close
        end

        private

        # :nodoc
        def send_request topic, cargo
          socket.write(payload(topic, cargo))
        end

        # :nodoc
        def handle_response
          header = socket.recv(5)
          if header == "ZBXD\1"
            data_header = socket.recv(8)
            length      = data_header[0,4].unpack("i")[0]
            response    = MultiJson.load(socket.recv(length))
            puts response["info"]
          else
            puts "Invalid response: #{header}"
          end
        end

        # :nodoc
        def payload topic, cargo={}
          body = body_for(topic, cargo)
          header_for(body) + body
        end

        # :nodoc
        def body_for topic, cargo={}
          MultiJson.dump({request: "sender data", data: zabbix_events_from(topic, cargo) })
        end

        # :nodoc
        def header_for body
          length = body.bytesize
          "ZBXD\1".encode("ascii") + [length].pack("i") + "\x00\x00\x00\x00"
        end

        # :nodoc
        def zabbix_events_from topic, cargo, scope=''
          events = []
          case cargo
          when Hash
            cargo.each_pair do |key, value|
              events += zabbix_events_from(topic, value, new_scope(scope, key))
            end
          when Array
            cargo.each_with_index do |item, index|
              events += zabbix_events_from(topic, item, new_scope(scope, index))
            end
          else
            events << event_body(topic, scope, cargo)
          end
          events
        end

        # :nodoc
        def new_scope(current_scope, new_scope)
          [current_scope, new_scope].map(&:to_s).reject(&:empty?).join('.')
        end

        # :nodoc
        def event_body topic, scope, cargo
          value = case cargo
                  when Hash  then cargo[:value]
                  when Array then cargo.first
                  else cargo
                  end
          { host: topic, key: scope, value: value }
        end
        
      end
    end

