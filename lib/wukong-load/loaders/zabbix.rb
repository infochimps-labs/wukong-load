module Wukong
  module Load

    # Loads data on pre-defined items into a Zabbix server.
    #
    # Uses {Rubix}[http://github.com/infochimps-labs/rubix] to
    # communicate with Zabbix.
    class ZabbixLoader < Loader
      
      field :server,          String,  :default => 'localhost',  :doc => "Hostname of Zabbix server (where data is sent)"
      field :port,            Integer, :default => 10051,        :doc => "Trapper port on Zabbix server"

      field :url,      String, default: 'http://localhost/api_jsonrpc.php', doc: "URL of the Zabbix API"
      field :username, String, default: 'admin', doc: "Username for the Zabbix API"
      field :password, String, default: 'zabbix', doc: "Password for the Zabbix API"

      field :host_field,      String,  :default => '_id',        :doc => "Name of field in each record naming the Zabbix host"
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

          { "_id": "first_zabbix_host",  "foo": { "bar": 3, "baz": { "quux": 4.3 } } }
          { "_id": "second_zabbix_host", "foo": { "bar": 12, } }

        Would have the same effect when loaded by wu-load as the
        following data when loaded by zabbix_sender:

          first_zabbix_host	foo.bar	3
          first_zabbix_host	foo.bar.baz.quux	4.3
          second_zabbix_host	foo.bar	12

       Writing the value of '3' for the key 'foo.bar' on host
       'first_zabbix_host', &c.
       EOF

      # Rubix object used to send events to the Zabbix server.
      attr_accessor :sender

      def setup
        require 'rubix'

        # Create a sender to send measurements about pre-defined
        # Zabbix items.
        log.debug("Connecting to Zabbix server at #{server}:#{port}")
        self.sender  = Rubix::Sender.new(server: server, port: port)

        # Connect via the API to create Hosts, Items, Triggers, &c.
        log.debug("Connecting to Zabbix API at #{url} as <#{username}>")
        Rubix.connect(url, username, password)

        Rubix::Builder.clear_cache
      end

      def load record
        case
        when record["_type"] == 'zabbix.host'
          Rubix::Builder.new(record).build
        else
          sender << zabbix_events_from(record.delete(host_field), record)
        end
      end

      # :nodoc
      def zabbix_events_from host, record, key=''
        events = []
        case record
        when Hash
          record.each_pair do |name, item|
            events += zabbix_events_from(host, item, nest_key(key, name))
          end
        when Array
          record.each_with_index do |item, index|
            events += zabbix_events_from(host, item, nest_key(scope, index))
          end
        else
          events << { host: host, key: key, value: record }
        end
        events
      end

      # :nodoc
      def nest_key(current_key, new_key)
        [current_key, new_key].map(&:to_s).reject(&:empty?).join('.')
      end

      register :zabbix_loader
      
    end
  end

end
