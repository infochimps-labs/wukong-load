module Wukong

  # Represents a generic HTTP request.
  class HttpRequest
    
    include Gorillib::Model

    field :timestamp,  Integer, :doc => "Timestamp at which the HTTP request was received"
    field :verb,       String,  :doc => "HTTP verb of the request"
    field :path,       String,  :doc => "Absolute path to the resource requested"
    field :params,     Hash,    :doc => "Query parameters contained in the request", :default => {}
    field :headers,    Hash,    :doc => "HTTP headers of the request", :default => {}
    field :ip_address, String,  :doc => "IP address of the client"
    field :body,       String,  :doc => "Body of the request"

    # Return the URL of this request.
    #
    # @return [String]
    def url
      File.join(headers['Host'] || '', (path || ''))
    end

    # Return the HTTP Referer of this request.
    #
    # @return [String]
    def referer
      headers['Referer']
    end
    alias_method :referrer, :referer

    # Return the HTTP User-Agent of this request.
    #
    # @return [String]
    def user_agent
      headers['User-Agent']
    end

    # Return the HTTP Cookie of this request.
    #
    # @return [String]
    def cookie
      headers['Cookie']
    end

    # Return the "best" IP address from this request.
    #
    # Will return the first IP address in the HTTP X-Forwarded-For chain
    # if present, otherwise will return the IP address of the request
    # itself.
    #
    # @return [String]
    def best_ip_address
      ip_string = headers['X-Forwarded-For']
      return ip_address if ip_string.blank?
      ips = ip_string.split(/\s*,\s*/)
      ips.empty? ? ip_address : ips.first # client comes first, then proxies in order
    end
    
  end
end
