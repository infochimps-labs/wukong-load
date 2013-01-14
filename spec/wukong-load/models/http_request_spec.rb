require 'spec_helper'

describe Wukong::HttpRequest do
  context "reporting the best IP address" do
    let(:real_ip)      { '10.122.122.122' }
    let(:forwarded_ip) { '10.123.123.123' }
    context "without an X-Forwarded-For header" do
      subject { Wukong::HttpRequest.receive(:ip_address => real_ip) }
      its(:best_ip_address) { should == real_ip }
    end
    context "with an X-Forwarded-For header" do
      subject do
        Wukong::HttpRequest.receive({
          :ip_address => real_ip,
          :headers    => {'X-Forwarded-For' => [forwarded_ip, '10.124.124.124'].join(', ') }
        })
      end
      its(:best_ip_address) { should == forwarded_ip }
    end
  end
end
