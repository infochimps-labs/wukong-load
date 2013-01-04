require 'wukong-load'
require 'wukong/spec_helpers'

RSpec.configure do |config|
  config.mock_with :rspec
  
  include Wukong::SpecHelpers

  config.before(:each) do
    Wukong::Log.level = Log4r::OFF
  end
  
  def root
    @root ||= Pathname.new(File.expand_path('../..', __FILE__))
  end

  def load_runner *args, &block
    runner(Wukong::Load::LoadRunner, 'wu-load', *args)
  end
end
