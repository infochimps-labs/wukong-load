require 'wukong-load'
require 'wukong/spec_helpers'

RSpec.configure do |config|
  config.mock_with :rspec
  include Wukong::SpecHelpers
end
