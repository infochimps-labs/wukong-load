# -*- encoding: utf-8 -*-
require File.expand_path('../lib/wukong-load/version', __FILE__)

Gem::Specification.new do |gem|
  gem.name        = 'wukong-load'
  gem.homepage    = 'https://github.com/infochimps-labs/wukong-load'
  gem.licenses    = ["Apache 2.0"]
  gem.email       = 'coders@infochimps.com'
  gem.authors     = ['Infochimps', 'Philip (flip) Kromer', 'Travis Dempsey', 'Dhruv Bansal']
  gem.version     = Wukong::Load::VERSION

  gem.summary     = 'Load data produced by Wukong processors and dataflows into data stores.'
  gem.description = <<-EOF
  Lets you load data from the command-line into data stores like

  * Elasticsearch
  * MongoDB
  * HBase
  * MySQL
  * Kafka

and others.
EOF

  gem.files         = `git ls-files`.split("\n")
  gem.executables   = ['wu-load', 'wu-dump', 'wu-sync', 'wu-sync-all']
  gem.test_files    = gem.files.grep(/^spec/)
  gem.require_paths = ['lib']

  gem.add_dependency('wukong', '3.0.1')
end
