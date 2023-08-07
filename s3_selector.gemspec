# frozen_string_literal: true

lib = File.expand_path('lib', __dir__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 's3_selector/version'

Gem::Specification.new do |spec|
  spec.name          = 's3_selector'
  spec.version       = S3Selector::VERSION
  spec.authors       = ['Nathaniel Rowe']
  spec.email         = ['nathaniel.rowe@terminus.com']

  spec.summary       = 'Stream s3 select results'
  spec.description   = 'Stream s3 select results'
  spec.homepage      = 'https://github.com/GetTerminus/s3_selector'

  spec.metadata['allowed_push_host'] = 'https://www.rubygems.org'

  spec.metadata['homepage_uri'] = spec.homepage
  spec.metadata['source_code_uri'] = 'https://github.com/GetTerminus/s3_selector'
  # spec.metadata['changelog_uri'] = 'http://google.com'

  # Specify which files should be added to the gem when it is released.
  # The `git ls-files -z` loads the files in the RubyGem that have been added into git.
  spec.files = Dir.chdir(File.expand_path(__dir__)) do
    `git ls-files -z`.split("\x0").reject { |f| f.match(%r{^(test|spec|features|Gemfile.lock|vendor)/}) }
  end
  spec.bindir        = 'exe'
  spec.executables   = spec.files.grep(%r{^exe/}) { |f| File.basename(f) }
  spec.require_paths = ['lib']
  spec.required_ruby_version = Gem::Requirement.new('>= 2.6.0')

  spec.add_dependency 'aws-sdk-s3'
  spec.add_dependency 'aws-sdk-athena'
  spec.add_dependency 'concurrent_executor'


  spec.add_development_dependency 'benchmark-ips'
  spec.add_development_dependency 'bundler', '~> 2.0'
  spec.add_development_dependency 'pry'
  spec.add_development_dependency 'rake', '~> 10.0'
  spec.add_development_dependency 'rspec', '~> 3.0'
  spec.add_development_dependency 'rubocop'
  spec.add_development_dependency 'simplecov'
  spec.add_development_dependency 'webmock'
end
