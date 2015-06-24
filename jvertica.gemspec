# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'jvertica/version'

Gem::Specification.new do |spec|
  spec.name          = "jvertica"
  spec.version       = Jvertica::VERSION
  spec.authors       = ["takahiro.nakayama"]
  spec.email         = ["civitaspo@gmail.com"]
  spec.summary       = %q{jvertica}
  spec.description   = %q{jvertica presents wrapper methods of jdbc-vertica java native methods.}
  spec.homepage      = "https://github.com/civitaspo/jvertica"
  spec.license       = "MIT"

  spec.files         = `git ls-files -z`.split("\x0")
  spec.executables   = spec.files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.test_files    = spec.files.grep(%r{^(test|spec|features)/})
  spec.require_paths = ["lib"]

  spec.add_development_dependency "bundler", "~> 1.7"
  spec.add_development_dependency "rake", "~> 10.0"
  spec.add_development_dependency "rspec"
  spec.add_development_dependency "dotenv"

  spec.add_dependency "jdbc-vertica"
end
