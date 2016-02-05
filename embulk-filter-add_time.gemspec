
Gem::Specification.new do |spec|
  spec.name          = "embulk-filter-add_time"
  spec.version       = "0.1.1"
  spec.authors       = ["Muga Nishizawa"]
  spec.summary       = %[Add time filter plugin for Embulk]
  spec.description   = %[Add time column to the schema]
  spec.email         = ["muga.nishizawa@gmail.com"]
  spec.licenses      = ["Apache 2.0"]
  spec.homepage      = "https://github.com/treasure-data/embulk-filter-add_time"

  spec.files         = `git ls-files`.split("\n") + Dir["classpath/*.jar"]
  spec.test_files    = spec.files.grep(%r"^(test|spec)/")
  spec.require_paths = ["lib"]

  spec.add_development_dependency 'bundler', ['~> 1.0']
  spec.add_development_dependency 'rake', ['>= 10.0']
end
