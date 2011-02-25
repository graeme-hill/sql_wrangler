spec = Gem::Specification.new do |s|
  s.name = "sql_wrangler"
  s.version = "0.0.1"
  s.authors = ["Graeme Hill"]
  s.email = "graemekh@gmail.com"
  s.homepage = "https://github.com/graeme-hill/sql_wrangler"
  s.platform = Gem::Platform::RUBY
  s.description = File.open("README").read
  s.summary = "A simple ORM alternative that makes it easier to deal with SQL queries in ruby."
  s.files = ["README", "lib/sql_wrangler.rb", "test/sql_wrangler_tests.rb"]
  s.require_path = "lib"
  s.test_files = ["test/sql_wrangler_tests.rb"]
  s.extra_rdoc_files = ["README"]
  s.has_rdoc = true
end