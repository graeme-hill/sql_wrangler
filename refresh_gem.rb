#!/usr/bin/ruby

require 'rubygems'

spec = eval File.open("sql_wrangler.gemspec").read

system %Q{rm *.gem; echo "Y\n" | gem uninstall -a sql_wrangler; gem build sql_wrangler.gemspec; gem install sql_wrangler-#{spec.version}.gem}