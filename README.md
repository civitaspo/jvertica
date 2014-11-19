# Jvertica

jvertica presents wrapper methods of jdbc-vertica java native methods.

## Installation

Add this line to your application's Gemfile:

```ruby
gem 'jvertica'
```

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install jvertica

## Usage

```ruby
require 'jvertica'

conn_opts = {
  host: 'vertica.com',
  port: 5433,
  user: 'xxxxxx',
  password: 'xxxxxx',
}

sql = 'select * from sandbox.jdbc_tests;'

c = Jvertica.connect conn_opts
c.query sql do |row|
  p row
end
```

## Connection Options

see [the url](http://my.vertica.com/docs/7.1.x/HTML/index.htm#Authoring/ProgrammersGuide/ClientJDBC/JDBCConnectionProperties.htm)


## Loading data into Vertica using COPY

```ruby
connection.copy("COPY table FROM STDIN ...") do |stdin|
  File.open('data.tsv', 'r') do |f|
    begin
      stdin << f.gets
    end until f.eof?
  end
end
```

## Contributing

1. Fork it ( https://github.com/[my-github-username]/jvertica/fork )
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create a new Pull Request
