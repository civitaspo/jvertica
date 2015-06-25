require 'jvertica'
require 'dotenv'

# Create .env file like
# HOSTNAME=
# USERNAME=
# PASSWORD=
Dotenv.load

params = {
  host: ENV['HOSTNAME'],
  port: 5433,
  user: ENV['USERNAME'],
  password: ENV['PASSWORD'],
}
puts params

vertica = Jvertica.connect(params)
vertica.query("CREATE TABLE IF NOT EXISTS sandbox.jvertica_test (id integer, str varchar(80))")
vertica.copy("COPY sandbox.jvertica_test FROM STDIN PARSER fjsonparser() NO COMMIT") do |io|
  File.open(File.expand_path('../data.json', __FILE__), 'r') do |f|
    begin
      io << f.gets
    end until f.eof?
  end
end
vertica.commit
vertica.query("SELECT * from sandbox.jvertica_test") do |row|
  p row
end
vertica.query("DROP TABLE sandbox.jvertica_test")
