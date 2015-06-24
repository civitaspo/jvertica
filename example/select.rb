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

sql = 'select * from dt;'

c = Jvertica.connect(params)
c.query(sql) do |row|
  p row
end
