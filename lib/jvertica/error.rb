class Jvertica
  class Error < StandardError; end
  class ConnectionError < Error; end
  class InvalidQuery < Error; end
  class InvalidObject < Error; end
  class InsufficientArgument < Error; end
end
