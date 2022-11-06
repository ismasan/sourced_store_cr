module Sourced
  module Errors
    class Error < Exception
    end

    class ConcurrencyError < Error
    end
  end
end
