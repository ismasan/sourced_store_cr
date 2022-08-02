# frozen_string_literal: true

require_relative "sourced_client/version"

module SourcedClient
  class Error < StandardError; end
end

require_relative 'sourced_client/client'
