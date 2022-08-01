# # Generated from google/protobuf/struct.proto for google.protobuf
require "protobuf"

module Google
  module Protobuf
    enum NullValue
      NULLVALUE = 0
    end

    struct Struct
      include ::Protobuf::Message

      struct FieldsEntry
        include ::Protobuf::Message

        contract_of "proto3" do
          optional :key, :string, 1
          optional :value, Value, 2
        end
      end

      contract_of "proto3" do
        repeated :fields, Struct::FieldsEntry, 1
      end
    end

    struct Value
      include ::Protobuf::Message

      contract_of "proto3" do
        optional :null_value, NullValue, 1
        optional :number_value, :double, 2
        optional :string_value, :string, 3
        optional :bool_value, :bool, 4
        optional :struct_value, Struct, 5
        optional :list_value, ListValue, 6
      end
    end

    struct ListValue
      include ::Protobuf::Message

      contract_of "proto3" do
        repeated :values, Value, 1
      end
    end
  end
end
