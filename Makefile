twirp:
	protoc --twirp_crystal_out=src/twirp_transport --crystal_out=src/twirp_transport --plugin=bin/protoc-gen-twirp_crystal --plugin=bin/protoc-gen-crystal ./protos/twirp.proto
	crystal tool format

ruby_client:
	protoc --ruby_out=clients/ruby --twirp_ruby_out=clients/ruby protos/twirp.proto
